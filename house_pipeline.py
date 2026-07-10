"""
house_pipeline.py
=================

US House Clerk financial disclosure (PTR) pipeline for Apex Q.

Completes the congressional picture: paired with the Senate eFD pipeline, this gives
Apex Q first-party coverage of both chambers, the coverage needed to eventually cut
the third-party Quiver congressional feed.

The House is the hardest congressional source. Unlike the Senate's queryable search,
the House Clerk publishes disclosures as PDF documents, many scanned or handwritten,
alongside a structured annual index. So this pipeline is deliberately two-tier:

  Tier 1 (reliable): the House Clerk publishes an annual ZIP for each year at
    disclosures-clerk.house.gov containing {YEAR}FD.txt, a tab-delimited index of every
    filing: member name, filing type, year, and the DocID that locates the PDF. We parse
    that index to know WHO filed a Periodic Transaction Report (type 'P') and WHEN. This
    is stable and gives real coverage of which representatives are actively trading.

  Tier 2 (best-effort): for each PTR, we attempt to download and parse the PDF to extract
    the individual trades (ticker, type, amount, date). Text-based PDFs parse cleanly;
    scanned/handwritten ones are flagged requires_ocr rather than guessed at. A filing we
    cannot parse is stored as a marker (member filed a PTR on date X) with no fabricated
    trades, so the data is honest.

Same discipline as every other pipeline: standalone module, own logger, total error
containment, reuses the shared ticker map. Reads only DATABASE_URL. Lambda-ready.
"""

import os
import re
import io
import csv
import json
import time
import zipfile
import logging
import threading
from datetime import datetime, date

import requests
import psycopg2
from psycopg2.extras import Json

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] house: %(message)s")
logger = logging.getLogger("house_pipeline")

HOUSE_USER_AGENT = "ApexQ/1.0 support@apexq.io"
HOUSE_ZIP_URL = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/%sFD.ZIP"
HOUSE_PDF_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/%s/%s.pdf"
HOUSE_MAX_RPS = 2

try:
    from lda_pipeline import map_company_to_ticker as _map_ticker
except Exception:
    _map_ticker = None


def _clean_ticker(raw):
    """Clean a ticker cell from a House filing. Kept conservative, returns None on junk."""
    if not raw:
        return None
    t = str(raw).strip().upper()
    t = re.sub(r"[^A-Z0-9.]", "", t)
    if not t or len(t) > 6 or not re.search(r"[A-Z]", t):
        return None
    if t in ("N", "NA", "NONE", "UNKNOWN"):
        return None
    return t


def _map_or_ticker(name, raw_ticker):
    """Prefer an explicit ticker cell; fall back to mapping the asset/company name."""
    t = _clean_ticker(raw_ticker)
    if t:
        return t
    if _map_ticker is not None:
        return _map_ticker(name)
    return None


# --------------------------------------------------------------------------- #
# Rate limiter
# --------------------------------------------------------------------------- #

class _TokenBucket:
    def __init__(self, rate):
        self.rate = float(rate); self.capacity = float(rate); self.tokens = float(rate)
        self.last = time.monotonic(); self.lock = threading.Lock()
    def take(self):
        with self.lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens < 1.0:
                time.sleep((1.0 - self.tokens) / self.rate); self.tokens = 0.0
            else:
                self.tokens -= 1.0


_bucket = _TokenBucket(HOUSE_MAX_RPS)


def _house_get(url, timeout=45, retries=3, binary=False):
    headers = {"User-Agent": HOUSE_USER_AGENT}
    for attempt in range(retries):
        _bucket.take()
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                time.sleep(2 ** attempt); continue
            logger.warning("HTTP %s on %s", r.status_code, url)
        except requests.RequestException as e:
            logger.warning("request error on %s: %s", url, e)
        time.sleep(1 + attempt)
    return None


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #

def _connect():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set; cannot connect to PostgreSQL.")
    if dsn.startswith("postgres://"):
        dsn = dsn.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(dsn)


def _create_table(conn):
    """House trades table, mirroring the Senate table shape so both merge into the app identically."""
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS congressional_trades_house (
            id SERIAL PRIMARY KEY,
            politician_name TEXT,
            party TEXT DEFAULT 'Unknown',
            state TEXT DEFAULT 'Unknown',
            ticker TEXT,
            transaction_type TEXT,
            amount TEXT,
            trade_date DATE,
            filing_date DATE,
            doc_id TEXT,
            parse_status TEXT DEFAULT 'parsed',
            raw_json JSONB,
            source TEXT DEFAULT 'house_clerk',
            content_hash TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT NOW()
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_house_ticker ON congressional_trades_house(ticker)")
    conn.commit()
    cur.close()


def _exists(conn, content_hash):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM congressional_trades_house WHERE content_hash = %s", (content_hash,))
    row = cur.fetchone()
    cur.close()
    return bool(row)


def _hash(*parts):
    import hashlib
    return hashlib.sha1("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Tier 1: the annual filing index
# --------------------------------------------------------------------------- #

def _fetch_index(year):
    """Download and parse the House annual disclosure index ZIP.

    Returns a list of dicts for Periodic Transaction Reports only (FilingType 'P'), each with
    member name, state district, filing year, filing date, and DocID. Returns [] on any failure.
    """
    url = HOUSE_ZIP_URL % year
    r = _house_get(url, binary=True)
    if r is None:
        logger.error("could not fetch House index ZIP for %s", year)
        return []
    filings = []
    try:
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        # The index text file is named {YEAR}FD.txt inside the ZIP.
        txt_name = None
        for n in zf.namelist():
            if n.lower().endswith(".txt"):
                txt_name = n
                break
        if not txt_name:
            logger.error("no index txt in House ZIP for %s", year)
            return []
        raw = zf.read(txt_name).decode("utf-8", errors="replace")
        # Tab-delimited with a header row: Prefix, Last, First, Suffix, FilingType, StateDst,
        # Year, FilingDate, DocID.
        reader = csv.DictReader(io.StringIO(raw), delimiter="\t")
        for row in reader:
            ftype = (row.get("FilingType") or "").strip().upper()
            if ftype != "P":  # P = Periodic Transaction Report (the trades)
                continue
            last = (row.get("Last") or "").strip()
            first = (row.get("First") or "").strip()
            name = (first + " " + last).strip()
            state_dst = (row.get("StateDst") or "").strip()
            state = state_dst[:2] if len(state_dst) >= 2 else "Unknown"
            filings.append({
                "name": name or "Unknown",
                "state": state,
                "filing_year": (row.get("Year") or "").strip(),
                "filing_date": _parse_date(row.get("FilingDate")),
                "doc_id": (row.get("DocID") or "").strip(),
            })
    except Exception as e:
        logger.error("parsing House index for %s: %s", year, e)
        return []
    logger.info("House index %s: %s PTR filings", year, len(filings))
    return filings


def _parse_date(s):
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    return None


# --------------------------------------------------------------------------- #
# Tier 2: best-effort PDF parsing
# --------------------------------------------------------------------------- #

def _parse_ptr_pdf(year, doc_id):
    """Attempt to extract trades from a PTR PDF. Returns (trades, requires_ocr).

    Text-based PDFs parse; scanned/handwritten ones yield no extractable text and are flagged
    requires_ocr rather than guessed at. Never raises.
    """
    url = HOUSE_PDF_URL % (year, doc_id)
    r = _house_get(url, binary=True)
    if r is None:
        return [], False
    text = ""
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            for page in pdf.pages[:6]:
                t = page.extract_text() or ""
                text += "\n" + t
    except Exception as e:
        logger.warning("pdf parse error %s: %s", doc_id, e)
        return [], True

    if len(text.strip()) < 40:
        # No meaningful text extracted: almost certainly a scanned/handwritten filing.
        return [], True

    trades = []
    # House PTR text lists transactions with a ticker in parentheses, a type (P/S/E), a date,
    # and an amount bracket. Patterns vary, so we scan line by line for a recognizable shape.
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # Ticker often appears as (AAPL) or [AAPL].
        tk = re.search(r"[\(\[]([A-Z]{1,6}(?:\.[A-Z])?)[\)\]]", line)
        # Transaction type keyword.
        ttype = None
        low = line.lower()
        if re.search(r"\bpurchase\b|\b\(p\)\b|\bbuy\b", low):
            ttype = "buy"
        elif re.search(r"\bsale\b|\bsold\b|\b\(s\)\b|\bsell\b", low):
            ttype = "sell"
        # Amount bracket like $1,001 - $15,000.
        amt = re.search(r"\$[\d,]+\s*[-–]\s*\$[\d,]+", line)
        # Date like 01/15/2026.
        dt = re.search(r"\d{2}/\d{2}/\d{4}", line)
        if tk and (ttype or amt):
            trades.append({
                "ticker": _clean_ticker(tk.group(1)),
                "transaction_type": ttype or "other",
                "amount": amt.group(0) if amt else None,
                "trade_date": _parse_date(dt.group(0)) if dt else None,
            })
    # Keep only rows with a usable ticker.
    trades = [t for t in trades if t.get("ticker")]
    return trades, False


# --------------------------------------------------------------------------- #
# Store
# --------------------------------------------------------------------------- #

def _store(conn, rec, summary, parse_status="parsed"):
    try:
        h = _hash(rec.get("politician_name"), rec.get("ticker"), rec.get("trade_date"),
                  rec.get("amount"), rec.get("transaction_type"), rec.get("doc_id"))
        if _exists(conn, h):
            summary["skipped_existing"] += 1
            return
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO congressional_trades_house (politician_name, party, state, ticker, "
            "transaction_type, amount, trade_date, filing_date, doc_id, parse_status, raw_json, content_hash) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (content_hash) DO NOTHING",
            (rec.get("politician_name"), rec.get("party", "Unknown"), rec.get("state", "Unknown"),
             rec.get("ticker"), rec.get("transaction_type"), rec.get("amount"),
             rec.get("trade_date"), rec.get("filing_date"), rec.get("doc_id"), parse_status,
             Json(rec.get("raw_json") or {}), h))
        conn.commit()
        cur.close()
        if parse_status == "parsed" and rec.get("ticker"):
            summary["trades"] += 1
    except Exception as e:
        logger.error("store error: %s", e)
        summary["errors"] += 1
        try:
            conn.rollback()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def fetch_house_trades(year=None, max_filings=40):
    """Main entry point. Pull recent House PTR filings and their trades. Never raises.

    Args:
        year: filing year to scan; defaults to the current year.
        max_filings: cap on how many PTR PDFs to parse this run (politeness + time budget).
    Returns a summary dict.
    """
    started = time.time()
    year = year or date.today().year
    summary = {"trades": 0, "errors": 0, "skipped_existing": 0, "requires_ocr": 0,
               "filings_seen": 0, "pdfs_parsed": 0}
    conn = None
    try:
        conn = _connect()
        _create_table(conn)

        filings = _fetch_index(year)
        summary["filings_seen"] = len(filings)
        if not filings:
            summary["note"] = "no House index filings for %s" % year
            summary["elapsed_sec"] = round(time.time() - started, 1)
            return summary

        # Newest filings first, capped for a polite run.
        filings.sort(key=lambda f: (f.get("filing_date") or date.min), reverse=True)
        processed = 0
        for f in filings:
            if processed >= max_filings:
                break
            doc_id = f.get("doc_id")
            if not doc_id:
                continue
            processed += 1
            try:
                trades, needs_ocr = _parse_ptr_pdf(year, doc_id)
                summary["pdfs_parsed"] += 1
                if needs_ocr:
                    summary["requires_ocr"] += 1
                    _store(conn, {
                        "politician_name": f["name"], "state": f["state"], "party": "Unknown",
                        "ticker": None, "transaction_type": "other", "amount": None,
                        "trade_date": None, "filing_date": f["filing_date"], "doc_id": doc_id,
                        "raw_json": {"parse_status": "requires_ocr"},
                    }, summary, parse_status="requires_ocr")
                    continue
                if not trades:
                    # Parsed but no recognizable trades: store a marker so we do not re-fetch endlessly.
                    _store(conn, {
                        "politician_name": f["name"], "state": f["state"], "party": "Unknown",
                        "ticker": None, "transaction_type": "other", "amount": None,
                        "trade_date": None, "filing_date": f["filing_date"], "doc_id": doc_id,
                        "raw_json": {"parse_status": "no_trades_found"},
                    }, summary, parse_status="empty")
                    continue
                for tr in trades:
                    _store(conn, {
                        "politician_name": f["name"], "state": f["state"], "party": "Unknown",
                        "ticker": tr["ticker"], "transaction_type": tr["transaction_type"],
                        "amount": tr["amount"], "trade_date": tr["trade_date"],
                        "filing_date": f["filing_date"], "doc_id": doc_id,
                        "raw_json": {"doc_id": doc_id},
                    }, summary)
            except Exception as e:
                logger.error("error on filing %s: %s", doc_id, e)
                summary["errors"] += 1
                try:
                    conn.rollback()
                except Exception:
                    pass

        summary["elapsed_sec"] = round(time.time() - started, 1)
        logger.info("house run complete: %s", summary)
        return summary
    except Exception as e:
        logger.error("fetch_house_trades fatal: %s", e)
        summary["errors"] += 1
        summary["note"] = str(e)
        summary["elapsed_sec"] = round(time.time() - started, 1)
        return summary
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    print(json.dumps(fetch_house_trades(max_filings=10), indent=2, default=str))
