"""
senate_efd_pipeline.py
======================

US Senate Electronic Financial Disclosures (eFD) pipeline for Apex Q.

Scrapes Senate Periodic Transaction Reports (PTRs), parses the disclosed trades,
and stores them in PostgreSQL. Built to eventually replace the Quiver congressional
feed with data Apex Q owns, sourced directly from the Senate.

This module is deliberately, completely independent of the SEC EDGAR pipeline.
The Senate eFD site is far more hostile to automation than SEC EDGAR: it is
session gated behind a terms agreement, its HTML changes, and some reports are
handwritten PDFs. So every failure mode here is caught and folded into a summary
return; nothing raises to the caller, and nothing this module does can touch the
SEC pipeline or the rest of the app.

Honest status note: the efdsearch flow below is reverse engineered from the live
site's behavior. It works against the site's current structure, but the Senate
changes it without notice, so the first live runs may need tuning. That is exactly
why this runs decoupled and why the app keeps using Quiver until this data is
proven and you choose to switch.

Shape note: the table mirrors the fields Apex Q already consumes from Quiver
(politician, transaction type, amount range, ticker, dates) so that swapping the
app from Quiver to this source later is a clean drop in rather than a rewrite.
"""

import os
import re
import json
import time
import hashlib
import logging
import threading
from datetime import datetime, date, timedelta

import requests
import psycopg2
from psycopg2.extras import Json

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] senate_efd: %(message)s")
logger = logging.getLogger("senate_efd_pipeline")

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

SENATE_USER_AGENT = "ApexQ/1.0 support@apexq.io"
EFD_HOME = "https://efdsearch.senate.gov/search/"
EFD_AGREE = "https://efdsearch.senate.gov/search/home/"
EFD_SEARCH_DATA = "https://efdsearch.senate.gov/search/report/data/"
EFD_BASE = "https://efdsearch.senate.gov"

# The Senate server firewalls aggressive clients hard, so we clamp well below what
# we use for the SEC. Three requests per second is polite and keeps us un-blocked.
SENATE_MAX_RPS = 3

# Amount ranges are disclosed as text bands; we store them verbatim.
TXN_TYPE_MAP = {
    "purchase": "buy",
    "sale": "sell",
    "sale (partial)": "sell",
    "sale (full)": "sell",
    "exchange": "exchange",
}


# --------------------------------------------------------------------------- #
# Rate limiter
# --------------------------------------------------------------------------- #

class _TokenBucket:
    """At most `rate` requests per second, blocking when dry. Independent of SEC's bucket."""
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


_senate_bucket = _TokenBucket(SENATE_MAX_RPS)


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
    """Create the Senate trades table if absent. Mirrors the Quiver-consumed shape so the
    eventual app swap from Quiver to this source is a drop in."""
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS congressional_trades_senate (
            id SERIAL PRIMARY KEY,
            politician_name TEXT,
            party TEXT DEFAULT 'Unknown',
            state TEXT DEFAULT 'Unknown',
            ticker TEXT,
            transaction_type TEXT,
            amount TEXT,
            trade_date DATE,
            filing_date DATE,
            raw_json JSONB,
            source TEXT DEFAULT 'senate_efd',
            content_hash TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT NOW()
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_senate_ticker ON congressional_trades_senate(ticker)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_senate_politician ON congressional_trades_senate(politician_name)")
    conn.commit()
    cur.close()


def _row_hash(politician, ticker, trade_date, amount, txn_type):
    """Stable idempotency key from the trade's identifying fields."""
    raw = "|".join(str(x) for x in (politician, ticker, trade_date, amount, txn_type))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Senate eFD session
# --------------------------------------------------------------------------- #

def _get_authenticated_senate_session():
    """Boot a session and clear the mandatory terms agreement gate.

    The Senate search sits behind a one time agreement form guarded by a CSRF token.
    We load the home page, read the csrfmiddlewaretoken, and POST the agreement so the
    session cookie is authorized for the data endpoint. Returns a ready Session, or None
    if the gate could not be cleared (in which case the run ends cleanly with an error).
    """
    s = requests.Session()
    s.headers.update({"User-Agent": SENATE_USER_AGENT,
                      "Referer": EFD_HOME,
                      "Accept": "text/html,application/xhtml+xml,application/json"})
    try:
        _senate_bucket.take()
        r = s.get(EFD_HOME, timeout=30)
        if r.status_code != 200:
            logger.error("efd home returned %s", r.status_code)
            return None
        token = _extract_csrf(r.text) or s.cookies.get("csrftoken")
        if not token:
            logger.error("no CSRF token on efd home; site structure may have changed")
            return None
        _senate_bucket.take()
        r2 = s.post(EFD_AGREE,
                    data={"csrfmiddlewaretoken": token, "prohibition_agreement": "1"},
                    headers={"Referer": EFD_HOME}, timeout=30)
        if r2.status_code not in (200, 302):
            logger.error("efd agreement POST returned %s", r2.status_code)
            return None
        # Stash the token for later data POSTs.
        s.headers.update({"X-CSRFToken": token, "X-Requested-With": "XMLHttpRequest"})
        s._efd_csrf = token
        logger.info("senate efd session authorized")
        return s
    except requests.RequestException as e:
        logger.error("efd session error: %s", e)
        return None


def _extract_csrf(html):
    m = re.search(r'name="csrfmiddlewaretoken"\s+value="([^"]+)"', html)
    if m:
        return m.group(1)
    m = re.search(r'csrfmiddlewaretoken["\']?\s*[:=]\s*["\']([^"\']+)', html)
    return m.group(1) if m else None


# --------------------------------------------------------------------------- #
# Fetch and parse
# --------------------------------------------------------------------------- #

def _clean_ticker(raw):
    """Clean a manually entered ticker. Senators type these by hand, so they are messy.

    Keep letters, digits, and a single dot (BRK.B), uppercased. Reject obvious non tickers.
    We do NOT strip digits, some valid tickers contain them; the naive [^A-Z] scrub would
    have corrupted those.
    """
    if not raw:
        return None
    t = str(raw).strip().upper()
    if t in ("N/A", "NA", "--", "—", "-", "", "NONE", "UNKNOWN"):
        return None
    # Allow letters, digits, and dots; drop everything else.
    t = re.sub(r"[^A-Z0-9.]", "", t)
    if not t or len(t) > 6 or not re.search(r"[A-Z]", t):
        return None
    return t


def _classify_txn(raw_type):
    if not raw_type:
        return "other"
    key = str(raw_type).strip().lower()
    for k, v in TXN_TYPE_MAP.items():
        if k in key:
            return v
    return "other"


def _parse_efd_date(s):
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    return None


def _search_ptrs(session, days_back):
    """Query the eFD data endpoint for recent PTR filings within the window.

    The endpoint is a DataTables backed JSON API. Returns a list of filing rows, each a
    list like [first, last, office, report_type, filing_date_html]. Returns [] on any
    failure so the caller can end cleanly.
    """
    results = []
    start_dt = (date.today() - timedelta(days=days_back)).strftime("%m/%d/%Y")
    end_dt = date.today().strftime("%m/%d/%Y")
    payload = {
        "start": "0", "length": "100",
        "report_types": "[11]",            # 11 = Periodic Transaction Report
        "filer_types": "[]",
        "submitted_start_date": start_dt + " 00:00:00",
        "submitted_end_date": end_dt + " 23:59:59",
        "candidate_state": "", "senator_state": "", "office_id": "", "first_name": "", "last_name": "",
        "csrfmiddlewaretoken": getattr(session, "_efd_csrf", ""),
    }
    try:
        _senate_bucket.take()
        r = session.post(EFD_SEARCH_DATA, data=payload, timeout=30)
        if r.status_code != 200:
            logger.error("efd search data returned %s", r.status_code)
            return []
        data = r.json()
        results = data.get("data", [])
        logger.info("efd search returned %s PTR filings", len(results))
    except (requests.RequestException, ValueError) as e:
        logger.error("efd search error: %s", e)
        return []
    return results


def _extract_report_link(filing_row):
    """Pull the report href and filing date from a search result row.

    Real row shape (confirmed from the live site):
      [first_name, last_name, "Last, First (Senator)", "<a href=...>Periodic Transaction Report...", "07/08/2026"]
    The anchor lives in the report-type cell (index 3), not the last cell. We scan every cell for
    an href so we are resilient to column shifts, and read the visible date from the last cell.
    """
    href = None
    for cell in filing_row:
        m = re.search(r'href="([^"]+)"', str(cell))
        if m:
            href = m.group(1)
            break
    # Filing date is the last cell, typically a plain date string.
    filed = None
    if filing_row:
        last = re.sub(r"<[^>]+>", "", str(filing_row[-1])).strip()
        dm = re.search(r"(\d{2}/\d{2}/\d{4})", last)
        filed = dm.group(1) if dm else None
    if href and href.startswith("/"):
        href = EFD_BASE + href
    return href, filed


def debug_first_report(days_back=7):
    """Diagnostic: fetch the first PTR found and return what its page actually contains, so the
    table parser can be tuned against the real HTML instead of assumptions. Returns a small dict
    with the report URL, whether a table was found, the column headers, and the first data row's
    raw cells. Never stores anything. Safe to expose behind the cron token."""
    out = {"filings_found": 0, "report_url": None, "has_table": False,
           "headers": [], "first_row_cells": [], "note": ""}
    try:
        session = _get_authenticated_senate_session()
        if session is None:
            out["note"] = "could not authorize session"
            return out
        filings = _search_ptrs(session, days_back)
        out["filings_found"] = len(filings)
        if not filings:
            out["note"] = "no PTR filings in window"
            return out
        url, _ = _extract_report_link(filings[0])
        out["report_url"] = url
        if not url:
            out["note"] = "no report link extracted from filing row"
            out["raw_filing_row"] = [str(c)[:120] for c in filings[0]]
            return out
        _senate_bucket.take()
        r = session.get(url, timeout=30)
        html = r.text
        out["status"] = r.status_code
        out["is_pdf"] = ("/paper/" in url or ".pdf" in url.lower())
        # Pull table headers.
        thead = re.search(r"<thead>(.*?)</thead>", html, re.DOTALL)
        if thead:
            out["headers"] = [re.sub(r"<[^>]+>", "", c).strip()
                              for c in re.findall(r"<th[^>]*>(.*?)</th>", thead.group(1), re.DOTALL)]
        # Pull first data row cells.
        tbody = re.search(r"<tbody>(.*?)</tbody>", html, re.DOTALL)
        body = tbody.group(1) if tbody else html
        first_tr = re.search(r"<tr[^>]*>(.*?)</tr>", body, re.DOTALL)
        if first_tr:
            out["has_table"] = True
            out["first_row_cells"] = [re.sub(r"<[^>]+>", "", c).strip()
                                      for c in re.findall(r"<td[^>]*>(.*?)</td>", first_tr.group(1), re.DOTALL)]
        out["table_striped_present"] = "table-striped" in html
        out["html_length"] = len(html)
        return out
    except Exception as e:
        out["note"] = "error: " + str(e)
        return out


def _parse_report_page(session, url):
    """Fetch a PTR report page and extract its transaction rows from the .table-striped table.

    Returns (rows, needs_ocr). rows is a list of dicts. needs_ocr is True when the report is
    a scanned/handwritten PDF with no HTML table, which we flag rather than fail on.
    """
    try:
        _senate_bucket.take()
        r = session.get(url, timeout=30)
        if r.status_code != 200:
            return [], False
        html = r.text
    except requests.RequestException as e:
        logger.error("report fetch error %s: %s", url, e)
        return [], False

    # Paper (handwritten) filings link to a PDF and have no HTML transaction table.
    if "/paper/" in url or ".pdf" in url.lower():
        return [], True
    if "table-striped" not in html and "<table" not in html:
        # No parseable table; likely an image based report.
        return [], True

    rows = []
    # Grab the first table body and split into rows/cells with light regex, avoiding a
    # BeautifulSoup dependency. Senate PTR tables are simple and regular.
    tbody = re.search(r"<tbody>(.*?)</tbody>", html, re.DOTALL)
    body = tbody.group(1) if tbody else html
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", body, re.DOTALL):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.DOTALL)
        cells = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
        if len(cells) < 7:
            continue
        # Typical PTR columns: #, Transaction Date, Owner, Ticker, Asset Name, Type, Amount
        # Column positions vary slightly; locate by content where possible.
        trade_date = _parse_efd_date(cells[1])
        ticker = _clean_ticker(cells[3])
        asset = cells[4]
        txn_type = _classify_txn(cells[5])
        amount = cells[6]
        if not ticker:
            continue
        rows.append({"trade_date": trade_date, "ticker": ticker, "asset": asset,
                     "transaction_type": txn_type, "amount": amount})
    return rows, False


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def fetch_senate_trades(days_back=1):
    """Main entry point. Scrape and store recent Senate PTR trades.

    Returns {"trades": int, "errors": int, "elapsed_sec": float, ...}. Never raises: any
    failure is caught and reflected in the summary, so a Senate outage can never crash the
    caller or affect the SEC pipeline.
    """
    started = time.time()
    summary = {"trades": 0, "errors": 0, "skipped_existing": 0, "requires_ocr": 0, "filings": 0}
    conn = None
    try:
        conn = _connect()
        _create_table(conn)
        session = _get_authenticated_senate_session()
        if session is None:
            summary["errors"] += 1
            summary["note"] = "could not authorize senate session"
            summary["elapsed_sec"] = round(time.time() - started, 1)
            return summary

        filings = _search_ptrs(session, days_back)
        summary["filings"] = len(filings)

        for frow in filings:
            try:
                # frow: [first, last, office/state, report_type, filing_date_cell]
                first = str(frow[0]).strip() if len(frow) > 0 else ""
                last = str(frow[1]).strip() if len(frow) > 1 else ""
                office = str(frow[2]).strip() if len(frow) > 2 else ""
                politician = (first + " " + last).strip() or "Unknown"
                # Office often reads like "Senator - State (Party)"; pull hints if present.
                state = "Unknown"; party = "Unknown"
                sm = re.search(r"([A-Z]{2})\b", office)
                if sm:
                    state = sm.group(1)
                pm = re.search(r"\((R|D|I)[^)]*\)", office)
                if pm:
                    party = {"R": "Republican", "D": "Democrat", "I": "Independent"}.get(pm.group(1), "Unknown")

                url, filed = _extract_report_link(frow)
                filing_date = _parse_efd_date(filed)
                if not url:
                    continue

                rows, needs_ocr = _parse_report_page(session, url)
                if needs_ocr:
                    summary["requires_ocr"] += 1
                    # Record a marker row so we know a report needs OCR, without fake trade data.
                    _store_trade(conn, {
                        "politician_name": politician, "party": party, "state": state,
                        "ticker": None, "transaction_type": "other", "amount": None,
                        "trade_date": None, "filing_date": filing_date,
                        "raw_json": {"url": url, "parse_status": "requires_ocr"},
                    }, summary, parse_status="requires_ocr")
                    continue

                for tr in rows:
                    _store_trade(conn, {
                        "politician_name": politician, "party": party, "state": state,
                        "ticker": tr["ticker"], "transaction_type": tr["transaction_type"],
                        "amount": tr["amount"], "trade_date": tr["trade_date"],
                        "filing_date": filing_date,
                        "raw_json": {"url": url, "asset": tr.get("asset")},
                    }, summary)
            except Exception as e:
                logger.error("error on filing: %s", e)
                summary["errors"] += 1
                try:
                    conn.rollback()
                except Exception:
                    pass

        summary["elapsed_sec"] = round(time.time() - started, 1)
        logger.info("senate run complete: %s", summary)
        return summary
    except Exception as e:
        # Total containment: any unexpected failure ends here, never propagates.
        logger.error("fetch_senate_trades fatal: %s", e)
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


def _store_trade(conn, t, summary, parse_status="parsed"):
    """Insert one trade, idempotent via content hash. Updates the summary counters."""
    try:
        h = _row_hash(t["politician_name"], t["ticker"], t["trade_date"], t["amount"], t["transaction_type"])
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM congressional_trades_senate WHERE content_hash = %s", (h,))
        if cur.fetchone():
            summary["skipped_existing"] += 1
            cur.close()
            return
        cur.execute(
            "INSERT INTO congressional_trades_senate (politician_name, party, state, ticker, "
            "transaction_type, amount, trade_date, filing_date, raw_json, content_hash) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (content_hash) DO NOTHING",
            (t["politician_name"], t.get("party", "Unknown"), t.get("state", "Unknown"),
             t.get("ticker"), t.get("transaction_type"), t.get("amount"),
             t.get("trade_date"), t.get("filing_date"), Json(t.get("raw_json") or {}), h))
        conn.commit()
        cur.close()
        if parse_status == "parsed" and t.get("ticker"):
            summary["trades"] += 1
    except Exception as e:
        logger.error("store trade error: %s", e)
        summary["errors"] += 1
        try:
            conn.rollback()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Sample call
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    # Manual test: scan the last 7 days so there is something to find. Production cron calls
    # fetch_senate_trades(days_back=1) daily.
    result = fetch_senate_trades(days_back=7)
    print(json.dumps(result, indent=2, default=str))
