"""
lda_pipeline.py
===============

US Senate Lobbying Disclosure Act (LDA) pipeline for Apex Q.

Pulls lobbying disclosure filings from the Senate LDA REST API, maps the paying
client to a stock ticker where possible, and stores the records in PostgreSQL.
This is a differentiator: almost no retail stock tool surfaces who is lobbying,
how much they spent, and on which bills.

Same discipline as the SEC and Senate pipelines: standalone module, its own
logger, its own error containment so a failure here can never touch the other
pipelines or the app. Reads only DATABASE_URL. Lifts to AWS Lambda unchanged.

The company to ticker map (COMPANY_TICKER_MAP) is intentionally exported so the
FDA and DoD pipelines can reuse the exact same mapping, since all three need to
turn a messy corporate name into a ticker. Building it once keeps the three
pipelines consistent.

Honest note on the source: the LDA API is a public REST endpoint (JSON, despite
some docs mentioning XML) at lda.senate.gov/api/v1/. It is more stable than the
scraped sources, but it is paginated and rate aware, so we page politely and
contain every failure.
"""

import os
import re
import json
import time
import logging
import threading
from datetime import datetime, date

import requests
import psycopg2
from psycopg2.extras import Json

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] lda: %(message)s")
logger = logging.getLogger("lda_pipeline")

LDA_USER_AGENT = "ApexQ/1.0 support@apexq.io"
LDA_API = "https://lda.senate.gov/api/v1/filings/"
LDA_MAX_RPS = 2  # the LDA API is not high volume; stay gentle


# --------------------------------------------------------------------------- #
# Company -> ticker map (shared with FDA and DoD pipelines)
# --------------------------------------------------------------------------- #
# Keyed by a normalized lowercase company name fragment. Kept deliberately focused
# on large, frequently lobbying/contracting public companies rather than trying to
# be exhaustive; an unmapped client is still stored with a null ticker so no data
# is lost. Matching is done on normalized substrings so "Pfizer Inc" and "Pfizer,
# Inc." both resolve.
COMPANY_TICKER_MAP = {
    "apple": "AAPL", "microsoft": "MSFT", "amazon": "AMZN", "alphabet": "GOOGL",
    "google": "GOOGL", "meta platforms": "META", "facebook": "META", "nvidia": "NVDA",
    "tesla": "TSLA", "berkshire hathaway": "BRK.B", "jpmorgan": "JPM", "jp morgan": "JPM",
    "visa": "V", "mastercard": "MA", "walmart": "WMT", "exxon": "XOM", "exxonmobil": "XOM",
    "chevron": "CVX", "johnson & johnson": "JNJ", "johnson and johnson": "JNJ",
    "procter & gamble": "PG", "procter and gamble": "PG", "home depot": "HD",
    "bank of america": "BAC", "coca-cola": "KO", "coca cola": "KO", "pepsico": "PEP",
    "pepsi": "PEP", "broadcom": "AVGO", "eli lilly": "LLY", "lilly": "LLY",
    "abbvie": "ABBV", "merck": "MRK", "pfizer": "PFE", "thermo fisher": "TMO",
    "costco": "COST", "mcdonald": "MCD", "walt disney": "DIS", "disney": "DIS",
    "cisco": "CSCO", "accenture": "ACN", "adobe": "ADBE", "salesforce": "CRM",
    "netflix": "NFLX", "intel": "INTC", "advanced micro devices": "AMD", "amd": "AMD",
    "qualcomm": "QCOM", "texas instruments": "TXN", "oracle": "ORCL", "ibm": "IBM",
    "international business machines": "IBM", "verizon": "VZ", "at&t": "T", "at & t": "T",
    "comcast": "CMCSA", "t-mobile": "TMUS", "t mobile": "TMUS", "boeing": "BA",
    "lockheed martin": "LMT", "lockheed": "LMT", "raytheon": "RTX", "rtx": "RTX",
    "northrop grumman": "NOC", "northrop": "NOC", "general dynamics": "GD",
    "l3harris": "LHX", "l3 harris": "LHX", "honeywell": "HON", "caterpillar": "CAT",
    "deere": "DE", "john deere": "DE", "3m": "MMM", "general electric": "GE",
    "ge aerospace": "GE", "united parcel": "UPS", "ups": "UPS", "fedex": "FDX",
    "united airlines": "UAL", "delta air": "DAL", "american airlines": "AAL",
    "southwest airlines": "LUV", "goldman sachs": "GS", "morgan stanley": "MS",
    "wells fargo": "WFC", "citigroup": "C", "american express": "AXP", "amex": "AXP",
    "blackrock": "BLK", "charles schwab": "SCHW", "schwab": "SCHW", "united health": "UNH",
    "unitedhealth": "UNH", "cvs health": "CVS", "cvs": "CVS", "cigna": "CI",
    "elevance": "ELV", "humana": "HUM", "centene": "CNC", "amgen": "AMGN",
    "gilead": "GILD", "bristol-myers": "BMY", "bristol myers": "BMY", "moderna": "MRNA",
    "regeneron": "REGN", "vertex pharm": "VRTX", "biogen": "BIIB", "medtronic": "MDT",
    "abbott": "ABT", "danaher": "DHR", "stryker": "SYK", "boston scientific": "BSX",
    "intuitive surgical": "ISRG", "becton": "BDX", "chipotle": "CMG", "starbucks": "SBUX",
    "nike": "NKE", "lowe's": "LOW", "lowes": "LOW", "target": "TGT", "tjx": "TJX",
    "dollar general": "DG", "dollar tree": "DLTR", "general motors": "GM",
    "ford motor": "F", "ford": "F", "stellantis": "STLA", "paypal": "PYPL",
    "block": "SQ", "coinbase": "COIN", "palantir": "PLTR", "servicenow": "NOW",
    "intuit": "INTU", "applied materials": "AMAT", "lam research": "LRCX",
    "micron": "MU", "analog devices": "ADI", "kla": "KLAC", "synopsys": "SNPS",
    "cadence": "CDNS", "marvell": "MRVL", "coherent": "COHR", "crown castle": "CCI",
    "american tower": "AMT", "prologis": "PLD", "equinix": "EQIX", "simon property": "SPG",
    "conocophillips": "COP", "phillips 66": "PSX", "marathon petroleum": "MPC",
    "valero": "VLO", "occidental": "OXY", "schlumberger": "SLB", "halliburton": "HAL",
    "duke energy": "DUK", "southern company": "SO", "nextera": "NEE", "dominion energy": "D",
    "philip morris": "PM", "altria": "MO", "colgate": "CL", "kimberly-clark": "KMB",
    "kimberly clark": "KMB", "mondelez": "MDLZ", "kraft heinz": "KHC", "general mills": "GIS",
    "kellanova": "K", "kellogg": "K", "tyson": "TSN", "archer-daniels": "ADM",
    "archer daniels": "ADM", "sherwin-williams": "SHW", "sherwin williams": "SHW",
    "emerson electric": "EMR", "illinois tool": "ITW", "parker-hannifin": "PH",
    "eaton": "ETN", "union pacific": "UNP", "norfolk southern": "NSC", "csx": "CSX",
    "waste management": "WM", "republic services": "RSG", "automatic data": "ADP",
    "paychex": "PAYX", "moody's": "MCO", "s&p global": "SPGI", "s & p global": "SPGI",
    "cme group": "CME", "intercontinental exchange": "ICE", "nasdaq": "NDAQ",
    "aon": "AON", "marsh & mclennan": "MMC", "marsh mclennan": "MMC",
    "progressive": "PGR", "chubb": "CB", "travelers": "TRV", "allstate": "ALL",
    "metlife": "MET", "prudential financial": "PRU", "aflac": "AFL",
}


def map_company_to_ticker(name):
    """Return a ticker for a company name via normalized word-boundary match, or None.

    Shared by the FDA and DoD pipelines. Deliberately conservative: matches a known company
    fragment only on whole-word boundaries, so "Klamath Irrigation District" does NOT match the
    "kla" in KLAC, and "Berkshire" only matches as a full word. Returns None rather than a wrong
    ticker, since a false map is worse than no map.
    """
    if not name:
        return None
    norm = re.sub(r"[^a-z0-9&\s\-']", " ", str(name).lower())
    norm = re.sub(r"\s+", " ", norm).strip()
    if not norm:
        return None
    # Longest keys first so multi-word names win over shorter fragments.
    for key in sorted(COMPANY_TICKER_MAP.keys(), key=len, reverse=True):
        # Word-boundary match: the key must appear as whole word(s), not inside another word.
        # re.escape handles keys with special chars like "at&t" or "s&p global".
        pattern = r"(?<![a-z0-9])" + re.escape(key) + r"(?![a-z0-9])"
        if re.search(pattern, norm):
            return COMPANY_TICKER_MAP[key]
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


_bucket = _TokenBucket(LDA_MAX_RPS)


def _lda_get(url, params=None, timeout=30, retries=3):
    headers = {"User-Agent": LDA_USER_AGENT, "Accept": "application/json"}
    for attempt in range(retries):
        _bucket.take()
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
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
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS lobbying_disclosures (
            id SERIAL PRIMARY KEY,
            filing_uuid TEXT UNIQUE,
            registrant_name TEXT,
            client_name TEXT,
            client_ticker TEXT,
            amount NUMERIC,
            issue_description TEXT,
            specific_issues TEXT,
            filing_year INTEGER,
            filing_period TEXT,
            raw_json JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_lobby_ticker ON lobbying_disclosures(client_ticker)")
    conn.commit()
    cur.close()


def _exists(conn, filing_uuid):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM lobbying_disclosures WHERE filing_uuid = %s", (filing_uuid,))
    row = cur.fetchone()
    cur.close()
    return bool(row)


# --------------------------------------------------------------------------- #
# Parse and store
# --------------------------------------------------------------------------- #

def _to_amount(v):
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", "").replace("$", ""))
    except (ValueError, TypeError):
        return None


def _store_filing(conn, f, summary):
    try:
        uuid = f.get("filing_uuid") or f.get("url") or ""
        if not uuid or _exists(conn, uuid):
            summary["skipped_existing"] += 1
            return
        registrant = ((f.get("registrant") or {}).get("name")) or ""
        client = ((f.get("client") or {}).get("name")) or ""
        ticker = map_company_to_ticker(client)
        amount = _to_amount(f.get("income") or f.get("expenses"))
        year = f.get("filing_year")
        period = f.get("filing_period_display") or f.get("filing_period") or ""
        # Lobbying activities: collect issue codes and descriptions.
        issues = f.get("lobbying_activities") or []
        codes = []
        descs = []
        for a in issues:
            gi = a.get("general_issue_code_display") or a.get("general_issue_code") or ""
            if gi:
                codes.append(gi)
            d = a.get("description") or ""
            if d:
                descs.append(d)
        issue_description = "; ".join(sorted(set(codes)))[:500]
        specific = " | ".join(descs)[:2000]
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO lobbying_disclosures (filing_uuid, registrant_name, client_name, "
            "client_ticker, amount, issue_description, specific_issues, filing_year, filing_period, raw_json) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (filing_uuid) DO NOTHING",
            (uuid, registrant, client, ticker, amount, issue_description, specific,
             year, period, Json({"uuid": uuid})))
        conn.commit()
        cur.close()
        summary["filings"] += 1
    except Exception as e:
        logger.error("store filing error: %s", e)
        summary["errors"] += 1
        try:
            conn.rollback()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def fetch_lobbying_data(pages=3, filing_year=None):
    """Main entry point. Pull recent LDA filings and store them. Never raises.

    Args:
        pages: how many pages of the API to walk (25 filings per page by default).
        filing_year: restrict to a year; defaults to the current year.
    Returns {"filings": int, "errors": int, "skipped_existing": int, ...}.
    """
    started = time.time()
    summary = {"filings": 0, "errors": 0, "skipped_existing": 0, "pages": 0}
    conn = None
    try:
        conn = _connect()
        _create_table(conn)
        year = filing_year or date.today().year
        url = LDA_API
        params = {"filing_year": year, "page": 1}
        for p in range(1, pages + 1):
            params["page"] = p
            r = _lda_get(url, params=params)
            if r is None:
                summary["errors"] += 1
                break
            try:
                data = r.json()
            except ValueError:
                summary["errors"] += 1
                break
            results = data.get("results", []) if isinstance(data, dict) else []
            if not results:
                break
            summary["pages"] += 1
            for f in results:
                _store_filing(conn, f, summary)
            if isinstance(data, dict) and not data.get("next"):
                break
        summary["elapsed_sec"] = round(time.time() - started, 1)
        logger.info("lda run complete: %s", summary)
        return summary
    except Exception as e:
        logger.error("fetch_lobbying_data fatal: %s", e)
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
    print(json.dumps(fetch_lobbying_data(pages=2), indent=2, default=str))
