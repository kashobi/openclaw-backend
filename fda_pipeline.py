"""
fda_pipeline.py
===============

openFDA catalyst pipeline for Apex Q.

Pulls recent drug approvals and device clearances from the FDA's open data API,
maps the sponsor/applicant to a stock ticker, and stores them in PostgreSQL. FDA
decisions are hard catalysts: an approval or a clearance can move a stock more
than a quarter of earnings, and surfacing them on the report is exactly the kind
of actionable, personally relevant signal that sets Apex Q apart.

Same discipline as the SEC, Senate, and LDA pipelines: standalone module, own
logger, full error containment so a failure here never touches the others. Reads
only DATABASE_URL. Lifts to AWS Lambda unchanged.

Reuses the company to ticker map from lda_pipeline so all pipelines share one
mapping, with a safe local fallback if that import is unavailable. openFDA is a
public, documented JSON API and needs no key for this volume, though it is rate
limited to about 240 requests/minute unauthenticated, which we stay well under.
"""

import os
import re
import json
import time
import logging
import threading
from datetime import datetime, date, timedelta

import requests
import psycopg2
from psycopg2.extras import Json

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] fda: %(message)s")
logger = logging.getLogger("fda_pipeline")

FDA_USER_AGENT = "ApexQ/1.0 support@apexq.io"
FDA_DRUG_API = "https://api.fda.gov/drug/drugsfda.json"
FDA_DEVICE_API = "https://api.fda.gov/device/510k.json"
FDA_MAX_RPS = 3


# --------------------------------------------------------------------------- #
# Shared ticker map (reused from lda_pipeline, with a safe fallback)
# --------------------------------------------------------------------------- #

try:
    from lda_pipeline import map_company_to_ticker as _map_ticker
except Exception:  # pragma: no cover - fallback if lda module unavailable
    _map_ticker = None


# A small local map used only if lda_pipeline cannot be imported, so this module never
# hard-fails on a missing import. The real, larger map lives in lda_pipeline.
_FALLBACK_MAP = {
    "pfizer": "PFE", "merck": "MRK", "eli lilly": "LLY", "lilly": "LLY", "abbvie": "ABBV",
    "johnson & johnson": "JNJ", "johnson and johnson": "JNJ", "bristol-myers": "BMY",
    "bristol myers": "BMY", "amgen": "AMGN", "gilead": "GILD", "moderna": "MRNA",
    "regeneron": "REGN", "vertex": "VRTX", "biogen": "BIIB", "novartis": "NVS",
    "astrazeneca": "AZN", "glaxosmithkline": "GSK", "gsk": "GSK", "sanofi": "SNY",
    "novo nordisk": "NVO", "medtronic": "MDT", "abbott": "ABT", "boston scientific": "BSX",
    "stryker": "SYK", "intuitive surgical": "ISRG", "becton": "BDX", "edwards lifesciences": "EW",
    "roche": "RHHBY", "genentech": "RHHBY",
}


def map_sponsor_to_ticker(name):
    """Map an FDA sponsor/applicant name to a ticker using the shared LDA map, with a
    pharma-focused fallback. Uses word-boundary matching to avoid substring false positives."""
    if _map_ticker is not None:
        t = _map_ticker(name)
        if t:
            return t
    # Fallback, also word-boundary matched.
    if not name:
        return None
    norm = re.sub(r"[^a-z0-9&\s\-']", " ", str(name).lower())
    norm = re.sub(r"\s+", " ", norm).strip()
    for key in sorted(_FALLBACK_MAP.keys(), key=len, reverse=True):
        if re.search(r"(?<![a-z0-9])" + re.escape(key) + r"(?![a-z0-9])", norm):
            return _FALLBACK_MAP[key]
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


_bucket = _TokenBucket(FDA_MAX_RPS)


def _fda_get(url, params=None, timeout=30, retries=3):
    headers = {"User-Agent": FDA_USER_AGENT, "Accept": "application/json"}
    for attempt in range(retries):
        _bucket.take()
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r
            if r.status_code == 404:
                return None  # openFDA returns 404 when a search yields no results
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
        CREATE TABLE IF NOT EXISTS fda_approvals (
            id SERIAL PRIMARY KEY,
            fda_key TEXT UNIQUE,
            drug_name TEXT,
            sponsor_name TEXT,
            sponsor_ticker TEXT,
            approval_date DATE,
            indication TEXT,
            kind TEXT,
            raw_json JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fda_ticker ON fda_approvals(sponsor_ticker)")
    conn.commit()
    cur.close()


def _exists(conn, fda_key):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM fda_approvals WHERE fda_key = %s", (fda_key,))
    row = cur.fetchone()
    cur.close()
    return bool(row)


def _remap_existing_tickers(conn, summary):
    """Self-heal sponsor tickers on every run, like the LDA pipeline, so map improvements reach
    already-stored rows without manual database work."""
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, sponsor_name, sponsor_ticker FROM fda_approvals")
        rows = cur.fetchall()
        fixed = 0
        for rid, sponsor, current in rows:
            correct = map_sponsor_to_ticker(sponsor)
            if correct != current:
                cur.execute("UPDATE fda_approvals SET sponsor_ticker = %s WHERE id = %s", (correct, rid))
                fixed += 1
        conn.commit(); cur.close()
        summary["remapped"] = fixed
        if fixed:
            logger.info("remapped %s existing sponsor tickers", fixed)
    except Exception as e:
        logger.error("remap error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Parse helpers
# --------------------------------------------------------------------------- #

def _parse_fda_date(s):
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:10] if "-" in s else s[:8], fmt).date()
        except ValueError:
            continue
    return None


def _store_drug(conn, rec, summary):
    """Store one drugsfda record. Each record can have multiple products and submissions."""
    try:
        app_no = rec.get("application_number") or ""
        sponsor = rec.get("sponsor_name") or ""
        products = rec.get("products") or []
        # Approval date comes from the most recent submission with an action date.
        subs = rec.get("submissions") or []
        approval_date = None
        for s in subs:
            if s.get("submission_status") == "AP":
                d = _parse_fda_date(s.get("submission_status_date"))
                if d and (approval_date is None or d > approval_date):
                    approval_date = d
        for prod in products:
            name = prod.get("brand_name") or prod.get("generic_name") or ""
            if not name:
                continue
            key = "drug:" + app_no + ":" + name
            if _exists(conn, key):
                summary["skipped_existing"] += 1
                continue
            ticker = map_sponsor_to_ticker(sponsor)
            indication = ""
            te = prod.get("te_code") or ""
            dosage = prod.get("dosage_form") or ""
            route = prod.get("route") or ""
            indication = " ".join([x for x in [dosage, route] if x]).strip()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO fda_approvals (fda_key, drug_name, sponsor_name, sponsor_ticker, "
                "approval_date, indication, kind, raw_json) VALUES (%s,%s,%s,%s,%s,%s,'drug',%s) "
                "ON CONFLICT (fda_key) DO NOTHING",
                (key, name, sponsor, ticker, approval_date, indication, Json({"app": app_no})))
            conn.commit(); cur.close()
            summary["approvals"] += 1
    except Exception as e:
        logger.error("store drug error: %s", e)
        summary["errors"] += 1
        try:
            conn.rollback()
        except Exception:
            pass


def _store_device(conn, rec, summary):
    """Store one 510(k) device clearance record."""
    try:
        kno = rec.get("k_number") or ""
        if not kno:
            return
        key = "device:" + kno
        if _exists(conn, key):
            summary["skipped_existing"] += 1
            return
        name = rec.get("device_name") or ""
        sponsor = rec.get("applicant") or ""
        approval_date = _parse_fda_date(rec.get("decision_date"))
        ticker = map_sponsor_to_ticker(sponsor)
        indication = rec.get("advisory_committee_description") or ""
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO fda_approvals (fda_key, drug_name, sponsor_name, sponsor_ticker, "
            "approval_date, indication, kind, raw_json) VALUES (%s,%s,%s,%s,%s,%s,'device',%s) "
            "ON CONFLICT (fda_key) DO NOTHING",
            (key, name, sponsor, ticker, approval_date, indication, Json({"k": kno})))
        conn.commit(); cur.close()
        summary["approvals"] += 1
    except Exception as e:
        logger.error("store device error: %s", e)
        summary["errors"] += 1
        try:
            conn.rollback()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def fetch_fda_approvals(days_back=30, limit=100):
    """Main entry point. Pull recent drug approvals and device clearances. Never raises.

    Returns {"approvals": int, "errors": int, "skipped_existing": int, ...}.
    """
    started = time.time()
    summary = {"approvals": 0, "errors": 0, "skipped_existing": 0, "drug": 0, "device": 0}
    conn = None
    try:
        conn = _connect()
        _create_table(conn)
        _remap_existing_tickers(conn, summary)

        start = (date.today() - timedelta(days=days_back)).strftime("%Y%m%d")
        end = date.today().strftime("%Y%m%d")

        # Drugs: recently approved applications. Search submissions by approval date.
        drug_params = {
            "search": "submissions.submission_status_date:[%s TO %s]" % (start, end),
            "limit": limit,
        }
        r = _fda_get(FDA_DRUG_API, params=drug_params)
        if r is not None:
            try:
                results = r.json().get("results", [])
            except ValueError:
                results = []
            for rec in results:
                _store_drug(conn, rec, summary)
            summary["drug"] = len(results)

        # Devices: recent 510(k) clearances by decision date.
        dev_params = {
            "search": "decision_date:[%s TO %s]" % (start, end),
            "limit": limit,
        }
        r2 = _fda_get(FDA_DEVICE_API, params=dev_params)
        if r2 is not None:
            try:
                dresults = r2.json().get("results", [])
            except ValueError:
                dresults = []
            for rec in dresults:
                _store_device(conn, rec, summary)
            summary["device"] = len(dresults)

        summary["elapsed_sec"] = round(time.time() - started, 1)
        logger.info("fda run complete: %s", summary)
        return summary
    except Exception as e:
        logger.error("fetch_fda_approvals fatal: %s", e)
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
    print(json.dumps(fetch_fda_approvals(days_back=60), indent=2, default=str))
