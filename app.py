from flask import Flask, jsonify, request, Response, session, redirect
from flask_cors import CORS
import yfinance as yf
import requests
import os
import traceback
import time
import json
import re
import html
import logging
import csv
import threading
import random
import io
from functools import wraps
from datetime import datetime, timedelta

# ============ STRIPE PAYMENTS ============
# Guarded import so a missing package never crashes boot. Add `stripe` to requirements.txt and
# set the four STRIPE_ env vars. Until that is done, checkout returns a clean error, not a boot crash.
try:
    import stripe as _stripe
    _sk = os.environ.get("STRIPE_SECRET_KEY", "")
    if _sk:
        _stripe.api_key = _sk
except Exception:
    _stripe = None
STRIPE_PUBLISHABLE = os.environ.get("STRIPE_PUBLISHABLE_KEY", "")
STRIPE_PRICE_ID = os.environ.get("STRIPE_PRICE_ID", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
# ============ END STRIPE PAYMENTS ============

# Twilio SMS and phone verification were removed. Signup no longer collects a phone number.

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# SECURITY: rate limiting on sensitive routes. Imported defensively so that if the package is
# somehow missing in an environment, the app still boots and simply runs without the limit rather
# than crashing. default_limits is empty so ONLY routes explicitly decorated below are limited;
# every other route is untouched. Storage is in process memory, which is right for a single
# instance. A multi instance deployment would point storage_uri at Redis instead.
try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
    limiter = Limiter(key_func=get_remote_address, app=app, default_limits=[], storage_uri="memory://")
    login_limit = limiter.limit("5 per minute")
    _LIMITER_ON = True
except Exception as _limiter_err:
    logger.warning("flask_limiter unavailable, login rate limiting disabled: %s" % _limiter_err)
    limiter = None
    _LIMITER_ON = False
    def login_limit(f):
        return f


# SECURITY: a custom, friendly 429 for the login limit. Login is the only rate limited route, so
# any 429 in this app comes from that limit and this message is the right one.
@app.errorhandler(429)
def _ratelimit_handler(e):
    return jsonify({"error": "Too many login attempts. Try again shortly."}), 429


FINNHUB_KEY = os.environ.get("FINNHUB_KEY", "")
QUIVER_KEY = os.environ.get("QUIVER_KEY", "")
GEMINI_KEY = os.environ.get("GEMINI_KEY", "")
DEEPSEEK_KEY = os.environ.get("DEEPSEEK_KEY") or os.environ.get("DeepSeek") or ""
FMP_KEY = os.environ.get("FMP_KEY", "")
FMP_BASE = "https://financialmodelingprep.com"


def fmp_get(path):
    # Safe FMP call. Reads the key from the environment, never raises, and returns None on
    # any failure so the main report is never affected if FMP is missing or a plan limits it.
    if not FMP_KEY:
        return None
    try:
        sep = "&" if "?" in path else "?"
        url = FMP_BASE + path + sep + "apikey=" + FMP_KEY
        r = requests.get(url, timeout=8)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        logger.error("fmp_get error %s: %s" % (path, e))
    return None

CACHE = {}
CACHE_TTL = 60 * 15   # 15 minutes


# ---------- data validation guardrails ----------
# Quarantine bad data before it ever reaches a report. The rule of the whole app is accuracy over
# completeness: a wrong number is far worse than a missing one. These checks catch the classic bad
# data that scrapers produce: future-dated trades, impossible price jumps, market caps that do not
# reconcile with shares outstanding.

def _valid_trade_date(trade_date, filing_date=None):
    """A trade/disclosure date must not be in the future, and a filing cannot predate the trade.
    Returns True if the record is safe to display."""
    from datetime import date as _d, datetime as _dt
    def _asdate(x):
        if x is None:
            return None
        if isinstance(x, _dt):
            return x.date()
        if isinstance(x, _d):
            return x
        try:
            return _dt.strptime(str(x)[:10], "%Y-%m-%d").date()
        except Exception:
            return None
    td = _asdate(trade_date)
    fd = _asdate(filing_date)
    today = _d.today()
    if td and td > today:
        return False  # trade in the future: impossible, quarantine
    if fd and fd > today:
        return False  # filed in the future: impossible
    if td and fd and fd < td:
        return False  # disclosed before it happened: impossible
    return True


def _price_move_sane(price, prev_close, has_catalyst=False):
    """A single-session move above 20% without a known catalyst/halt is suspect (likely a bad tick
    or a split not adjusted). Returns (ok, pct). ok=False means flag/quarantine for review."""
    try:
        p = float(price); pc = float(prev_close)
        if pc <= 0:
            return True, 0.0
        pct = (p - pc) / pc * 100.0
        if abs(pct) > 20 and not has_catalyst:
            return False, round(pct, 2)
        return True, round(pct, 2)
    except (TypeError, ValueError):
        return True, 0.0


def _reconcile_market_cap(price, shares_outstanding, reported_cap):
    """Market cap should equal price * shares within ~2%. If it does not, trust shares (from SEC/
    fundamentals) and recompute, rather than displaying a mismatched figure. Returns a cap or None."""
    try:
        p = float(price); sh = float(shares_outstanding)
        computed = p * sh
        if reported_cap:
            rc = float(reported_cap)
            if rc > 0 and abs(computed - rc) / rc <= 0.02:
                return rc  # within tolerance, reported is fine
        return computed if computed > 0 else (float(reported_cap) if reported_cap else None)
    except (TypeError, ValueError):
        try:
            return float(reported_cap) if reported_cap else None
        except (TypeError, ValueError):
            return None




def get_cache(key):
    if key in CACHE:
        data, ts = CACHE[key]
        if time.time() - ts < CACHE_TTL:
            return data
    return None

def set_cache(key, data):
    CACHE[key] = (data, time.time())


# ============ BACKEND UPGRADE: Secondary data source (Finnhub) for real-time prices ============
# Finnhub provides real-time quotes when yfinance data is delayed (15 min on many symbols).
# A simple in-process rate limiter keeps us under the free-tier 60 calls/minute cap.
# This is additive: if FINNHUB_KEY is unset or the call fails, every existing code path
# falls back to yfinance exactly as before. No scoring, caching, or route logic changes.
_FINNHUB_CALLS = []
FINNHUB_RATE_LIMIT = 55  # Stay safely under 60/min on the free tier

def _finnhub_rate_ok():
    """Returns True if we have budget for another Finnhub call in this minute window."""
    now = time.time()
    global _FINNHUB_CALLS
    _FINNHUB_CALLS = [t for t in _FINNHUB_CALLS if now - t < 60.0]
    return len(_FINNHUB_CALLS) < FINNHUB_RATE_LIMIT

def fetch_finnhub_quote(symbol):
    """Real-time quote from Finnhub. Returns dict with price, prev_close, change_pct, source
    or None on any failure. Never raises. Cached for 30 seconds per symbol to avoid redundant
    calls within rapid refresh cycles."""
    if not FINNHUB_KEY or not _finnhub_rate_ok():
        return None
    ckey = "fhq_" + symbol
    cached = CACHE.get(ckey)
    if cached and (time.time() - cached[1]) < 30:
        return cached[0]
    _FINNHUB_CALLS.append(time.time())
    try:
        url = "https://finnhub.io/api/v1/quote?symbol=%s&token=%s" % (symbol, FINNHUB_KEY)
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            d = r.json()
            if d and isinstance(d.get("c"), (int, float)) and d["c"] > 0:
                price = round(float(d["c"]), 2)
                prev_close = round(float(d.get("pc", price)), 2)
                change = round(float(d.get("d", 0)), 2)
                change_pct = round(float(d.get("dp", 0)), 2)
                result = {
                    "price": price,
                    "change": change,
                    "change_pct": change_pct,
                    "prev_close": prev_close,
                    "source": "finnhub",
                }
                CACHE[ckey] = (result, time.time())
                return result
    except Exception as e:
        logger.error("fetch_finnhub_quote %s: %s" % (symbol, e))
    return None

def get_realtime_price(symbol):
    """Tries Finnhub first for real-time, falls back to yfinance. Returns price dict or None.
    Used by the SSE streaming endpoint and the new market-snapshot endpoint. The existing
    light_score and compute_full_report functions have their own inline Finnhub integration
    so their scoring logic is untouched."""
    fq = fetch_finnhub_quote(symbol)
    if fq:
        return fq
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="5d", timeout=10)
        if hist.empty:
            return None
        cur = fmt_price(hist["Close"].iloc[-1])
        prev = fmt_price(hist["Close"].iloc[-2]) if len(hist) > 1 else cur
        chg = round(((cur - prev) / prev) * 100, 2) if prev else 0
        return {
            "price": cur,
            "change": round(cur - prev, 2) if prev else 0,
            "change_pct": chg,
            "prev_close": prev,
            "source": "yfinance",
        }
    except Exception as e:
        logger.error("get_realtime_price %s: %s" % (symbol, e))
    return None
# =========================================================================================


# CHUNK: send the bare domain to www, a backup in case the Porkbun forward misses
@app.before_request
def force_www():
    host = (request.host or "").split(":")[0].lower()
    if host == "apexq.io":
        return redirect(request.url.replace("://apexq.io", "://www.apexq.io", 1), code=301)


import gzip as _gzip


# CHUNK: gzip every sizable text response when the browser accepts it. The single index.html is
# 289KB raw and roughly 60KB gzipped, so this is the single biggest page load win. Registered
# BEFORE the data_timestamp handler on purpose: Flask runs after_request hooks in reverse
# registration order, so this one runs last, compressing only after the JSON has been stamped.
@app.after_request
def compress_response(response):
    try:
        if response.direct_passthrough or response.status_code != 200:
            return response
        if "gzip" not in (request.headers.get("Accept-Encoding") or "").lower():
            return response
        if response.headers.get("Content-Encoding"):
            return response
        ct = (response.content_type or "").lower()
        if not any(t in ct for t in ("text/html", "application/json", "application/javascript", "text/css", "text/plain", "image/svg")):
            return response
        body = response.get_data()
        if len(body) < 1024:
            return response
        response.set_data(_gzip.compress(body, 6))
        response.headers["Content-Encoding"] = "gzip"
        response.headers["Content-Length"] = str(len(response.get_data()))
        response.headers["Vary"] = "Accept-Encoding"
    except Exception as e:
        logger.error("gzip compress error: %s" % e)
    return response


# CHUNK: stamp every JSON response with data_timestamp if it does not already carry one.
# Cached endpoints embed their own fetch time, which is preserved here, so the frontend's
# "Updated X ago" reflects when the data was actually pulled, not when it was served.
@app.after_request
def add_data_timestamp(response):
    try:
        ct = response.content_type or ""
        if ct.startswith("application/json"):
            payload = response.get_json(silent=True)
            if isinstance(payload, dict) and "data_timestamp" not in payload:
                payload["data_timestamp"] = int(time.time())
                response.set_data(json.dumps(payload))
    except Exception as e:
        logger.error("data_timestamp stamp error: %s" % e)
    return response


# ---------- Database and accounts ----------
import secrets as _secrets
try:
    import psycopg2
except Exception as _e:
    psycopg2 = None
    logger.error("psycopg2 not available: %s" % _e)
from werkzeug.security import generate_password_hash, check_password_hash

DATABASE_URL = os.environ.get("DATABASE_URL", "")


def get_db():
    if not DATABASE_URL or psycopg2 is None:
        return None
    return psycopg2.connect(DATABASE_URL)


def ensure_db():
    conn = get_db()
    if conn is None:
        logger.error("ensure_db: no database connection")
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS users ("
            "id SERIAL PRIMARY KEY,"
            "username TEXT UNIQUE NOT NULL,"
            "password_hash TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT NOW())"
        )
        # Premium tier column, added in place so existing user rows keep working.
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS tier TEXT DEFAULT 'free'")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS app_settings ("
            "key TEXT PRIMARY KEY,"
            "value TEXT NOT NULL)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS watchlist ("
            "id SERIAL PRIMARY KEY,"
            "user_id INTEGER NOT NULL,"
            "symbol TEXT NOT NULL,"
            "name TEXT,"
            "added_at TIMESTAMP DEFAULT NOW(),"
            "UNIQUE (user_id, symbol))"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS holdings ("
            "id SERIAL PRIMARY KEY,"
            "user_id INTEGER NOT NULL,"
            "symbol TEXT NOT NULL,"
            "shares NUMERIC NOT NULL,"
            "avg_cost NUMERIC NOT NULL,"
            "added_at TIMESTAMP DEFAULT NOW(),"
            "UNIQUE (user_id, symbol))"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS recent_searches ("
            "id SERIAL PRIMARY KEY,"
            "user_id INTEGER NOT NULL,"
            "symbol TEXT NOT NULL,"
            "viewed_at TIMESTAMP DEFAULT NOW(),"
            "UNIQUE (user_id, symbol))"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS verdict_history ("
            "id SERIAL PRIMARY KEY,"
            "user_id INTEGER NOT NULL,"
            "symbol TEXT NOT NULL,"
            "last_verdict TEXT,"
            "last_checked TIMESTAMP DEFAULT NOW(),"
            "UNIQUE (user_id, symbol))"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS alert_subscriptions ("
            "user_id INTEGER NOT NULL,"
            "email TEXT,"
            "verified INTEGER DEFAULT 0,"
            "alert_prefs TEXT DEFAULT 'all',"
            "UNIQUE (user_id))"
        )
        # User data capture, phone verification, and paper trading columns, all added in place
        # so existing rows keep working. Email is unique but nullable, so old accounts stay valid.
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT UNIQUE")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name TEXT")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS phone TEXT")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS phone_verified BOOLEAN DEFAULT false")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS paper_cash NUMERIC DEFAULT 1000000")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS snaptrade_secret TEXT")
        # One time migration to the one million bankroll. The old default was 100000, and accounts
        # that traded against that baseline would show nonsense profit math against the new number,
        # so every practice account resets to a clean 1000000 and its practice trades clear. The
        # app_meta marker makes this idempotent: it runs exactly once, never again on redeploys.
        cur.execute("CREATE TABLE IF NOT EXISTS app_meta (key TEXT PRIMARY KEY, value TEXT)")
        cur.execute("SELECT 1 FROM app_meta WHERE key = 'paper_one_million'")
        if cur.fetchone() is None:
            # On a brand new database this runs before paper_trades exists, so check first.
            cur.execute("SELECT to_regclass('paper_trades')")
            if cur.fetchone()[0] is not None:
                cur.execute("DELETE FROM paper_trades")
            cur.execute("UPDATE users SET paper_cash = 1000000")
            cur.execute("INSERT INTO app_meta (key, value) VALUES ('paper_one_million', 'done')")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS paper_trades ("
            "id SERIAL PRIMARY KEY,"
            "user_id INTEGER,"
            "symbol TEXT,"
            "shares NUMERIC,"
            "buy_price NUMERIC,"
            "buy_date TIMESTAMP DEFAULT NOW(),"
            "sold BOOLEAN DEFAULT false,"
            "sell_price NUMERIC,"
            "sell_date TIMESTAMP)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS user_alerts ("
            "id SERIAL PRIMARY KEY,"
            "user_id INTEGER NOT NULL,"
            "alert_type TEXT NOT NULL,"
            "symbol TEXT,"
            "politician_name TEXT,"
            "threshold_price NUMERIC,"
            "direction TEXT,"
            "enabled BOOLEAN DEFAULT true,"
            "created_at TIMESTAMP DEFAULT NOW())"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS alert_log ("
            "id SERIAL PRIMARY KEY,"
            "user_id INTEGER NOT NULL,"
            "alert_type TEXT,"
            "symbol TEXT,"
            "message TEXT,"
            "reason TEXT,"
            "link TEXT,"
            "triggered_at TIMESTAMP DEFAULT NOW(),"
            "seen BOOLEAN DEFAULT false)"
        )
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS onesignal_token TEXT")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS insider_clusters ("
            "id SERIAL PRIMARY KEY,"
            "ticker TEXT NOT NULL,"
            "company_name TEXT,"
            "unique_insiders_count INTEGER DEFAULT 1,"
            "total_capital_deployed NUMERIC DEFAULT 0,"
            "executive_roles TEXT[],"
            "insider_names TEXT[],"
            "window_start_date DATE,"
            "window_end_date DATE,"
            "last_alert_count INTEGER DEFAULT 0,"
            "is_resolved BOOLEAN DEFAULT false,"
            "created_at TIMESTAMP DEFAULT NOW())"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS morning_briefings ("
            "id SERIAL PRIMARY KEY,"
            "user_id INTEGER NOT NULL,"
            "date TEXT,"
            "content TEXT,"
            "created_at TIMESTAMP DEFAULT NOW(),"
            "UNIQUE (user_id, date))"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS committee_assignments ("
            "politician_name TEXT,"
            "committee TEXT)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS user_sector_weights ("
            "id SERIAL PRIMARY KEY,"
            "user_id INTEGER NOT NULL,"
            "sector TEXT,"
            "weight NUMERIC,"
            "source TEXT DEFAULT 'snaptrade',"
            "updated_at TIMESTAMP DEFAULT NOW())"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS ai_cache ("
            "id SERIAL PRIMARY KEY,"
            "cache_key TEXT UNIQUE,"
            "response TEXT,"
            "audio_path TEXT,"
            "created_at TIMESTAMP DEFAULT NOW())"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS user_devices ("
            "id SERIAL PRIMARY KEY,"
            "user_id INTEGER NOT NULL,"
            "device_token TEXT,"
            "platform TEXT DEFAULT 'web',"
            "created_at TIMESTAMP DEFAULT NOW(),"
            "UNIQUE (user_id, device_token))"
        )
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS agreed_tos BOOLEAN DEFAULT false")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS email_queue ("
            "id SERIAL PRIMARY KEY,"
            "user_id INTEGER,"
            "subject TEXT,"
            "body TEXT,"
            "sent BOOLEAN DEFAULT false,"
            "created_at TIMESTAMP DEFAULT NOW())"
        )
        conn.commit()
        cur.close()
        logger.info("ensure_db: tables ready")
        # Each create_tables call gets its own try/except so one hanging DDL
        # cannot block the entire app from booting. Tables that fail to create
        # at boot will be retried on the next request that needs them.
        for _tbl_fn in [snap_create_tables, fs_create_tables, trial_create_tables,
                        convergence_create_tables, maturity_create_tables, signals_create_tables]:
            try:
                _tbl_fn()
            except Exception as _sce:
                logger.warning("table create at boot (%s): %s" % (_tbl_fn.__name__, _sce))
    except Exception as e:
        logger.error("ensure_db error: %s" % e)
    finally:
        conn.close()


def get_secret_key():
    # Set SECRET_KEY in Railway to a permanent value to prevent user logout on redeploy.
    # If the env var is missing, a value is generated once and stored in app_settings, so it stays
    # stable across redeploys rather than logging everyone out on each deploy.
    env_secret = os.environ.get("SECRET_KEY", "")
    if env_secret:
        return env_secret
    conn = get_db()
    if conn is None:
        return "apexq-temporary-dev-secret"
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM app_settings WHERE key = 'secret_key'")
        row = cur.fetchone()
        if row:
            cur.close()
            return row[0]
        new_secret = _secrets.token_hex(32)
        cur.execute(
            "INSERT INTO app_settings (key, value) VALUES ('secret_key', %s) "
            "ON CONFLICT (key) DO NOTHING",
            (new_secret,),
        )
        conn.commit()
        cur.execute("SELECT value FROM app_settings WHERE key = 'secret_key'")
        row = cur.fetchone()
        cur.close()
        return row[0] if row else new_secret
    except Exception as e:
        logger.error("get_secret_key error: %s" % e)
        return "apexq-temporary-dev-secret"
    finally:
        conn.close()


try:
    ensure_db()
except Exception as _e:
    logger.error("startup ensure_db failed: %s" % _e)

app.secret_key = get_secret_key()
app.config.update(
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=60 * 60 * 24 * 30,
)


def current_user():
    uid = session.get("user_id")
    uname = session.get("username")
    if uid and uname:
        return {"id": uid, "username": uname, "tier": session.get("tier", "free")}
    return None


# ============ PREMIUM TIER ============
# Free users get a small daily allowance of scans and questions and up to five portfolio
# holdings. Premium unlocks unlimited use plus data export. The tier lives on the users row and
# is mirrored into the session at login. Daily usage is tracked in a simple in process dict that
# resets when the calendar day changes. Note: this counter is per process, so if the app is ever
# run with multiple workers each worker keeps its own count.
FREE_DAILY_SCANS = 3
FREE_DAILY_ASKS = 3
_USAGE = {"date": None, "scan": {}, "ask": {}}

# Master switch for every free tier limit. While building and before marketing, keep this OFF so
# nothing blocks testing: the daily scan and question caps, the premium only features, and the five
# holdings cap are all lifted.
#
# PAUSED FOR NOW. This is hard set to False so nothing, including any leftover Railway environment
# variable, can turn the paywall on by accident. When you are ready to market Apex Q and switch the
# limits on, change False to True on the line below and redeploy. That one edit is the whole switch.
FREE_LIMITS_ENABLED = False

def is_premium(u):
    return bool(u and u.get("tier") == "premium")

def _usage_today():
    today = time.strftime("%Y-%m-%d")
    if _USAGE["date"] != today:
        _USAGE["date"] = today
        _USAGE["scan"] = {}
        _USAGE["ask"] = {}
    return _USAGE

def _usage_key(u):
    if u and u.get("id"):
        return "u%s" % u["id"]
    fwd = request.headers.get("X-Forwarded-For", "")
    ip = fwd.split(",")[0].strip() if fwd else (request.remote_addr or "anon")
    return "ip:" + ip

def usage_limit(kind):
    return FREE_DAILY_SCANS if kind == "scan" else FREE_DAILY_ASKS

def usage_count(kind, u):
    return _usage_today()[kind].get(_usage_key(u), 0)

def usage_gate(kind):
    """For a free user at or over the daily limit, returns a 402 response tuple. Otherwise it
    counts this use and returns None. Premium users are never limited and never counted."""
    if not FREE_LIMITS_ENABLED:
        return None
    u = current_user()
    if is_premium(u):
        return None
    d = _usage_today()
    key = _usage_key(u)
    used = d[kind].get(key, 0)
    if used >= usage_limit(kind):
        word = "scans" if kind == "scan" else "questions"
        return jsonify({"error": "premium_required",
                        "message": "You have used your %d free %s for today. Upgrade to Premium for unlimited." % (usage_limit(kind), word)}), 402
    d[kind][key] = used + 1
    return None

def require_premium(f):
    @wraps(f)
    def _wrap(*args, **kwargs):
        if FREE_LIMITS_ENABLED and not is_premium(current_user()):
            return jsonify({"error": "premium_required",
                            "message": "Upgrade to Premium to use this feature."}), 402
        return f(*args, **kwargs)
    return _wrap
# ============ END PREMIUM TIER ============


INDEX_ALIASES = {
    "SPX": "^GSPC", "SP500": "^GSPC", "S&P500": "^GSPC", "S&P 500": "^GSPC", "GSPC": "^GSPC",
    "^SPX": "^GSPC", "^GSPC": "^GSPC", "SPY500": "^GSPC",
    "NASDAQ": "^IXIC", "IXIC": "^IXIC", "^IXIC": "^IXIC", "NDX": "^IXIC", "NASDAQ COMPOSITE": "^IXIC",
    "DOW": "^DJI", "DOWJONES": "^DJI", "DOW JONES": "^DJI", "DJIA": "^DJI", "DJI": "^DJI", "^DJI": "^DJI",
    "RUSSELL": "^RUT", "RUSSELL2000": "^RUT", "RUSSELL 2000": "^RUT", "RUT": "^RUT", "^RUT": "^RUT",
    "VIX": "^VIX", "^VIX": "^VIX", "FEAR": "^VIX", "FEAR INDEX": "^VIX", "VOLATILITY": "^VIX",
    "GOLD": "GC=F", "GOLD FUTURES": "GC=F", "GC=F": "GC=F", "GC": "GC=F", "XAU": "GC=F",
    "SILVER": "SI=F", "SI=F": "SI=F",
    "OIL": "CL=F", "WTI": "CL=F", "OIL WTI": "CL=F", "CRUDE": "CL=F", "CRUDE OIL": "CL=F", "CL=F": "CL=F",
    "NATGAS": "NG=F", "NATURAL GAS": "NG=F", "NG=F": "NG=F",
    "COPPER": "HG=F", "HG=F": "HG=F",
    "BITCOIN": "BTC-USD", "BTC": "BTC-USD", "BTC-USD": "BTC-USD",
    "ETHEREUM": "ETH-USD", "ETH": "ETH-USD", "ETH-USD": "ETH-USD",
}


def resolve_ticker(query):
    query = query.strip()
    # Indices, futures, and crypto majors resolve directly, before any search. yfinance search
    # mangles caret symbols and friendly names (VIX, GOLD, OIL) into the wrong instrument, which is
    # what caused every index to show one identical wrong price. Direct mapping fixes that.
    up = query.upper()
    if up in INDEX_ALIASES:
        return INDEX_ALIASES[up]
    if up.startswith("^") or "=F" in up or up.endswith("-USD"):
        return up
    # If the query already LOOKS like a ticker (short, no spaces, letters plus maybe a dot or dash),
    # use it directly. Never round-trip it through Yahoo's search, which is unreliable from a cloud
    # IP and has returned faulty data for plain symbols like AAPL, resolving them into nothing and
    # breaking the whole report. Searching is only for company-name queries.
    if re.match(r"^[A-Z]{1,6}([.\-][A-Z]{1,4})?$", up):
        return up
    try:
        s = yf.Search(query, max_results=1)
        if s.quotes:
            return s.quotes[0].get("symbol", query.upper())
    except Exception:
        pass
    return query.upper()


# CHUNK: Ask/Compare name resolution helper. Real tickers are short, no spaces, letters with
# maybe a dot, dash, caret, or equals (BRK.B, BTC-USD, ^GSPC, GC=F). Anything else is a name.
def looks_like_ticker(s):
    s = (s or "").strip()
    if not s or " " in s or len(s) > 6:
        return False
    return bool(re.match(r"^[A-Za-z.\-\^=]+$", s))

def fmt_price(val):
    try:
        return round(float(val), 2)
    except:
        return val

def score_to_conviction(score):
    if score >= 8:
        return "Very High"
    elif score >= 5:
        return "High"
    elif score >= 3:
        return "Moderate"
    elif score >= 1:
        return "Low"
    else:
        return "Very Low"


@app.route("/cron/house-trades")
def cron_house_trades():
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    try:
        from house_pipeline import fetch_house_trades
    except Exception as e:
        logger.error("house import failed: %s" % e)
        return jsonify({"error": "house pipeline unavailable"}), 503
    try:
        return jsonify(fetch_house_trades(max_filings=int(request.args.get("max", 40))))
    except Exception as e:
        logger.error("house run error: %s" % e)
        return jsonify({"error": str(e)}), 500


@app.route("/cron/fda-approvals")
def cron_fda_approvals():
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    try:
        from fda_pipeline import fetch_fda_approvals
    except Exception as e:
        logger.error("fda import failed: %s" % e)
        return jsonify({"error": "fda pipeline unavailable"}), 503
    try:
        return jsonify(fetch_fda_approvals(days_back=30))
    except Exception as e:
        logger.error("fda run error: %s" % e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/fda")
def api_fda():
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"symbol": "", "approvals": []})
    ck = "fda_" + symbol
    cached = CACHE.get(ck)
    if cached and (time.time() - cached[1]) < 900:
        return jsonify(cached[0])
    out = {"symbol": symbol, "approvals": []}
    conn = get_db()
    if conn is None:
        return jsonify(out)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT drug_name, sponsor_name, approval_date, indication, kind "
            "FROM fda_approvals WHERE sponsor_ticker = %s "
            "ORDER BY approval_date DESC NULLS LAST LIMIT 15", (symbol,))
        for r in cur.fetchall():
            out["approvals"].append({
                "drug_name": r[0], "sponsor_name": r[1],
                "approval_date": r[2].isoformat() if r[2] else None,
                "indication": r[3], "kind": r[4],
            })
        cur.close(); conn.close()
    except Exception as e:
        logger.error("api_fda: %s" % e)
        try:
            conn.close()
        except Exception:
            pass
        return jsonify(out)
    CACHE[ck] = (out, time.time())
    return jsonify(out)


@app.route("/cron/lda-lobbying")
def cron_lda_lobbying():
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    try:
        from lda_pipeline import fetch_lobbying_data
    except Exception as e:
        logger.error("lda import failed: %s" % e)
        return jsonify({"error": "lda pipeline unavailable"}), 503
    try:
        return jsonify(fetch_lobbying_data(pages=3))
    except Exception as e:
        logger.error("lda run error: %s" % e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/lobbying")
def api_lobbying():
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"symbol": "", "records": []})
    ck = "lobbying_" + symbol
    cached = CACHE.get(ck)
    if cached and (time.time() - cached[1]) < 900:
        return jsonify(cached[0])
    out = {"symbol": symbol, "records": []}
    conn = get_db()
    if conn is None:
        return jsonify(out)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT registrant_name, client_name, amount, issue_description, filing_year, filing_period "
            "FROM lobbying_disclosures WHERE client_ticker = %s "
            "ORDER BY filing_year DESC, id DESC LIMIT 10", (symbol,))
        for r in cur.fetchall():
            out["records"].append({
                "registrant_name": r[0], "client_name": r[1],
                "amount": float(r[2]) if r[2] is not None else None,
                "issue_description": r[3], "filing_year": r[4], "filing_period": r[5],
            })
        cur.close(); conn.close()
    except Exception as e:
        logger.error("api_lobbying: %s" % e)
        try:
            conn.close()
        except Exception:
            pass
        return jsonify(out)
    CACHE[ck] = (out, time.time())
    return jsonify(out)


@app.route("/cron/senate-trades")
def cron_senate_trades():
    # Token gate, same pattern as the SEC cron. Independent of SEC entirely.
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    # Local import so a missing or broken Senate module never breaks app startup. A Senate
    # failure is fully contained here and cannot touch the SEC pipeline or anything else.
    try:
        from senate_efd_pipeline import fetch_senate_trades
    except Exception as e:
        logger.error("senate import failed: %s" % e)
        return jsonify({"error": "senate pipeline unavailable"}), 503
    try:
        return jsonify(fetch_senate_trades(days_back=3))
    except Exception as e:
        logger.error("senate run error: %s" % e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/senate-trades")
def api_senate_trades():
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"symbol": "", "trades": []})
    ck = "senate_trades_" + symbol
    cached = CACHE.get(ck)
    if cached and (time.time() - cached[1]) < 900:
        return jsonify(cached[0])
    out = {"symbol": symbol, "trades": []}
    conn = get_db()
    if conn is None:
        return jsonify(out)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT politician_name, party, state, transaction_type, amount, trade_date, filing_date "
            "FROM congressional_trades_senate WHERE ticker = %s AND ticker IS NOT NULL "
            "ORDER BY trade_date DESC NULLS LAST LIMIT 50", (symbol,))
        for r in cur.fetchall():
            out["trades"].append({
                "politician_name": r[0], "party": r[1], "state": r[2],
                "transaction_type": r[3], "amount": r[4],
                "trade_date": r[5].isoformat() if r[5] else None,
                "filing_date": r[6].isoformat() if r[6] else None,
            })
        cur.close(); conn.close()
    except Exception as e:
        # Table may not exist yet, or any query issue: return empty gracefully, never error.
        logger.error("api_senate_trades: %s" % e)
        try:
            conn.close()
        except Exception:
            pass
        return jsonify(out)
    CACHE[ck] = (out, time.time())
    return jsonify(out)


@app.route("/")
def home():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    html = open(path, encoding="utf-8").read()
    # Inject the OneSignal App ID from the environment so push works without hardcoding it. When
    # no ID is set, the placeholder stays and the SDK init self skips.
    osid = os.environ.get("ONESIGNAL_APP_ID", "").strip()
    if osid:
        html = html.replace("__ONESIGNAL_APP_ID__", osid)
    resp = Response(html, mimetype="text/html")
    # Revalidate on every visit so a deploy is picked up immediately, but allow the browser to
    # reuse its copy when nothing changed instead of re downloading the whole app shell.
    resp.headers["Cache-Control"] = "no-cache"
    return resp


def _serve_file(filename, mimetype, binary=False):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    if binary:
        resp = Response(open(path, "rb").read(), mimetype=mimetype)
    else:
        resp = Response(open(path, encoding="utf-8").read(), mimetype=mimetype)
    # Icons and the manifest change rarely; a week of browser cache removes them from every
    # repeat load. The service worker stays at no-cache so updates roll out immediately.
    if filename == "sw.js":
        resp.headers["Cache-Control"] = "no-cache"
    else:
        resp.headers["Cache-Control"] = "public, max-age=604800"
    return resp


@app.route("/manifest.json")
def manifest():
    return _serve_file("manifest.json", "application/json")


@app.route("/sw.js")
def service_worker():
    return _serve_file("sw.js", "application/javascript")


@app.route("/icon-192.png")
def icon_192():
    return _serve_file("icon-192.png", "image/png", binary=True)


@app.route("/icon-512.png")
def icon_512():
    return _serve_file("icon-512.png", "image/png", binary=True)


@app.route("/apple-touch-icon.png")
def apple_touch_icon():
    return _serve_file("apple-touch-icon.png", "image/png", binary=True)


@app.route("/auth/me")
def auth_me():
    u = current_user()
    if u:
        # Refresh tier from the database so the badge and limits are always correct,
        # even for sessions created before the tier column existed.
        conn = get_db()
        if conn is not None:
            try:
                cur = conn.cursor()
                cur.execute("SELECT COALESCE(tier, 'free'), phone, COALESCE(phone_verified, false), email, first_name, COALESCE(agreed_tos, false) FROM users WHERE id = %s", (u["id"],))
                row = cur.fetchone()
                cur.close()
                if row:
                    session["tier"] = row[0]
                    u["tier"] = row[0]
                    u["phone"] = row[1]
                    u["phone_verified"] = bool(row[2])
                    u["email"] = row[3]
                    u["first_name"] = row[4]
                    u["agreed_tos"] = bool(row[5])
            except Exception as e:
                logger.error("auth_me tier error: %s" % e)
            finally:
                conn.close()
    return jsonify({"user": u})


@app.route("/auth/signup", methods=["POST"])
def auth_signup():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    email = (data.get("email") or "").strip()
    first_name = (data.get("first_name") or "").strip()[:80]
    last_name = (data.get("last_name") or "").strip()[:80]
    phone = (data.get("phone") or "").strip()[:30]
    if len(username) < 3 or len(username) > 30:
        return jsonify({"error": "Username must be 3 to 30 characters."}), 400
    if not all(c.isalnum() or c in "_." for c in username):
        return jsonify({"error": "Username can use letters, numbers, underscore, and period only."}), 400
    if len(password) < 8:
        return jsonify({"error": "Password must be at least 8 characters."}), 400
    if not email or not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        return jsonify({"error": "Enter a valid email address."}), 400
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Accounts are not available right now."}), 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE LOWER(username) = LOWER(%s)", (username,))
        if cur.fetchone():
            cur.close()
            return jsonify({"error": "That username is taken."}), 409
        cur.execute("SELECT id FROM users WHERE LOWER(email) = LOWER(%s)", (email,))
        if cur.fetchone():
            cur.close()
            return jsonify({"error": "That email is already registered."}), 409
        pw_hash = generate_password_hash(password)
        cur.execute(
            "INSERT INTO users (username, password_hash, email, first_name, last_name, phone, tier, paper_cash) "
            "VALUES (%s, %s, %s, %s, %s, %s, 'free', 1000000) RETURNING id",
            (username, pw_hash, email, first_name or None, last_name or None, phone or None),
        )
        uid = cur.fetchone()[0]
        conn.commit()
        cur.close()
        session.permanent = True
        session["user_id"] = uid
        session["username"] = username
        session["tier"] = "free"
        return jsonify({"ok": True, "user": {"id": uid, "username": username, "tier": "free",
                                             "phone": phone or None, "phone_verified": False}})
    except Exception as e:
        logger.error("signup error: %s" % e)
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify({"error": "Could not create account. The email or username may already be in use."}), 500
    finally:
        conn.close()


@app.route("/auth/login", methods=["POST"])
@login_limit  # SECURITY: 5 login attempts per minute per IP
def auth_login():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not password:
        return jsonify({"error": "Enter your username and password."}), 400
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Accounts are not available right now."}), 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, username, password_hash, COALESCE(tier, 'free') FROM users WHERE LOWER(username) = LOWER(%s)", (username,))
        row = cur.fetchone()
        cur.close()
        if not row or not check_password_hash(row[2], password):
            return jsonify({"error": "Wrong username or password."}), 401
        session.permanent = True
        session["user_id"] = row[0]
        session["username"] = row[1]
        session["tier"] = row[3]
        return jsonify({"ok": True, "user": {"id": row[0], "username": row[1], "tier": row[3]}})
    except Exception as e:
        logger.error("login error: %s" % e)
        return jsonify({"error": "Could not log in. Try again."}), 500
    finally:
        conn.close()


@app.route("/auth/logout", methods=["POST"])
def auth_logout():
    session.clear()
    return jsonify({"ok": True})


# Marketing list export as CSV. Per spec this is open for internal use, but because it exposes
# every user's email and phone, you can lock it by setting MARKETING_EXPORT_TOKEN and then
# calling it as /marketing/export?token=YOURTOKEN. With no token set, it stays open.
@app.route("/marketing/export")
def marketing_export():
    gate = os.environ.get("MARKETING_EXPORT_TOKEN", "")
    if gate and request.args.get("token") != gate:
        return Response("error,unauthorized\n", mimetype="text/csv"), 401
    conn = get_db()
    if conn is None:
        return Response("error,no database\n", mimetype="text/csv"), 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT email, first_name, last_name, phone, COALESCE(phone_verified, false), created_at "
                    "FROM users ORDER BY created_at ASC NULLS LAST")
        rows = cur.fetchall()
        cur.close()
        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(["email", "first_name", "last_name", "phone", "phone_verified", "created_at"])
        for em, fn, ln, ph, pv, ts in rows:
            w.writerow([em or "", fn or "", ln or "", ph or "", "yes" if pv else "no", ts.isoformat() if ts else ""])
        return Response(out.getvalue(), mimetype="text/csv",
                        headers={"Content-Disposition": "attachment; filename=apexq_marketing.csv"})
    except Exception as e:
        logger.error("marketing_export error: %s" % e)
        return Response("error,export failed\n", mimetype="text/csv"), 500
    finally:
        conn.close()


# Phone verification via Twilio was removed. Signup no longer collects or requires a phone
# number. The users.phone column is retained for legacy rows but is never written or read.

@app.route("/auth/export", methods=["GET"])
def auth_export():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    uid = u["id"]
    out = {"username": u["username"], "email": None, "watchlist": [], "holdings": [], "alert_prefs": None}
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database not available."}), 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT symbol, name, added_at FROM watchlist WHERE user_id=%s ORDER BY added_at ASC", (uid,))
        for sym, nm, ts in cur.fetchall():
            out["watchlist"].append({"symbol": sym, "name": nm, "added_at": ts.isoformat() if ts else None})
        cur.execute("SELECT symbol, shares, avg_cost, added_at FROM holdings WHERE user_id=%s ORDER BY added_at ASC", (uid,))
        for sym, sh, ac, ts in cur.fetchall():
            try:
                shares_f = float(sh)
                avg_f = float(ac)
            except (TypeError, ValueError):
                shares_f, avg_f = None, None
            out["holdings"].append({"symbol": sym, "shares": shares_f, "avg_cost": avg_f, "added_at": ts.isoformat() if ts else None})
        cur.execute("SELECT email, alert_prefs FROM alert_subscriptions WHERE user_id=%s", (uid,))
        sub = cur.fetchone()
        if sub:
            out["email"] = sub[0]
            out["alert_prefs"] = sub[1] or "all"
        cur.close()
    except Exception as e:
        logger.error("export error: %s" % e)
        return jsonify({"error": "Could not export your data."}), 500
    finally:
        conn.close()
    return jsonify(out)


# SECURITY: permanently deletes the account and every row tied to it, then clears the session.
@app.route("/auth/delete", methods=["POST"])
def auth_delete():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    uid = u["id"]
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database not available."}), 500
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM watchlist WHERE user_id=%s", (uid,))
        cur.execute("DELETE FROM holdings WHERE user_id=%s", (uid,))
        cur.execute("DELETE FROM verdict_history WHERE user_id=%s", (uid,))
        cur.execute("DELETE FROM alert_subscriptions WHERE user_id=%s", (uid,))
        cur.execute("DELETE FROM users WHERE id=%s", (uid,))
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error("delete error: %s" % e)
        return jsonify({"error": "Could not delete your account."}), 500
    finally:
        conn.close()
    session.clear()
    return jsonify({"ok": True})


# SECURITY/compliance: plain English legal pages, served as standalone styled HTML.
def _legal_page(title, inner):
    doc = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>%s | Apex Q</title>
<style>
body{margin:0;background:#f0f3f8;color:#141a2b;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;line-height:1.6;-webkit-font-smoothing:antialiased;}
.wrap{max-width:720px;margin:0 auto;padding:32px 22px 64px;}
a.back{display:inline-block;margin-bottom:20px;color:#1a1f71;font-weight:700;text-decoration:none;font-size:14px;}
h1{font-size:26px;margin:0 0 6px;letter-spacing:-.01em;}
h2{font-size:16px;margin:26px 0 6px;color:#1a1f71;}
p,li{font-size:15px;color:#33405c;}
ul{padding-left:20px;}
.disc{margin-top:28px;padding:14px 16px;background:#fff;border:1px solid #e2e8f2;border-radius:12px;font-size:14px;color:#33405c;}
.upd{margin-top:18px;font-size:13px;color:#5c6b85;}
</style></head>
<body><div class="wrap"><a class="back" href="/">&#8592; Back to Apex Q</a>
<h1>%s</h1>
%s
<p class="disc"><strong>Educational use only.</strong> Apex Q is an educational research terminal. Nothing it shows is financial, investment, legal, or tax advice, and nothing here is a recommendation to buy or sell any security. Markets carry risk. Do your own research and speak with a licensed professional before making any financial decision.</p>
<p class="upd">Last updated June 2026.</p></div></body></html>""" % (title, title, inner)
    return Response(doc, mimetype="text/html")


@app.route("/terms")
def terms_page():
    inner = """
<p>Welcome to Apex Q. By using this terminal you agree to these terms. Please read them in full.</p>
<h2>What Apex Q is</h2>
<p>Apex Q is an educational research tool. It reads live market data, valuation figures, analyst views, and public filings, and it explains what those signals mean in plain English. It exists to help you learn how to read the market for yourself.</p>
<h2>Not financial advice</h2>
<p>Apex Q does not give financial, investment, legal, or tax advice. The verdicts, scores, and summaries it produces are educational illustrations only. They are not recommendations and they are not a solicitation to buy or sell anything. You alone are responsible for your decisions.</p>
<h2>Accuracy and availability</h2>
<p>Market and company data come from third party providers. We work to keep it accurate and current, but we cannot guarantee it is complete, correct, or always available. Figures may be delayed, and the service may go offline for maintenance or for reasons outside our control.</p>
<h2>Your account</h2>
<p>You are responsible for keeping your login details private and for everything done under your account. Tell us right away if you believe your account has been accessed without your permission.</p>
<h2>Acceptable use</h2>
<p>Use Apex Q for your own lawful, personal, educational purposes. Do not scrape it in bulk, attempt to break or overload it, resell access to it, or use it to break any law.</p>
<h2>Limitation of liability</h2>
<p>Apex Q is provided as is, without warranties of any kind. To the fullest extent allowed by law, we are not liable for any loss arising from your use of the terminal or from any decision you make based on it.</p>
<h2>Changes</h2>
<p>We may update these terms or the service over time. Continued use after a change means you accept the updated terms.</p>
"""
    return _legal_page("Terms of Service", inner)


@app.route("/privacy")
def privacy_page():
    inner = """
<p><b>Last updated July 2, 2026</b></p>
<p>This Privacy Policy describes how Apex Q collects, uses, and protects your information when you use the service, and the rights and controls you have over your data. Apex Q is operated by Xfinity Holdings LLC of Trumbull, Connecticut, United States, referred to below as the Company, we, us, or our. By using Apex Q you agree to the practices described here.</p>

<h2>Information we collect</h2>
<p>Account information you provide: a username, a secure one way hash of your password (we never store your password in plain text), your email address, your first and last name, and your phone number if you provide one for verification.</p>
<p>Content you create in the app: the stocks you save to your watchlist, the holdings you enter into the portfolio tracker, and your practice trades in the practice trading feature.</p>
<p>Usage data collected automatically: standard technical information such as your device's IP address, browser type, the pages you visit, and the time and date of your visit. We use this to keep the service running, secure, and improving.</p>

<h2>Payments</h2>
<p>Subscriptions are processed by Stripe, Inc. Your card number and full payment details go directly to Stripe and never touch or rest on our servers. We receive only what is needed to manage your subscription, such as its status and tier. Stripe's privacy policy is at https://stripe.com/privacy.</p>

<h2>Phone verification</h2>
<p>If you verify your phone number, the verification text message is delivered by Twilio Inc. Your number is shared with Twilio only to deliver that message. Twilio's privacy policy is at https://www.twilio.com/legal/privacy.</p>

<h2>AI features and data sharing</h2>
<p>Apex Q's question and answer features are powered by third party artificial intelligence providers. When you use these features, the following is sent to the provider: the question you type, the ticker or tickers involved, and the live market context Apex Q assembles to ground the answer, such as current prices and fundamentals. We do not send your email address, your name, your account credentials, your payment information, or your device identifiers to any AI provider.</p>
<p>Our current AI providers are DeepSeek, whose privacy policy is at https://platform.deepseek.com, and Google (Gemini), whose privacy policy is at https://policies.google.com/privacy. These providers may change over time and this policy will be updated when they do.</p>
<p>By using the AI powered features you consent to this transmission. If you choose not to use those features, no data is shared with AI providers.</p>

<h2>Brokerage connection and portfolio data</h2>
<p>Apex Q offers an optional feature that lets you connect a brokerage account to see your real holdings inside the app. Connections are handled entirely by SnapTrade, a third party financial data provider. Apex Q never receives, processes, or stores your brokerage login credentials at any point. Where your brokerage supports it, SnapTrade uses OAuth, meaning you sign in directly with your brokerage and SnapTrade receives a secure token rather than your password. SnapTrade is SOC 2 Type II certified, and you can review its security practices at https://snaptrade.com/security.</p>
<p>The connection is strictly read only. No trading, transfer, or write capability exists anywhere in Apex Q. We store a connection identifier and a per user connection secret so your link keeps working, and your holdings are fetched on demand and held briefly in a temporary cache to keep the app fast. Your brokerage positions are not written into our permanent database.</p>
<p>You can disconnect at any time with the disconnect button in the app, which removes the link and the stored connection secret. Per SnapTrade's data policy, your data belongs to you, and we do not sell or share it.</p>

<h2>Market data providers</h2>
<p>When you look up a stock, the request goes to third party market data providers, currently Finnhub, Financial Modeling Prep, and Yahoo Finance, to fetch prices, fundamentals, and filings. Those lookups are about the ticker, not about you, and your identity is not handed to them as part of normal use.</p>

<h2>How we use your information</h2>
<p>To provide and maintain the service and show you your own data. To manage your account and subscription. To contact you about the service, such as verification codes, security notices, and, only if you opt in, product news you can unsubscribe from at any time. To power the AI features as described above. To understand usage and improve the product. We do not sell your personal information, we do not share it with advertisers, and we do not give it to data brokers.</p>

<h2>Where your data lives</h2>
<p>Apex Q runs on Railway infrastructure in the United States with a PostgreSQL database. Data moves between your device and our servers over encrypted connections. If you use the service from outside the United States, you consent to your information being processed in the United States.</p>

<h2>Retention</h2>
<p>We keep your personal data only as long as needed for the purposes above, to comply with legal obligations, to resolve disputes, and to enforce our agreements. Usage data is generally kept for a shorter period unless needed for security.</p>

<h2>Your controls</h2>
<p>You can export everything stored about you at any time from within the app. You can delete your account and all of its data at any time, and deletion is permanent: it removes your watchlist, your holdings, your practice trades, any brokerage connection, and any alert or email subscription. You can also contact us to request access, correction, or deletion.</p>

<h2>Children's privacy</h2>
<p>Apex Q is not directed at anyone under the age of 13, and we do not knowingly collect personal information from anyone under 13. If you believe a child has provided us personal information, contact us and we will remove it.</p>

<h2>Links to other websites</h2>
<p>The service may link to websites we do not operate, including news sources and provider pages. We are not responsible for their content or privacy practices, and we encourage you to review the privacy policy of every site you visit.</p>

<h2>Security</h2>
<p>We use industry standard measures to protect your data, including encryption in transit and hashed credentials. No method of transmission or storage is completely secure, and while we work hard to protect your information, we cannot guarantee absolute security.</p>

<h2>Changes to this policy</h2>
<p>We may update this policy from time to time. Changes are posted on this page with an updated date at the top, and material changes will be flagged prominently in the service or by email.</p>

<h2>Contact us</h2>
<p>Questions about this policy or your data: support@apexq.io. Apex Q, Trumbull, Connecticut, United States.</p>
"""
    return _legal_page("Privacy Policy", inner)


# Recent SEC EDGAR filings for a symbol, parsed from the public ATOM feed and cached two hours.
# SEC fair access requires a descriptive User-Agent with a contact, so one is set below; change
# the contact if you fork this. Any failure, a non equity symbol, or an empty feed returns [].
@app.route("/filings/<symbol>")
def filings(symbol):
    sym = (symbol or "").strip().upper()
    if not sym:
        return jsonify([])
    ckey = "filings_" + sym
    cached = CACHE.get(ckey)
    if cached and (time.time() - cached[1]) < 7200:
        return jsonify(cached[0])

    url = ("https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=%s"
           "&type=&dateb=&owner=include&count=10&output=atom" % sym)
    headers = {
        "User-Agent": "Apex Q educational research (contact: research@apexq.io)",
        "Accept-Encoding": "gzip, deflate",
    }
    out = []
    try:
        resp = requests.get(url, headers=headers, timeout=12)
        if resp.status_code != 200 or not resp.text:
            set_cache(ckey, [])
            return jsonify([])
        import xml.etree.ElementTree as ET

        def _local(tag):
            return tag.split("}")[-1] if "}" in tag else tag

        root = ET.fromstring(resp.text)
        entries = [el for el in root.iter() if _local(el.tag) == "entry"]
        for e in entries[:5]:
            title = ftype = fdate = link = updated = cat_term = atom_link = ""
            for child in e.iter():
                lt = _local(child.tag)
                txt = (child.text or "").strip()
                if lt == "title" and not title:
                    title = txt
                elif lt == "filing-type" and not ftype:
                    ftype = txt
                elif lt == "filing-date" and not fdate:
                    fdate = txt
                elif lt == "filing-href" and not link:
                    link = txt
                elif lt == "updated" and not updated:
                    updated = txt
                elif lt == "category" and not cat_term:
                    cat_term = (child.get("term") or "").strip()
                elif lt == "link" and not atom_link:
                    href = child.get("href")
                    if href:
                        atom_link = href.strip()
            final_type = ftype or cat_term or (title.split()[0] if title else "Filing")
            final_date = fdate or (updated.split("T")[0] if updated else "")
            final_link = link or atom_link
            if not final_link and not final_type:
                continue
            out.append({"title": title, "type": final_type, "date": final_date, "link": final_link})
    except Exception as ex:
        logger.error("filings %s error: %s" % (sym, ex))
        set_cache(ckey, [])
        return jsonify([])

    set_cache(ckey, out)
    return jsonify(out)


@app.route("/watchlist", methods=["GET"])
def watchlist_list():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database not available."}), 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT symbol, name FROM watchlist WHERE user_id = %s ORDER BY added_at DESC", (u["id"],))
        rows = cur.fetchall()
        cur.close()
        return jsonify({"items": [{"symbol": r[0], "name": r[1] or r[0]} for r in rows]})
    except Exception as e:
        logger.error("watchlist list error: %s" % e)
        return jsonify({"error": "Could not load your watchlist."}), 500
    finally:
        conn.close()


@app.route("/watchlist/add", methods=["POST"])
def watchlist_add():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "").strip().upper()
    name = (data.get("name") or "").strip()
    if not symbol or len(symbol) > 15:
        return jsonify({"error": "Invalid symbol."}), 400
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database not available."}), 500
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO watchlist (user_id, symbol, name) VALUES (%s, %s, %s) "
            "ON CONFLICT (user_id, symbol) DO UPDATE SET name = EXCLUDED.name",
            (u["id"], symbol, name),
        )
        conn.commit()
        cur.close()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("watchlist add error: %s" % e)
        return jsonify({"error": "Could not save to your watchlist."}), 500
    finally:
        conn.close()


@app.route("/watchlist/remove", methods=["POST"])
def watchlist_remove():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"error": "Invalid symbol."}), 400
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database not available."}), 500
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM watchlist WHERE user_id = %s AND symbol = %s", (u["id"], symbol))
        conn.commit()
        cur.close()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("watchlist remove error: %s" % e)
        return jsonify({"error": "Could not remove from your watchlist."}), 500
    finally:
        conn.close()


# CHUNK: Portfolio Tracker — manual holdings with live value and gain or loss. Educational only.
def compute_portfolio(uid):
    # 60 second cache so rapid refreshes do not re-run the whole aggregation. The underlying
    # prices come from light_score, which carries its own cache, so this never hammers Yahoo.
    # Returns the payload dict, not a response, so both /portfolio and /dashboard can reuse it.
    ckey = "portfolio_" + str(uid)
    entry = CACHE.get(ckey)
    if entry and (time.time() - entry[1]) < 60:
        return entry[0]
    conn = get_db()
    if conn is None:
        return {"error": "Database not available."}
    try:
        cur = conn.cursor()
        cur.execute("SELECT symbol, shares, avg_cost FROM holdings WHERE user_id = %s ORDER BY added_at ASC", (uid,))
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        logger.error("portfolio get error: %s" % e)
        return {"error": "Could not load your portfolio."}
    finally:
        conn.close()

    holdings = []
    tot_mv = 0.0
    tot_cb = 0.0
    tot_day = 0.0
    for symbol, shares, avg_cost in rows:
        try:
            shares_f = float(shares)
            avg_f = float(avg_cost)
        except (TypeError, ValueError):
            continue
        r = light_score(symbol)
        cost_basis = round(shares_f * avg_f, 2)
        if not r or not isinstance(r.get("price"), (int, float)):
            holdings.append({
                "symbol": symbol, "shares": shares_f, "avg_cost": round(avg_f, 2),
                "price": None, "change_pct": None,
                "market_value": "N/A", "cost_basis": cost_basis,
                "gain_loss": "N/A", "gain_loss_pct": "N/A", "day_change": "N/A",
            })
            continue
        price = float(r["price"])
        change_pct = r.get("change_pct")
        market_value = round(shares_f * price, 2)
        gain_loss = round(market_value - cost_basis, 2)
        gain_loss_pct = round((price / avg_f - 1) * 100, 2) if avg_f > 0 else 0
        day_change = round(shares_f * price * (change_pct / 100.0), 2) if isinstance(change_pct, (int, float)) else 0
        holdings.append({
            "symbol": symbol, "shares": shares_f, "avg_cost": round(avg_f, 2),
            "price": round(price, 2), "change_pct": change_pct,
            "market_value": market_value, "cost_basis": cost_basis,
            "gain_loss": gain_loss, "gain_loss_pct": gain_loss_pct, "day_change": day_change,
        })
        tot_mv += market_value
        tot_cb += cost_basis
        tot_day += day_change

    # Allocation percent per holding, now that the total market value is known. A holding with an
    # N/A market value, or a portfolio whose whole value is zero, gets 0 so the math stays clean.
    for h in holdings:
        mv = h.get("market_value")
        h["allocation_pct"] = round((mv / tot_mv) * 100, 2) if (tot_mv > 0 and isinstance(mv, (int, float))) else 0

    tot_gl = round(tot_mv - tot_cb, 2)
    tot_gl_pct = round((tot_mv / tot_cb - 1) * 100, 2) if tot_cb > 0 else 0
    payload = {
        "holdings": holdings,
        "totals": {
            "market_value": round(tot_mv, 2),
            "cost_basis": round(tot_cb, 2),
            "gain_loss": tot_gl,
            "gain_loss_pct": tot_gl_pct,
            "day_change": round(tot_day, 2),
        },
        "data_timestamp": int(time.time()),
    }
    set_cache(ckey, payload)
    return payload


def compute_portfolio_history(uid):
    # Last 7 days of total portfolio value, one point per trading day, summed as shares * close
    # across every holding. This is the one place that fetches multi day history, so it is cached
    # for an hour and kept out of compute_portfolio so the 60 second totals path and the dashboard
    # never trigger it. Returns a list of {date, value}, oldest first, or an empty list.
    ckey = "portfolio_hist_" + str(uid)
    entry = CACHE.get(ckey)
    if entry and (time.time() - entry[1]) < 3600:
        return entry[0]
    conn = get_db()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute("SELECT symbol, shares FROM holdings WHERE user_id = %s", (uid,))
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        logger.error("portfolio history db error: %s" % e)
        return []
    finally:
        conn.close()

    if not rows:
        set_cache(ckey, [])
        return []

    totals = {}
    for symbol, shares in rows:
        try:
            shares_f = float(shares)
        except (TypeError, ValueError):
            continue
        try:
            hist = yf.Ticker(symbol).history(period="7d", timeout=10)
        except Exception as e:
            logger.error("portfolio history fetch %s: %s" % (symbol, e))
            continue
        if hist is None or len(hist) == 0 or "Close" not in hist:
            continue
        try:
            closes = hist["Close"]
            for idx, val in closes.items():
                try:
                    v = float(val)
                except (TypeError, ValueError):
                    continue
                if v != v:  # NaN guard
                    continue
                dstr = idx.strftime("%Y-%m-%d")
                totals[dstr] = totals.get(dstr, 0.0) + shares_f * v
        except Exception as e:
            logger.error("portfolio history parse %s: %s" % (symbol, e))
            continue

    out = [{"date": d, "value": round(totals[d], 2)} for d in sorted(totals.keys())][-7:]
    set_cache(ckey, out)
    return out


@app.route("/portfolio", methods=["GET"])
def portfolio_get():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    result = compute_portfolio(u["id"])
    if isinstance(result, dict) and result.get("error"):
        return jsonify(result), 500
    if isinstance(result, dict):
        # Copy so the 60 second cached payload, also read by the dashboard, is never mutated.
        result = dict(result)
        result["history"] = compute_portfolio_history(u["id"])
    return jsonify(result)


@app.route("/portfolio/add", methods=["POST"])
def portfolio_add():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    uid = u["id"]
    data = request.get_json(silent=True) or {}
    raw_sym = (data.get("symbol") or "").strip()
    # Let the box accept a company name too, resolving it to a ticker before saving.
    if raw_sym and not looks_like_ticker(raw_sym.upper()):
        raw_sym = resolve_ticker(raw_sym)
    symbol = raw_sym.strip().upper()
    if not symbol or len(symbol) > 10:
        return jsonify({"error": "Enter a valid ticker symbol."}), 400
    try:
        shares = float(data.get("shares"))
        avg_cost = float(data.get("avg_cost"))
    except (TypeError, ValueError):
        return jsonify({"error": "Shares and average cost must be numbers."}), 400
    if shares <= 0:
        return jsonify({"error": "Shares must be greater than zero."}), 400
    if avg_cost < 0:
        return jsonify({"error": "Average cost cannot be negative."}), 400
    # Verify the ticker is real before it ever reaches the table. A symbol that cannot produce a
    # price would sit in the portfolio as a dead N/A row forever, so it gets rejected at the door
    # with a pointer to the closest likely match when one exists.
    if _paper_price(symbol) is None:
        suggestion = ""
        try:
            alt = resolve_ticker(symbol)
            if alt and alt.upper() != symbol and _paper_price(alt.upper()) is not None:
                suggestion = " Did you mean " + alt.upper() + "?"
        except Exception:
            pass
        return jsonify({"error": "No price data found for " + symbol + ". Check the ticker." + suggestion}), 400
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database not available."}), 500
    try:
        cur = conn.cursor()
        # Free tier limit. A new symbol counts against the cap, an existing one is just an update.
        cur.execute("SELECT COUNT(*) FROM holdings WHERE user_id = %s", (uid,))
        count = cur.fetchone()[0]
        cur.execute("SELECT 1 FROM holdings WHERE user_id = %s AND symbol = %s", (uid, symbol))
        exists = cur.fetchone() is not None
        if FREE_LIMITS_ENABLED and count >= 5 and not exists and not is_premium(u):
            cur.close()
            return jsonify({"error": "premium_required", "message": "Free accounts can track up to 5 holdings. Upgrade to Premium for unlimited."}), 402
        cur.execute(
            "INSERT INTO holdings (user_id, symbol, shares, avg_cost) VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (user_id, symbol) DO UPDATE SET shares = EXCLUDED.shares, avg_cost = EXCLUDED.avg_cost",
            (uid, symbol, shares, avg_cost),
        )
        conn.commit()
        cur.close()
        CACHE.pop("portfolio_" + str(uid), None)
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("portfolio add error: %s" % e)
        return jsonify({"error": "Could not save that holding."}), 500
    finally:
        conn.close()


@app.route("/portfolio/remove", methods=["POST"])
def portfolio_remove():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    uid = u["id"]
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"error": "Invalid symbol."}), 400
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database not available."}), 500
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM holdings WHERE user_id = %s AND symbol = %s", (uid, symbol))
        conn.commit()
        cur.close()
        CACHE.pop("portfolio_" + str(uid), None)
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("portfolio remove error: %s" % e)
        return jsonify({"error": "Could not remove that holding."}), 500
    finally:
        conn.close()


@app.route("/search")
def search_ticker():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "No query"}), 400
    try:
        s = yf.Search(q, max_results=6)
        results = [{"symbol": x.get("symbol"), "name": x.get("longname") or x.get("shortname")} for x in s.quotes if x.get("symbol")]
        if not results:
            for r in _china_spot_table():
                if q in r["name"] or r["code"].startswith(q):
                    results.append({"symbol": _china_symbol(r["code"]), "name": r["name"]})
                    if len(results) >= 6:
                        break
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500




# ---------- Asian market coverage ----------
# akshare is imported guarded so a missing or broken install can never take the app down. Bare six
# digit A share codes are normalized to Yahoo suffixed symbols (6xxxxx to .SS, others to .SZ) and
# then flow through the SAME engine as every US stock, which keeps verdicts, Alpha Scores, and the
# report identical in shape. DEVIATION, DOCUMENTED: the spec wanted akshare to feed the report
# itself, but yfinance already covers .SS, .SZ, and .HK reliably and consistently with the rest of
# the engine, so akshare is used where it is uniquely strong: Chinese name search and Guba retail
# sentiment.
try:
    import akshare as _ak
except Exception as _ake:
    _ak = None
    logger.error("akshare unavailable: %s" % _ake)


INDEX_NAMES = {
    "^GSPC": "S&P 500", "^IXIC": "Nasdaq Composite", "^DJI": "Dow Jones Industrial Average",
    "^RUT": "Russell 2000", "^VIX": "CBOE Volatility Index", "^FTSE": "FTSE 100",
    "^N225": "Nikkei 225", "^HSI": "Hang Seng", "^GDAXI": "DAX",
    "GC=F": "Gold Futures", "SI=F": "Silver Futures", "CL=F": "Crude Oil Futures",
    "NG=F": "Natural Gas Futures", "HG=F": "Copper Futures", "BTC-USD": "Bitcoin",
    "ETH-USD": "Ethereum",
}

INDEX_DESCRIPTIONS = {
    "^GSPC": "The S&P 500 is a market cap weighted index of 500 leading US companies, the most common benchmark for the overall US stock market.",
    "^IXIC": "The Nasdaq Composite tracks over 3,000 stocks listed on the Nasdaq exchange, weighted heavily toward technology companies.",
    "^DJI": "The Dow Jones Industrial Average tracks 30 large, established US companies. It is price weighted, so higher priced stocks move it more.",
    "^RUT": "The Russell 2000 tracks 2,000 smaller US companies and is watched as a gauge of small cap and domestic economic health.",
    "^VIX": "The VIX measures the market's expected volatility over the next 30 days. It often rises when fear rises, which is why it is called the fear gauge.",
    "^FTSE": "The FTSE 100 tracks the 100 largest companies listed on the London Stock Exchange.",
    "^N225": "The Nikkei 225 is Japan's leading stock index, tracking 225 large companies on the Tokyo Stock Exchange.",
    "^HSI": "The Hang Seng tracks the largest companies listed in Hong Kong.",
    "^GDAXI": "The DAX tracks 40 major companies trading on the Frankfurt Stock Exchange in Germany.",
    "GC=F": "Gold futures reflect the market price of gold, a metal often bought as a store of value when investors seek safety.",
    "SI=F": "Silver futures track the price of silver, used both as an investment and widely in industry.",
    "CL=F": "Crude oil futures track the price of oil, a driver of energy costs and a closely watched signal of global demand.",
    "NG=F": "Natural gas futures track the price of natural gas, a key heating and power generation fuel.",
    "HG=F": "Copper futures track the price of copper, often called Doctor Copper for how well its price reflects economic health.",
    "BTC-USD": "Bitcoin is the largest cryptocurrency by value, trading 24 hours a day with no company, earnings, or central authority behind it.",
    "ETH-USD": "Ethereum is the second largest cryptocurrency and the leading platform for smart contracts and decentralized apps.",
}


def _is_index_symbol(sym):
    s = (sym or "").upper()
    return s in INDEX_NAMES or s.startswith("^")


def _build_index_report(symbol):
    """Simplified report for an index or future: name, price, change, extended hours, and a one
    line market context. No scoring engine, no company sections. Returns None on failure so the
    caller can fall through to the normal path."""
    sym = symbol.upper()
    try:
        t = yf.Ticker(sym)
        # Prefer the daily history close, which is reliable for indices and futures. yfinance's
        # info.regularMarketPrice is often stale, wrong, or shared across symbols for ^ tickers,
        # which caused the identical bad price. History is per symbol and trustworthy.
        price = None
        prev = None
        try:
            hist = t.history(period="1mo")
            if hist is not None and not hist.empty:
                price = float(hist["Close"].iloc[-1])
                prev = float(hist["Close"].iloc[-2]) if len(hist) > 1 else price
        except Exception as he:
            logger.error("index hist %s: %s" % (sym, he))
        info = {}
        try:
            info = t.info or {}
        except Exception:
            info = {}
        if price is None:
            price = info.get("regularMarketPrice") or info.get("previousClose")
            prev = info.get("previousClose")
        if price is None:
            return None
        price = float(price)
        change_pct = 0.0
        if prev:
            try:
                change_pct = round((price - float(prev)) / float(prev) * 100, 2)
            except (TypeError, ValueError, ZeroDivisionError):
                change_pct = 0.0
        name = INDEX_NAMES.get(sym, info.get("shortName") or info.get("longName") or sym)
        # Honest factual context based on the move itself. Indices do not have company fundamentals
        # for the AI context layer to ground on, so a plain factual line is more trustworthy than
        # an AI guess.
        direction = "up" if change_pct > 0 else ("down" if change_pct < 0 else "flat")
        if sym == "^VIX":
            context = ("The VIX measures expected market volatility over the next 30 days. It is %s %.2f%% today. "
                       "A rising VIX often reflects growing fear or uncertainty, a falling VIX calmer conditions." % (direction, abs(change_pct)))
        else:
            context = ("%s is %s %.2f%% today, sitting at %s. This tracks a broad basket, so it reflects overall "
                       "market mood rather than any single company." % (name, direction, abs(change_pct), ("%.2f" % price)))
        is_future = "=F" in sym
        is_crypto = sym.endswith("-USD")
        kind = "future" if is_future else ("cryptocurrency" if is_crypto else "index")
        note = ("This is a %s. It represents a basket or benchmark, not an individual company, so "
                "company level signals like insider trades, congressional activity, and moat do not "
                "apply here." % kind)
        ext = None
        try:
            ext = extended_hours(info, price)
        except Exception:
            ext = None
        return {
            "symbol": sym,
            "name": name,
            "price": round(price, 2),
            "change_pct": change_pct,
            "verdict": "INDEX",
            "asset_class": "index",
            "description": INDEX_DESCRIPTIONS.get(sym, ""),
            "market_context": context,
            "index_note": note,
            "extended": ext,
            "data_timestamp": int(time.time()),
        }
    except Exception as e:
        logger.error("index report %s: %s" % (sym, e))
        return None


def _asset_class(symbol):
    """crypto, macro, or stock. Crypto pairs end in a dash USD quote; macro covers futures (=F),
    currency pairs (=X), yield indexes (^), and the commodity index."""
    sym = (symbol or "").upper()
    if sym.endswith("-USD") and len(sym) > 4:
        return "crypto"
    if "=F" in sym or "=X" in sym or sym.startswith("^") or sym == "BCOM":
        return "macro"
    return "stock"


def _china_symbol(sym):
    """Yahoo form for a bare A share code, or the symbol unchanged."""
    if re.fullmatch(r"\d{6}", sym or ""):
        return sym + (".SS" if sym.startswith(("6", "9")) else ".SZ")
    return sym


def _china_spot_table():
    cached = CACHE.get("ak_spot")
    if cached and (time.time() - cached[1]) < 3600:
        return cached[0]
    if _ak is None:
        return []
    rows = []
    try:
        df = _ak.stock_zh_a_spot_em() if hasattr(_ak, "stock_zh_a_spot_em") else _ak.stock_zh_a_spot()
        codes = df["\u4ee3\u7801"].tolist() if "\u4ee3\u7801" in df.columns else df[df.columns[1]].tolist()
        names = df["\u540d\u79f0"].tolist() if "\u540d\u79f0" in df.columns else df[df.columns[2]].tolist()
        for c, n in zip(codes, names):
            rows.append({"code": str(c), "name": str(n)})
    except Exception as e:
        logger.error("akshare spot: %s" % e)
    CACHE["ak_spot"] = (rows, time.time())
    return rows


@app.route("/search/china")
def search_china():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"results": []})
    out = []
    for r in _china_spot_table():
        if q in r["name"] or r["code"].startswith(q):
            out.append({"symbol": _china_symbol(r["code"]), "name": r["name"]})
            if len(out) >= 6:
                break
    return jsonify({"results": out})


def fetch_guba_sentiment(symbol):
    """Retail mood from Guba post titles for an A share code: keyword counted ratio, label, and
    post count. Returns None when akshare or the Guba feed is unavailable, never a guess."""
    code = symbol.split(".")[0]
    if not re.fullmatch(r"\d{6}", code) or _ak is None:
        return None
    ck = "guba_" + code
    cached = CACHE.get(ck)
    if cached and (time.time() - cached[1]) < 1800:
        return cached[0]
    titles = []
    for fname in ("stock_guba_em", "stock_guba_sina"):
        fn = getattr(_ak, fname, None)
        if fn is None:
            continue
        try:
            df = fn(symbol=code)
            col = None
            for cname in df.columns:
                if "\u6807\u9898" in str(cname) or "title" in str(cname).lower():
                    col = cname
                    break
            if col is not None:
                titles = [str(t) for t in df[col].tolist()[:80]]
                break
        except Exception as e:
            logger.error("guba %s: %s" % (fname, e))
    if not titles:
        CACHE[ck] = (None, time.time())
        return None
    pos_words = ["\u6da8", "\u5229\u597d", "\u4e70", "\u725b", "\u52a0\u4ed3", "\u7a81\u7834", "\u5f3a"]
    neg_words = ["\u8dcc", "\u5229\u7a7a", "\u5356", "\u718a", "\u4e8f", "\u8dd1", "\u5272"]
    pos = sum(1 for t in titles for w in pos_words if w in t)
    neg = sum(1 for t in titles for w in neg_words if w in t)
    total = pos + neg
    ratio = round(pos / total, 2) if total else 0.5
    label = "Bullish" if ratio >= 0.6 else ("Bearish" if ratio <= 0.4 else "Neutral")
    res = {"guba_sentiment": ratio, "guba_label": label, "guba_post_count": len(titles)}
    CACHE[ck] = (res, time.time())
    return res


@app.route("/guba")
def guba_route():
    symbol = (request.args.get("symbol") or "").strip().upper()
    res = fetch_guba_sentiment(symbol)
    return jsonify(res or {"available": False})


def clean_text(s):
    # Strips HTML, decodes entities, normalizes odd characters, and rejoins broken
    # ordinals like "16 th" so news text reads clean instead of garbled.
    if not s:
        return ""
    s = str(s)
    s = html.unescape(s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    repl = {
        "\u2018": "'", "\u2019": "'", "\u201c": '"', "\u201d": '"',
        "\u2013": " ", "\u2014": " ", "\u2026": "...", "\u00a0": " ",
        "\u00ad": "", "\ufffd": "", "\u2022": " ", "\u200b": "",
    }
    for k, v in repl.items():
        s = s.replace(k, v)
    s = re.sub(r"(\d)\s+(st|nd|rd|th)\b", r"\1\2", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def trim_words(s, limit=170):
    # Trims to a clean word boundary so summaries never cut off mid word.
    if not s:
        return ""
    if len(s) <= limit:
        return s
    cut = s[:limit]
    sp = cut.rfind(" ")
    if sp > 50:
        cut = cut[:sp]
    return cut.rstrip(" ,.;:") + "..."


def flip_name(s):
    # Insider feeds list names last name first (RISHEL JEREMY DYLAN). Flip to natural
    # first name first and clean the capitalization (Jeremy Dylan Rishel).
    if not s:
        return ""
    s = str(s).strip()
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        if len(parts) == 2:
            s = parts[1] + " " + parts[0]
    else:
        toks = s.split()
        if len(toks) >= 2:
            s = " ".join(toks[1:] + [toks[0]])
    return " ".join(w.capitalize() for w in s.split())


INSIDER_CLEVEL = ["CHIEF", "CEO", "CFO", "COO", "CTO", "PRESIDENT", "CHAIR", "DIRECTOR", "OFFICER", "FOUNDER", "10%", "VICE PRESIDENT", "EVP", "SVP"]


def classify_insider_kind(text):
    # Shared with the full report so the sector list and the report can never disagree.
    t = str(text).lower()
    if any(w in t for w in ["award", "grant", "gift", "bonus"]):
        return "grant"
    if any(w in t for w in ["exercise", "conversion", "convert", "option", "derivative"]):
        return "option"
    if any(w in t for w in ["tax", "withh", "surrender", "forfeit"]):
        return "tax"
    if any(w in t for w in ["sale", "sold", "sell"]):
        return "sell"
    if any(w in t for w in ["purchase", "bought"]):
        return "buy"
    if "dispos" in t:
        return "sell"
    if "acqui" in t:
        return "buy"
    return "other"


def is_strong_uptrend(info, cur):
    # A stock up big over the past year. Used to read insider selling in context: trimming after
    # a big run is profit taking, not a warning. Prefer the trailing one year return; fall back
    # to price well above the 200 day average when the yearly figure is missing.
    try:
        yr = info.get("52WeekChange")
        if isinstance(yr, (int, float)):
            return yr >= 0.40
        dma200 = info.get("twoHundredDayAverage")
        if isinstance(dma200, (int, float)) and dma200 > 0 and isinstance(cur, (int, float)):
            return cur >= dma200 * 1.20
    except Exception:
        pass
    return False


def insider_selling_cap(ticker_obj, cur_price, strong_uptrend=False):
    # Returns True when a cluster of executives is selling, the same rule the full report uses
    # to refuse an APPROVE. In a strong uptrend that selling is profit taking, not a warning,
    # so it never caps. Used by the sector list so it never contradicts the full report.
    if strong_uptrend:
        return False
    try:
        it = ticker_obj.insider_transactions
        if it is None or it.empty:
            return False
        clevel_sells = 0
        exec_value = 0
        price = cur_price if isinstance(cur_price, (int, float)) and cur_price > 0 else 0
        for _, rr in it.head(12).iterrows():
            row = rr.to_dict()
            pos = row.get("Position") or row.get("Title") or row.get("Relation") or ""
            desc = row.get("Transaction") or row.get("Text") or ""
            basis = str(desc) if str(desc).strip() else " ".join(str(v) for v in row.values())
            kind = classify_insider_kind(basis)
            is_cl = any(c in str(pos).upper() for c in INSIDER_CLEVEL)
            if is_cl and kind == "sell":
                clevel_sells += 1
                try:
                    exec_value += int(float(row.get("Shares") or 0)) * price
                except Exception:
                    pass
        return clevel_sells >= 3 or exec_value >= 20000000
    except Exception:
        return False


def run_referee(cur, chg, pe, tgt, rec, market_cap, volume, beta, hist, news, congressional, insider):
    # The referee checks every number for sanity before it reaches the screen,
    # raises plain English flags for anything stale, missing, or unusual, and
    # scores how much of the picture is solid so the report can state its confidence honestly.
    flags = []

    def warn(t):
        flags.append({"level": "warn", "text": t})

    def note(t):
        flags.append({"level": "info", "text": t})

    price_ok = isinstance(cur, (int, float)) and cur > 0
    if not price_ok:
        warn("The live price did not come back cleanly, so this read may be unreliable. Check the price on another source before trusting it.")

    target_ok = (tgt != "N/A" and tgt is not None)
    if target_ok and price_ok:
        try:
            ratio = float(tgt) / cur
            if ratio >= 1.40 or ratio < 0.34:
                warn("The analyst price target sits unusually far from the current price, which can mean it is stale or an outlier. Treat the upside it implies with caution rather than at face value.")
        except Exception:
            pass

    pe_ok = (pe != "N/A" and pe is not None)
    if pe_ok:
        try:
            pn = float(pe)
            if pn < 0:
                warn("This company has no positive earnings right now, so the PE ratio is not meaningful. That is common for fast growing or turnaround companies, but it adds risk.")
            elif pn > 200:
                warn("The PE ratio is extremely high, which means either very high growth expectations or unusually low earnings. Either way the valuation is stretched.")
        except Exception:
            pe_ok = False

    beta_ok = (beta != "N/A" and beta is not None)
    if beta_ok:
        try:
            b = float(beta)
            if b < -1 or b > 4:
                warn("The volatility reading is unusual, which can happen with newer or thinly traded stocks. The risk numbers here may be less reliable.")
        except Exception:
            beta_ok = False

    vol_ok = isinstance(volume, (int, float)) and volume > 0
    if vol_ok and volume < 100000:
        warn("This stock trades on low daily volume. Thinly traded stocks can swing hard and can be harder to buy or sell at a fair price.")

    mc_ok = isinstance(market_cap, (int, float))
    if mc_ok and market_cap < 300000000:
        warn("This is a very small company. Small companies can grow fast but are more volatile and carry higher risk.")

    news_ok = bool(news)
    if not news_ok:
        note("No recent company news was found, so this read leans on the numbers more than the story.")

    smart_ok = bool(congressional) or bool(insider)
    hist_ok = hist is not None and len(hist) >= 2

    data_points = sum(1 for x in [price_ok, hist_ok, pe_ok, target_ok, beta_ok, mc_ok, news_ok, smart_ok] if x)
    warns = len([f for f in flags if f["level"] == "warn"])

    if (not price_ok) or warns >= 2 or data_points <= 3:
        confidence = "Low"
    elif warns >= 1 or data_points <= 5:
        confidence = "Medium"
    else:
        confidence = "High"

    return confidence, flags


# ---------- yfinance fallback router ----------
# This router tries yfinance first and, on any failure or empty result, falls back to Finnhub then
# FMP for daily OHLCV, returning the same shape everywhere so charts and scores never look broken.
# Every fallback is logged as a warning. Intraday intervals stay on yfinance, since the fallback
# providers' free tiers are daily only; an intraday miss returns empty rather than a wrong series.
def _yf_history_rows(symbol, period, interval):
    t = yf.Ticker(symbol)
    df = t.history(period=period, interval=interval)
    rows = []
    if df is None or df.empty:
        return rows
    for idx, row in df.iterrows():
        try:
            rows.append({
                "time": int(idx.timestamp()),
                "open": round(float(row["Open"]), 2),
                "high": round(float(row["High"]), 2),
                "low": round(float(row["Low"]), 2),
                "close": round(float(row["Close"]), 2),
                "volume": int(row["Volume"]) if row["Volume"] == row["Volume"] else 0,
            })
        except (ValueError, KeyError, TypeError):
            continue
    return rows


def _finnhub_history_rows(symbol, period):
    key = os.environ.get("FINNHUB_KEY", "").strip()
    if not key:
        return []
    days = {"5d": 5, "1mo": 31, "3mo": 93, "6mo": 186, "1y": 366, "2y": 731, "5y": 1827}.get(period, 366)
    to_ts = int(time.time())
    from_ts = to_ts - days * 86400
    try:
        r = requests.get("https://finnhub.io/api/v1/stock/candle",
                         params={"symbol": symbol, "resolution": "D", "from": from_ts, "to": to_ts, "token": key},
                         timeout=12)
        d = r.json()
        if d.get("s") != "ok":
            return []
        rows = []
        for i in range(len(d.get("t", []))):
            rows.append({"time": int(d["t"][i]), "open": round(d["o"][i], 2), "high": round(d["h"][i], 2),
                         "low": round(d["l"][i], 2), "close": round(d["c"][i], 2), "volume": int(d["v"][i])})
        return rows
    except Exception as e:
        logger.error("finnhub fallback %s: %s" % (symbol, e))
        return []


def _fmp_history_rows(symbol, period):
    key = os.environ.get("FMP_KEY", "").strip()
    if not key:
        return []
    try:
        r = requests.get("https://financialmodelingprep.com/api/v3/historical-price-full/" + symbol,
                         params={"apikey": key}, timeout=12)
        d = r.json()
        hist = d.get("historical") if isinstance(d, dict) else None
        if not hist:
            return []
        import datetime as _dt
        rows = []
        for bar in hist:
            try:
                ts = int(_dt.datetime.strptime(bar["date"], "%Y-%m-%d").timestamp())
                # Fall back to close for any missing OHLC field so a bar is never zeroed out.
                _c = bar.get("close", 0) or 0
                rows.append({"time": ts, "open": round(bar.get("open") or _c, 2), "high": round(bar.get("high") or _c, 2),
                             "low": round(bar.get("low") or _c, 2), "close": round(_c, 2),
                             "volume": int(bar.get("volume", 0) or 0)})
            except (ValueError, KeyError, TypeError):
                continue
        rows.sort(key=lambda x: x["time"])
        return rows
    except Exception as e:
        logger.error("fmp fallback %s: %s" % (symbol, e))
        return []


def _fmp_stable_history(symbol, period):
    """FMP 'stable' EOD price endpoint. Available on more plan tiers than the v3 endpoints."""
    key = os.environ.get("FMP_KEY", "").strip()
    if not key:
        return []
    try:
        r = requests.get("https://financialmodelingprep.com/stable/historical-price-eod/full",
                         params={"symbol": symbol, "apikey": key}, timeout=12)
        d = r.json()
        bars = d if isinstance(d, list) else (d.get("historical") if isinstance(d, dict) else None)
        if not bars:
            return []
        import datetime as _dt
        rows = []
        for bar in bars:
            try:
                ds = bar.get("date") or bar.get("datetime")
                ts = int(_dt.datetime.strptime(str(ds)[:10], "%Y-%m-%d").timestamp())
                _c = bar.get("close", 0) or 0
                rows.append({"time": ts, "open": round(bar.get("open") or _c, 2), "high": round(bar.get("high") or _c, 2),
                             "low": round(bar.get("low") or _c, 2), "close": round(_c, 2),
                             "volume": int(bar.get("volume", 0) or 0)})
            except (ValueError, KeyError, TypeError):
                continue
        rows.sort(key=lambda x: x["time"])
        return rows
    except Exception as e:
        logger.error("fmp stable history %s: %s" % (symbol, e))
        return []


def _alphavantage_history(symbol, period):
    """Alpha Vantage daily prices. Free tier (key AV_KEY); a reliable independent backstop."""
    key = os.environ.get("AV_KEY", "").strip()
    if not key:
        return []
    try:
        r = requests.get("https://www.alphavantage.co/query",
                         params={"function": "TIME_SERIES_DAILY", "symbol": symbol,
                                 "outputsize": "compact", "apikey": key}, timeout=15)
        d = r.json()
        series = d.get("Time Series (Daily)")
        if not series:
            return []
        import datetime as _dt
        rows = []
        for ds, bar in series.items():
            try:
                ts = int(_dt.datetime.strptime(ds, "%Y-%m-%d").timestamp())
                rows.append({"time": ts, "open": round(float(bar["1. open"]), 2),
                             "high": round(float(bar["2. high"]), 2), "low": round(float(bar["3. low"]), 2),
                             "close": round(float(bar["4. close"]), 2), "volume": int(float(bar["5. volume"]))})
            except (ValueError, KeyError, TypeError):
                continue
        rows.sort(key=lambda x: x["time"])
        return rows
    except Exception as e:
        logger.error("alphavantage history %s: %s" % (symbol, e))
        return []


def _stooq_history(symbol, period):
    """Stooq CSV daily prices. Completely free, no key, no auth. A dependable last-resort source
    that does not block cloud IPs the way Yahoo does, so it works from Railway."""
    try:
        s = symbol.lower()
        if "." not in s:
            s = s + ".us"
        r = requests.get("https://stooq.com/q/d/l/", params={"s": s, "i": "d"}, timeout=12)
        if r.status_code != 200 or not r.text or "Date" not in r.text[:50]:
            return []
        import datetime as _dt, csv as _csv, io as _io
        rows = []
        reader = _csv.DictReader(_io.StringIO(r.text))
        for bar in reader:
            try:
                ts = int(_dt.datetime.strptime(bar["Date"], "%Y-%m-%d").timestamp())
                _c = float(bar["Close"])
                rows.append({"time": ts, "open": round(float(bar.get("Open") or _c), 2),
                             "high": round(float(bar.get("High") or _c), 2), "low": round(float(bar.get("Low") or _c), 2),
                             "close": round(_c, 2), "volume": int(float(bar.get("Volume") or 0))})
            except (ValueError, KeyError, TypeError):
                continue
        rows.sort(key=lambda x: x["time"])
        return rows
    except Exception as e:
        logger.error("stooq history %s: %s" % (symbol, e))
        return []


def _tiingo_history(symbol, period):
    """Tiingo daily prices. Set DATA_PROVIDER=tiingo and TIINGO_KEY to make this the primary source.

    NOTE ON LICENSING: Tiingo's self-serve tiers are 'Internal Use Only'. That is fine for testing
    and development, but a commercial/redistribution license is required BEFORE showing this data to
    public users. Do not launch on an internal-use license.
    """
    key = os.environ.get("TIINGO_KEY", "").strip()
    if not key:
        return []
    try:
        from datetime import date as _date, timedelta as _td2
        days = {"5d": 10, "1mo": 40, "3mo": 100, "6mo": 190, "1y": 380, "5y": 1850}.get(period, 380)
        start = (_date.today() - _td2(days=days)).isoformat()
        r = requests.get("https://api.tiingo.com/tiingo/daily/%s/prices" % symbol.lower(),
                         params={"startDate": start, "token": key},
                         headers={"Content-Type": "application/json"}, timeout=15)
        if r.status_code != 200:
            logger.warning("tiingo %s: HTTP %s" % (symbol, r.status_code))
            return []
        bars = r.json()
        if not isinstance(bars, list) or not bars:
            return []
        import datetime as _dt
        rows = []
        for bar in bars:
            try:
                ts = int(_dt.datetime.strptime(str(bar["date"])[:10], "%Y-%m-%d").timestamp())
                _c = float(bar.get("adjClose") or bar.get("close") or 0)
                if _c <= 0:
                    continue
                rows.append({"time": ts,
                             "open": round(float(bar.get("adjOpen") or bar.get("open") or _c), 2),
                             "high": round(float(bar.get("adjHigh") or bar.get("high") or _c), 2),
                             "low": round(float(bar.get("adjLow") or bar.get("low") or _c), 2),
                             "close": round(_c, 2),
                             "volume": int(bar.get("adjVolume") or bar.get("volume") or 0)})
            except (ValueError, KeyError, TypeError):
                continue
        rows.sort(key=lambda x: x["time"])
        return rows
    except Exception as e:
        logger.error("tiingo history %s: %s" % (symbol, e))
        return []


def _polygon_history(symbol, period):
    """Polygon.io daily aggregates. Set DATA_PROVIDER=polygon and POLYGON_KEY to use as primary."""
    key = os.environ.get("POLYGON_KEY", "").strip()
    if not key:
        return []
    try:
        from datetime import date as _date, timedelta as _td2
        days = {"5d": 10, "1mo": 40, "3mo": 100, "6mo": 190, "1y": 380, "5y": 1850}.get(period, 380)
        end = _date.today().isoformat()
        start = (_date.today() - _td2(days=days)).isoformat()
        r = requests.get("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s"
                         % (symbol.upper(), start, end),
                         params={"adjusted": "true", "sort": "asc", "limit": 5000, "apiKey": key},
                         timeout=15)
        if r.status_code != 200:
            logger.warning("polygon %s: HTTP %s" % (symbol, r.status_code))
            return []
        d = r.json()
        bars = d.get("results") or []
        rows = []
        for bar in bars:
            try:
                ts = int(bar["t"] / 1000)  # polygon uses ms epoch
                _c = float(bar.get("c") or 0)
                if _c <= 0:
                    continue
                rows.append({"time": ts, "open": round(float(bar.get("o") or _c), 2),
                             "high": round(float(bar.get("h") or _c), 2),
                             "low": round(float(bar.get("l") or _c), 2),
                             "close": round(_c, 2), "volume": int(bar.get("v") or 0)})
            except (ValueError, KeyError, TypeError):
                continue
        rows.sort(key=lambda x: x["time"])
        return rows
    except Exception as e:
        logger.error("polygon history %s: %s" % (symbol, e))
        return []


# Provider registry. Set DATA_PROVIDER in the environment to make one of these the PRIMARY source,
# tried before everything else. Swapping providers is a one-line env change, no code edits, so you
# can test a provider on a cheap tier and move to the commercial license later without a rewrite.
PRIMARY_PROVIDERS = {
    "tiingo": _tiingo_history,
    "polygon": _polygon_history,
    "fmp": lambda s, p: _fmp_history_rows(s, p) or _fmp_stable_history(s, p),
}


def _primary_history(symbol, period):
    """Try the configured primary provider first, if one is set. Returns [] when unset or on failure,
    so the free fallback chain still runs underneath."""
    name = os.environ.get("DATA_PROVIDER", "").strip().lower()
    if not name:
        return []
    fn = PRIMARY_PROVIDERS.get(name)
    if not fn:
        logger.warning("DATA_PROVIDER '%s' is not a known provider; ignoring" % name)
        return []
    try:
        rows = fn(symbol, period)
        if rows:
            logger.info("primary provider %s served %s (%d bars)" % (name, symbol, len(rows)))
            return rows
        logger.warning("primary provider %s returned nothing for %s; falling back" % (name, symbol))
    except Exception as e:
        logger.error("primary provider %s failed for %s: %s" % (name, symbol, e))
    return []


def fetch_with_fallback(symbol, period="1y", interval="1d"):
    """OHLCV rows for a symbol, trying every available source until one returns data.

    Full fallback chain, because every source can fail independently: Yahoo blocks cloud IPs,
    Finnhub deprecated its candle endpoint, and FMP price endpoints require specific plan tiers.
    So we try them ALL and let whichever works win, logging which source served each request:

        1. yfinance            (free, but blocks cloud IPs)
        2. Stooq               (free, no key, works from cloud)  <- reliable on Railway
        3. FMP v3 history      (needs a plan tier with price data)
        4. FMP stable history  (available on more tiers)
        5. Finnhub candle      (deprecated for most, kept just in case)
        6. Alpha Vantage       (free key, independent backstop)
    """
    # 0. The configured PRIMARY provider (paid, licensed) always runs first when DATA_PROVIDER is set.
    # Everything below it is a free backstop for when the primary is unset, down, or over quota.
    if interval == "1d":
        rows = _primary_history(symbol, period)
        if rows:
            return rows

    try:
        rows = _yf_history_rows(symbol, period, interval)
        if rows:
            return rows
        logger.warning("fallback: yfinance empty for %s" % symbol)
    except Exception as e:
        logger.warning("fallback: yfinance failed for %s (%s)" % (symbol, e))

    if interval != "1d":
        return []

    rows = _stooq_history(symbol, period)
    if rows:
        logger.warning("fallback: served %s from Stooq" % symbol)
        return rows

    rows = _fmp_history_rows(symbol, period)
    if rows:
        logger.warning("fallback: served %s from FMP v3" % symbol)
        return rows

    rows = _fmp_stable_history(symbol, period)
    if rows:
        logger.warning("fallback: served %s from FMP stable" % symbol)
        return rows

    rows = _finnhub_history_rows(symbol, period)
    if rows:
        logger.warning("fallback: served %s from Finnhub" % symbol)
        return rows

    rows = _alphavantage_history(symbol, period)
    if rows:
        logger.warning("fallback: served %s from Alpha Vantage" % symbol)
        return rows

    logger.error("fallback: ALL price sources failed for %s" % symbol)
    return []


# ---------- AI response cache ----------
# Stores AI answers and generated audio in Postgres so identical questions and read alouds return
# instantly at no provider cost. Keyed by a hash of the prompt or text; entries expire by age.
def get_cached_ai(key, max_age_hours=3):
    conn = get_db()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT response, audio_path FROM ai_cache WHERE cache_key = %s AND "
                    "created_at > NOW() - INTERVAL '%s hours'" % ("%s", int(max_age_hours)), (key,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            return {"response": row[0], "audio_path": row[1]}
        return None
    except Exception as e:
        logger.error("get_cached_ai: %s" % e)
        try:
            conn.close()
        except Exception:
            pass
        return None


def set_cached_ai(key, response_text, audio_path=None):
    conn = get_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO ai_cache (cache_key, response, audio_path) VALUES (%s, %s, %s) "
            "ON CONFLICT (cache_key) DO UPDATE SET response = EXCLUDED.response, "
            "audio_path = EXCLUDED.audio_path, created_at = NOW()",
            (key, response_text, audio_path))
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        logger.error("set_cached_ai: %s" % e)
        try:
            conn.close()
        except Exception:
            pass


@app.route("/auth/device", methods=["POST"])
def auth_device():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    b = request.get_json(silent=True) or {}
    token = (b.get("device_token") or "").strip()
    if not token:
        return jsonify({"error": "no token"}), 400
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database unavailable."}), 500
    cur = conn.cursor()
    cur.execute("INSERT INTO user_devices (user_id, device_token, platform) VALUES (%s, %s, 'web') "
                "ON CONFLICT (user_id, device_token) DO NOTHING", (u["id"], token))
    # Mirror to the users token column the alert engine already reads.
    cur.execute("UPDATE users SET onesignal_token = %s WHERE id = %s", (token, u["id"]))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@app.route("/auth/agree-tos", methods=["POST"])
def auth_agree_tos():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database unavailable."}), 500
    cur = conn.cursor()
    cur.execute("UPDATE users SET agreed_tos = true WHERE id = %s", (u["id"],))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@app.route("/analyze")
def analyze():
    query = request.args.get("symbol", "").strip()
    if not query:
        return jsonify({"error": "No symbol provided"}), 400
    symbol = resolve_ticker(query)
    logger.info(f"ANALYZE: {query} -> {symbol}")
    # Major indices and futures get a clean, simplified report: price and change, a one line market
    # context, no company scoring, insider, congressional, or moat sections, since a basket has none.
    if _is_index_symbol(symbol):
        idx = _build_index_report(symbol)
        if idx:
            return jsonify(idx)
    result = compute_full_report(symbol)
    if result is None:
        return jsonify({"error": f"Could not pull data for {symbol}."}), 404
    ac = _asset_class(symbol)
    if ac != "stock":
        result = dict(result)
        result["asset_class"] = ac
        for k in ("insider", "congressional", "apex_moat", "revenue_growth", "profit_margin",
                  "debt_to_equity", "roe", "fcf_yield", "peg", "pb", "ps", "ev_ebitda",
                  "analyst_consensus", "sec_filings", "sector_guide"):
            if k in result:
                result[k] = None
    # Read only: flag a verdict flip against the stored baseline without acknowledging it. The
    # baseline advances only when the frontend calls /acknowledge-verdict, so the change keeps
    # surfacing in alerts and on the badge until the user has actually opened and seen it.
    result = _attach_verdict_change(result, symbol)
    # Opportunistic custom alert evaluation: if a logged in user views a stock, run their alerts
    # for it on a background thread so it never slows the response.
    try:
        _au = current_user()
        if _au and _asset_class(symbol) == "stock":
            _r = dict(result)
            threading.Thread(target=check_alerts_for_symbol, args=(_au["id"], symbol, _r), daemon=True).start()
    except Exception as _ae:
        logger.error("analyze alert hook: %s" % _ae)
    return jsonify(result)


@app.route("/acknowledge-verdict", methods=["POST"])
def acknowledge_verdict():
    # Marks a verdict change as seen for this user and symbol by advancing the stored baseline to
    # the current verdict. The frontend calls this silently once it has shown the change banner on
    # the report, so the same flip will not alert again.
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    data = request.get_json(silent=True) or {}
    raw = (data.get("symbol") or "").strip()
    if not raw:
        return jsonify({"error": "no_symbol"}), 400
    symbol = resolve_ticker(raw)
    report = compute_full_report(symbol)
    if report and report.get("verdict"):
        set_verdict(u["id"], symbol, report.get("verdict"))
    return jsonify({"ok": True})


# CHUNK: pre-market and post-market move, computed from the live quote against the regular close
def extended_hours(info, cur):
    try:
        state = (info.get("marketState") or "").upper()
        if state in ("PRE", "PREPRE"):
            price = info.get("preMarketPrice")
            label = "pre market"
        elif state in ("POST", "POSTPOST"):
            price = info.get("postMarketPrice")
            label = "after hours"
        else:
            return None
        if price is None or not cur:
            return None
        price = round(float(price), 2)
        chg = round(((price - cur) / cur) * 100, 2)
        if abs(chg) < 0.1:
            return None
        return {"session": label, "state": state, "price": price, "change_pct": chg}
    except Exception:
        return None


# CHUNK: flag a just released or imminent earnings report so the move has context
def earnings_flag(info):
    try:
        ts = info.get("earningsTimestamp") or info.get("earningsTimestampStart")
        if not ts:
            return None
        hrs = (time.time() - float(ts)) / 3600.0
        if 0 <= hrs <= 36:
            return "recent"
        if -36 <= hrs < 0:
            return "soon"
        return None
    except Exception:
        return None


# CHUNK: normalize a news timestamp from any source into unix seconds. Handles unix ints in
# seconds or milliseconds, and ISO 8601 strings with or without a Z or fractional seconds. This
# is what makes the "x hours ago" stamp show on yfinance items, whose dates are ISO strings.
def _news_ts(val):
    if not val:
        return 0
    if isinstance(val, (int, float)):
        v = int(val)
        return v // 1000 if v > 100000000000 else v
    s = str(val).strip()
    if s.isdigit():
        v = int(s)
        return v // 1000 if v > 100000000000 else v
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
    except Exception:
        pass
    try:
        base = re.split(r"[.+]", s)[0]
        return int(datetime.strptime(base, "%Y-%m-%dT%H:%M:%S").timestamp())
    except Exception:
        return 0


# CHUNK: a summary should be a sentence, never a bare link. Drop it if it is a URL or just the
# article link, so a raw URL never shows up where the teaser belongs.
def _clean_summary(summary, link=""):
    s = clean_text(summary or "")
    if not s:
        return ""
    if re.match(r"^https?://", s, re.I):
        return ""
    if link and s.strip() == str(link).strip():
        return ""
    return trim_words(s, 240)


def _full_summary(summary, link=""):
    # The full cleaned article text for the read-more modal. Same junk guards as the card preview
    # (drop a bare URL or a summary that is only the link), but no word trim. Capped at 1000 chars.
    s = clean_text(summary or "")
    if not s:
        return ""
    if re.match(r"^https?://", s, re.I):
        return ""
    if link and s.strip() == str(link).strip():
        return ""
    return s[:1000]


# CHUNK: company news from yfinance, the reliable backbone source. Parsed defensively for both the
# old flat format and the newer nested 'content' format, so a widely covered name like Apple always
# has company specific news instead of falling through to a general feed. Newest first, time stamped.
def yf_company_news(ticker_obj):
    out = []
    try:
        raw = ticker_obj.news or []
    except Exception:
        return out
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            c = item.get("content")
            if isinstance(c, dict):
                title = c.get("title")
                prov = (c.get("provider") or {}).get("displayName") or "News"
                link = ((c.get("canonicalUrl") or {}).get("url")
                        or (c.get("clickThroughUrl") or {}).get("url") or "")
                summary = c.get("summary") or c.get("description") or ""
                ts = _news_ts(c.get("pubDate") or c.get("displayTime") or c.get("providerPublishTime"))
            else:
                title = item.get("title")
                prov = item.get("publisher") or "News"
                link = item.get("link") or ""
                summary = item.get("summary") or ""
                ts = _news_ts(item.get("providerPublishTime"))
            if title:
                out.append({
                    "headline": clean_text(title),
                    "source": clean_text(prov),
                    "summary": _clean_summary(summary, link),
                    "summary_long": _full_summary(summary, link),
                    "url": link or "",
                    "ts": ts,
                })
        except Exception:
            continue
    out.sort(key=lambda a: a.get("ts", 0), reverse=True)
    return out


# CHUNK: shared full-report engine so Ask and the report use the same verdict
def build_news(symbol, ticker):
    # Company news, shared by the stock and ETF reports. Finnhub first when a key is present, then
    # yfinance as a reliable backbone so a covered name never shows "no company news", then a general
    # market feed as a last resort. Newest first, time stamped, with full text for the read-more modal.
    news = []
    if FINNHUB_KEY:
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            from_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
            fcu = f"https://finnhub.io/api/v1/company-news?symbol={symbol}&from={from_date}&to={today}&token={FINNHUB_KEY}"
            r = requests.get(fcu, timeout=8)
            cnt = len(r.json()) if r.status_code == 200 else 0
            logger.info("finnhub company-news %s status %s count %s" % (symbol, r.status_code, cnt))
            if r.status_code == 200:
                arts = [n for n in r.json() if n.get("headline")]
                arts.sort(key=lambda a: a.get("datetime", 0), reverse=True)
                for n in arts[:6]:
                    news.append({"headline": clean_text(n["headline"]), "source": clean_text(n.get("source", "News")), "summary": _clean_summary(n.get("summary", ""), n.get("url", "")), "summary_long": _full_summary(n.get("summary", ""), n.get("url", "")), "url": n.get("url", ""), "ts": _news_ts(n.get("datetime", 0))})
        except Exception as e:
            logger.error("finnhub company-news error %s: %s" % (symbol, e))

    if not news:
        try:
            yn = yf_company_news(ticker)
            logger.info("yfinance news %s count %s" % (symbol, len(yn)))
            if yn:
                news = yn[:6]
        except Exception as e:
            logger.error("yfinance news error %s: %s" % (symbol, e))

    if not news and FINNHUB_KEY:
        try:
            gu = f"https://finnhub.io/api/v1/news?category=general&token={FINNHUB_KEY}"
            r2 = requests.get(gu, timeout=8)
            if r2.status_code == 200:
                garts = [n for n in r2.json() if n.get("headline")]
                garts.sort(key=lambda a: a.get("datetime", 0), reverse=True)
                for n in garts[:4]:
                    news.append({"headline": clean_text(n["headline"]), "source": clean_text(n.get("source", "Market News")) + " (General)", "summary": _clean_summary(n.get("summary", ""), n.get("url", "")), "summary_long": _full_summary(n.get("summary", ""), n.get("url", "")), "url": n.get("url", ""), "ts": _news_ts(n.get("datetime", 0))})
        except Exception as e:
            logger.error("finnhub general-news error: %s" % e)
    return news


def build_etf_report(symbol, ticker, info, hist, cur, chg):
    # ETFs are judged on cost, diversification, and what they hold, not the stock scoring engine. This
    # builds a tailored educational payload and reuses the exact same news pipeline as the stock report.
    def to_pct(v):
        if not isinstance(v, (int, float)):
            return None
        return round(v * 100, 2) if abs(v) < 1 else round(v, 2)

    news = build_news(symbol, ticker)
    market_cap = info.get("marketCap", "N/A")
    volume = int(hist["Volume"].iloc[-1]) if not hist.empty else 0
    beta = fmt_price(info.get("beta"))
    confidence, flags = run_referee(cur, chg, "N/A", "N/A", "HOLD", market_cap, volume, beta, hist, news, [], [])

    earn = earnings_flag(info)
    ext = extended_hours(info, cur)
    ext_note = ""
    if ext:
        direction = "up" if ext["change_pct"] >= 0 else "down"
        ext_note = "%s is %s %s percent in %s trading, at about $%s. The figures below are based on the regular session close, not this move." % (
            symbol, direction, abs(ext["change_pct"]), ext["session"], ext["price"])

    er_raw = info.get("expenseRatio")
    if er_raw is None:
        er_raw = info.get("annualReportExpenseRatio")
    if er_raw is None:
        er_raw = info.get("netExpenseRatio")
    if er_raw is None:
        er_raw = info.get("operatingExpense")
    expense_ratio = to_pct(er_raw)
    if expense_ratio is None:
        expense_ratio = "N/A"

    y_raw = info.get("yield")
    if y_raw is None:
        y_raw = info.get("dividendYield")
    etf_yield = to_pct(y_raw)
    if etf_yield is None:
        etf_yield = "N/A"

    total_assets = info.get("totalAssets")
    if not isinstance(total_assets, (int, float)):
        total_assets = "N/A"

    # Top holdings: try .info first, then the funds_data feed where current yfinance keeps fund data.
    holdings = []
    raw_h = info.get("holdings")
    if isinstance(raw_h, list):
        for h in raw_h[:10]:
            if isinstance(h, dict):
                holdings.append({"symbol": h.get("symbol") or h.get("holdingName") or "", "name": h.get("holdingName") or "", "weight": to_pct(h.get("holdingPercent"))})
    if not holdings:
        try:
            th = ticker.funds_data.top_holdings
            if th is not None and hasattr(th, "iterrows"):
                cols = list(th.columns)
                wcol = "Holding Percent" if "Holding Percent" in cols else ("holdingPercent" if "holdingPercent" in cols else None)
                ncol = "Name" if "Name" in cols else None
                for sym_idx, row in th.head(10).iterrows():
                    holdings.append({
                        "symbol": str(sym_idx),
                        "name": str(row[ncol]) if ncol else "",
                        "weight": to_pct(row[wcol]) if wcol else None,
                    })
        except Exception:
            pass

    # Sector weightings: normalize a dict or a list of single key dicts, then fall back to funds_data.
    sector_weights = {}
    sw_raw = info.get("sectorWeightings")
    if isinstance(sw_raw, dict):
        for k, v in sw_raw.items():
            p = to_pct(v)
            if p is not None:
                sector_weights[k] = p
    elif isinstance(sw_raw, list):
        for item in sw_raw:
            if isinstance(item, dict):
                for k, v in item.items():
                    p = to_pct(v)
                    if p is not None:
                        sector_weights[k] = p
    if not sector_weights:
        try:
            sw2 = ticker.funds_data.sector_weightings
            if isinstance(sw2, dict):
                for k, v in sw2.items():
                    p = to_pct(v)
                    if p is not None:
                        sector_weights[k] = p
        except Exception:
            pass

    fw_high = info.get("fiftyTwoWeekHigh")
    fw_low = info.get("fiftyTwoWeekLow")

    # CHUNK: ETF quality snapshot. A simple, transparent score from cost, size, and diversification,
    # turned into a plain label. Educational shorthand, never a recommendation.
    etf_quality = None
    try:
        er_val = float(expense_ratio) if expense_ratio != "N/A" else None
        aum_val = float(total_assets) if total_assets != "N/A" else None
        num_holdings = len(holdings)
        score = 0
        if er_val is not None:
            if er_val <= 0.10:
                score += 3
            elif er_val <= 0.30:
                score += 2
            elif er_val <= 0.60:
                score += 1
        if aum_val is not None:
            if aum_val >= 10e9:
                score += 2
            elif aum_val >= 1e9:
                score += 1
        if num_holdings >= 500:
            score += 2
        elif num_holdings >= 100:
            score += 1
        if score >= 6:
            quality_label = "Low Cost, Well Diversified"
        elif score >= 4:
            quality_label = "Moderate Cost, Adequately Diversified"
        elif score >= 2:
            quality_label = "Higher Cost or Concentrated"
        else:
            quality_label = "Costly or Narrow"
        etf_quality = {
            "score": score,
            "label": quality_label,
            "expense_ratio": expense_ratio,
            "total_assets": total_assets,
            "num_holdings": num_holdings,
        }
    except Exception as e:
        logger.error("ETF quality score error for %s: %s" % (symbol, e))
        etf_quality = None

    result = {
        "symbol": symbol,
        "name": info.get("longName", symbol),
        "sector": info.get("category", "") or "",
        "price": cur,
        "change_pct": chg,
        "market_cap": market_cap,
        "volume": volume,
        "beta": beta,
        "confidence": confidence,
        "flags": flags,
        "verdict": "ETF",
        "quoteType": "ETF",
        "expense_ratio": expense_ratio,
        "total_assets": total_assets,
        "category": info.get("category") or "N/A",
        "fund_family": info.get("fundFamily") or "N/A",
        "yield": etf_yield,
        "holdings": holdings,
        "sector_weights": sector_weights,
        "etf_quality": etf_quality,
        "fifty_two_week_high": fw_high if isinstance(fw_high, (int, float)) else "N/A",
        "fifty_two_week_low": fw_low if isinstance(fw_low, (int, float)) else "N/A",
        "news": news,
        "extended": ext,
        "earnings": earn,
        "extended_note": ext_note,
        "suggested_questions": [
            "What is the expense ratio and why does it matter?",
            "What are the top holdings?",
            "How diversified is this ETF?",
            "Explain this ETF report in plain English",
        ],
        "data_timestamp": int(time.time()),
    }
    return result


def _pretty_rating(key):
    # yfinance recommendationKey to a clean label, e.g. moderate_buy becomes Moderate Buy.
    if not key or not isinstance(key, str):
        return None
    k = key.strip().lower()
    if k in ("none", "", "n/a"):
        return None
    mapping = {
        "strong_buy": "Strong Buy", "buy": "Buy", "moderate_buy": "Moderate Buy",
        "outperform": "Outperform", "overweight": "Overweight", "hold": "Hold",
        "neutral": "Hold", "underperform": "Underperform", "underweight": "Underweight",
        "moderate_sell": "Moderate Sell", "sell": "Sell", "strong_sell": "Strong Sell",
    }
    if k in mapping:
        return mapping[k]
    return " ".join(w.capitalize() for w in k.replace("-", " ").replace("_", " ").split())


def read_verdict(user_id, symbol):
    # Pure read. Returns the last verdict stored for this user and symbol, or None if there is
    # no baseline yet. Never writes, so it is safe to call from a report view without
    # acknowledging anything.
    conn = get_db()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT last_verdict FROM verdict_history WHERE user_id=%s AND symbol=%s", (user_id, symbol))
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None
    except Exception as e:
        logger.error("read_verdict %s %s: %s" % (user_id, symbol, e))
        return None
    finally:
        conn.close()


def set_verdict(user_id, symbol, verdict):
    # Upsert the baseline for this user and symbol to the given verdict. This is the only place
    # the stored verdict advances, so a change stays unacknowledged until this runs (the
    # acknowledge endpoint, or the alerts pass establishing a first baseline). Blanks and ETFs
    # are never stored, so they never produce a flip.
    if not verdict or verdict == "ETF":
        return
    conn = get_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO verdict_history (user_id, symbol, last_verdict, last_checked) VALUES (%s,%s,%s,NOW()) "
            "ON CONFLICT (user_id, symbol) DO UPDATE SET last_verdict=EXCLUDED.last_verdict, last_checked=NOW()",
            (user_id, symbol, verdict),
        )
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error("set_verdict %s %s: %s" % (user_id, symbol, e))
    finally:
        conn.close()


def verdict_signal_reason(verdict, ins_buys, ins_sells, exec_sell_value, cong_buys, cong_sells, sharp_drop, eff_chg, rec, up, heavy_insider_selling):
    # Plain English for the single signal most responsible for the current verdict. Derived only
    # from the live signals in this report, so it is honest about what is driving the call now.
    chg_up = isinstance(eff_chg, (int, float)) and eff_chg > 2
    chg_dn = isinstance(eff_chg, (int, float)) and eff_chg < -3
    up_big = isinstance(up, (int, float)) and up > 10
    if verdict == "APPROVE":
        if ins_buys >= 1:
            return "insider buying detected"
        if cong_buys >= 2:
            return "lawmakers have been buying"
        if rec in ("BUY", "STRONG_BUY"):
            return "analysts turned more positive"
        if chg_up:
            return "price momentum improved"
        if up_big:
            return "analyst upside expanded"
        return "the positive signals now outweigh the negatives"
    if verdict == "PASS":
        if sharp_drop:
            return "a sharp price drop"
        if heavy_insider_selling or (isinstance(exec_sell_value, (int, float)) and exec_sell_value >= 20000000):
            return "heavy insider selling"
        if rec in ("SELL", "STRONG_SELL"):
            return "analysts turned more negative"
        if chg_dn:
            return "price weakness"
        return "the negative signals now outweigh the positives"
    if sharp_drop:
        return "a sharp move that needs to settle"
    if heavy_insider_selling:
        return "insider selling worth watching"
    return "the signals are now mixed"


def _attach_verdict_change(base, symbol):
    # Per user verdict flip, layered onto the shared cached report without mutating it. This is a
    # pure read: it compares the current verdict to the stored baseline and flags a change, but it
    # does NOT advance the baseline. Acknowledgement happens only when the frontend calls
    # /acknowledge-verdict, so the flip keeps surfacing until the user has actually seen it.
    if not isinstance(base, dict):
        return base
    v = base.get("verdict")
    if not v or v == "ETF":
        return base
    u = current_user()
    if not u:
        return base
    prev = read_verdict(u["id"], symbol)
    if not prev or prev == v:
        return base
    out = dict(base)
    out["verdict_changed"] = True
    out["previous_verdict"] = prev
    out["verdict_reason"] = base.get("verdict_signal_reason") or "the balance of signals shifted"
    return out


# ============ APEX Q ALPHA SCORE ============
# Proprietary composite, 0 to 100, that folds momentum, fundamentals, insider and lawmaker
# flow, sector strength, and news tone into one number.
# Note on the ceiling: by the scoring rules, insider and congressional max at 12 each, so the
# practical top score is about 84. A reading of 70 or higher is therefore genuinely strong.
SECTOR_ETF = {
    "Technology": "XLK",
    "Financial Services": "XLF",
    "Healthcare": "XLV",
    "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP",
    "Energy": "XLE",
    "Industrials": "XLI",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Basic Materials": "XLB",
    "Communication Services": "XLC",
}


def _alpha_num(v):
    """Coerce a value to float, or None if it is not a usable number."""
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _five_day_change(ticker):
    """Five day percent change for a ticker, cached one hour. Returns float or None on failure."""
    ckey = "fdc_" + ticker
    cached = CACHE.get(ckey)
    if cached and (time.time() - cached[1]) < 3600:
        return cached[0]
    try:
        hist = yf.Ticker(ticker).history(period="5d")
        if hist.empty or len(hist) < 2:
            return None
        first = float(hist["Close"].iloc[0])
        last = float(hist["Close"].iloc[-1])
        if first <= 0:
            return None
        chg = round(((last - first) / first) * 100.0, 2)
        CACHE[ckey] = (chg, time.time())
        return chg
    except Exception as e:
        logger.error("_five_day_change %s: %s" % (ticker, e))
        return None


def _analyst_bullishness(rating_distribution, consensus_rating):
    """Convert an analyst rating distribution into a 0-1 bullishness score for the engine.

    Uses the strongBuy/buy/hold/sell/strongSell counts (weighted) when available; falls back to
    the consensus rating word. This is the '31 buy, 2 hold, 0 sell' data the report already pulls.
    """
    rd = rating_distribution
    if isinstance(rd, dict):
        weights = {"strongBuy": 1.0, "buy": 0.75, "hold": 0.5, "sell": 0.25, "strongSell": 0.0}
        total = sum(int(rd.get(k, 0) or 0) for k in weights)
        if total > 0:
            score = sum(int(rd.get(k, 0) or 0) * w for k, w in weights.items()) / float(total)
            return round(score, 3)
    cr = str(consensus_rating or "").lower()
    if "strong buy" in cr:
        return 0.9
    if "buy" in cr:
        return 0.75
    if "hold" in cr:
        return 0.5
    if "sell" in cr:
        return 0.2
    return None


def _target_upside(price, target):
    """Percent upside from current price to mean analyst target, or None."""
    p = _alpha_num(price)
    t = _alpha_num(target)
    if p is None or t is None or p <= 0:
        return None
    return round((t - p) / p * 100.0, 1)


def _recent_rating_dir(recent_actions):
    """+1 if the most recent analyst action was an upgrade, -1 if a downgrade, else 0."""
    if not recent_actions:
        return 0
    for a in recent_actions:
        act = str(a.get("action", "")).lower()
        if "up" in act or "upgrade" in act or "initiat" in act:
            return 1
        if "down" in act or "downgrade" in act:
            return -1
    return 0


def _moat_rationale(sector):
    """One sentence on WHY a company in this sector can hold off competitors. Static by sector,
    per the audit: real business logic beats reciting financial ratios."""
    s = (sector or "").lower()
    table = {
        "healthcare": "Patent protection, complex manufacturing, and clinical trial data create years of exclusivity that rivals cannot quickly replicate.",
        "technology": "Network effects, high switching costs, and entrenched developer ecosystems make it costly for customers to leave.",
        "financial services": "Regulatory licenses, a low-cost deposit base, and switching friction protect incumbents from new entrants.",
        "financial": "Regulatory licenses, a low-cost deposit base, and switching friction protect incumbents from new entrants.",
        "consumer defensive": "Brand loyalty, shelf-space dominance, and distribution scale keep everyday staples hard to dislodge.",
        "consumer cyclical": "Brand strength and scale in sourcing and logistics let leaders undercut smaller rivals on price and reach.",
        "communication services": "Content libraries, subscriber scale, and network effects raise the cost of competing at scale.",
        "industrials": "Long-cycle contracts, engineering know-how, and high capital requirements deter new competitors.",
        "energy": "Control of reserves, pipelines, and refining capacity creates infrastructure advantages that take decades to build.",
        "utilities": "Regulated monopolies and enormous fixed infrastructure make direct competition effectively impossible.",
        "basic materials": "Ownership of low-cost mines and processing assets gives durable cost advantages competitors cannot match.",
        "real estate": "Irreplaceable locations and long-term leases produce recurring income insulated from new supply.",
    }
    return table.get(s, "Durable competitive strengths are present; the specific source is not isolated for this sector.")


def _validate_report_data(symbol, price, chg, market_cap, info, congressional, insider):
    """Data validation guardrails. Quarantines obviously-bad data in place (mutates the lists to
    drop invalid rows) and logs anomalies for review. Never raises to the caller.

    Rules:
      - A one-day move beyond +/-25% is flagged for review (kept, but logged; real halts/catalysts
        do happen, so we log rather than hide the price).
      - Market cap should reconcile to price * shares outstanding within tolerance; if it is wildly
        off, log it (we trust the SEC-derived shares when available).
      - Congressional and insider trade dates cannot be in the future; such rows are dropped.
      - Insider filing date cannot precede the transaction date; such rows are dropped.
    """
    from datetime import datetime as _dt, date as _date
    today = _date.today()

    # Extreme one-day move.
    try:
        if chg is not None and abs(float(chg)) > 25:
            logger.warning("VALIDATION %s: extreme one-day move %.1f%% flagged for review" % (symbol, float(chg)))
    except (ValueError, TypeError):
        pass

    # Market cap reconciliation.
    try:
        shares = info.get("sharesOutstanding")
        if isinstance(market_cap, (int, float)) and shares and price:
            implied = float(price) * float(shares)
            if implied > 0 and abs(implied - market_cap) / max(implied, market_cap) > 0.10:
                logger.warning("VALIDATION %s: market cap %.0f vs implied %.0f (>10%% gap)" % (symbol, market_cap, implied))
    except (ValueError, TypeError, ZeroDivisionError):
        pass

    # Future-dated congressional trades: drop them.
    if isinstance(congressional, list):
        kept = []
        for t in congressional:
            d = t.get("date") or ""
            try:
                if d and _dt.strptime(str(d)[:10], "%Y-%m-%d").date() > today:
                    logger.warning("VALIDATION %s: dropped future-dated congressional trade %s" % (symbol, d))
                    continue
            except (ValueError, TypeError):
                pass
            kept.append(t)
        congressional[:] = kept

    # Future-dated insider trades, and filing-before-transaction: drop them.
    if isinstance(insider, list):
        kept = []
        for t in insider:
            td = t.get("date") or t.get("transaction_date") or ""
            fd = t.get("filing_date") or ""
            bad = False
            try:
                if td and _dt.strptime(str(td)[:10], "%Y-%m-%d").date() > today:
                    bad = True
                if td and fd:
                    _t = _dt.strptime(str(td)[:10], "%Y-%m-%d").date()
                    _f = _dt.strptime(str(fd)[:10], "%Y-%m-%d").date()
                    if _f < _t:
                        bad = True
            except (ValueError, TypeError):
                pass
            if bad:
                logger.warning("VALIDATION %s: dropped invalid insider row (txn %s, filed %s)" % (symbol, td, fd))
                continue
            kept.append(t)
        insider[:] = kept


def _validate_report_data_guard():
    return None


def compute_alpha_v2(sig):
    """Seven-factor transparent Alpha Score and Verdict engine.

    Takes a dict of signals and returns a fully explainable result: a 0-100 score, a verdict
    (APPROVE/WATCH/PASS), and a per-factor breakdown where every factor reports the points it
    earned, the points possible, and a one-sentence reason. Designed to never raise; any missing
    signal degrades gracefully to a documented default rather than failing.

    Expected keys in sig (all optional, safe defaults applied):
      r5, r1m, r3m: 5-day, 1-month, 3-month percent returns
      up_days_5: how many of the last 5 days were up
      chg_today: today's percent change
      pe: price/earnings; profit_margin: net margin (fraction); debt_to_equity
      ins_cbuys, ins_csells: C-level insider buy/sell counts; ins_buy_value, ins_sell_value
      profitable: bool
      cong_buys, cong_sells, cong_committee, cong_recent, cong_size_big, cong_has_data
      analyst_rating (0-1 bullishness), analyst_coverage (int), analyst_upside (pct),
        analyst_recent (+1 upgrade / -1 downgrade), analyst_has_data
      beta; news_sentiment (-1..1), has_catalyst, news_has_data
    """
    factors = []

    def add(name, earned, possible, reason):
        e = max(-possible, min(possible, round(earned)))
        factors.append({"name": name, "earned": int(e), "possible": possible, "reason": reason})
        return int(e)

    n = _alpha_num

    # 1. MOMENTUM QUALITY (15)
    r5, r1m, r3m = n(sig.get("r5")), n(sig.get("r1m")), n(sig.get("r3m"))
    m = 0
    # Score each window; when a window is genuinely missing (None), award a neutral half-credit
    # instead of zero, so a healthy stock is not punished for absent data (e.g. a large-cap served
    # via the fallback path that returns limited history). Zero is for a KNOWN bad return, not unknown.
    def _mom_pts(r):
        if r is None:
            return 2.5  # neutral: unknown, not bad
        return 5 if r > 3 else (3 if r > 0 else 0)
    m += _mom_pts(r5) if r5 is not None else _mom_pts(None)
    m += _mom_pts(r1m)
    m += _mom_pts(r3m)
    if (sig.get("up_days_5") or 0) >= 4:
        m += 2
    ct = n(sig.get("chg_today")) or 0
    if ct > 5 and (r1m or 0) < 0:
        m = min(m, 7)  # mean-reversion penalty: a pop against a down month is suspect
    _miss = [lbl for lbl, v in [("1m", r1m), ("3m", r3m)] if v is None]
    mom_reason = "5d %s, 1m %s, 3m %s.%s" % (
        _pct(r5), _pct(r1m), _pct(r3m),
        (" Some windows lack history; scored neutral, not negative." if _miss else ""))
    mom = add("Momentum Quality", m, 15, mom_reason)

    # 2. FUNDAMENTAL HEALTH (15) with fundamentals floor
    pe = n(sig.get("pe")); pm = n(sig.get("profit_margin")); de = n(sig.get("debt_to_equity"))
    f = 0
    if pe is None:
        f += 3
    elif pe <= 0:
        f += 1
    elif pe < 15:
        f += 7
    elif pe < 25:
        f += 5
    elif pe < 40:
        f += 3
    else:
        f += 1
    if de is None:
        f += 2
    elif de < 0.5:
        f += 4
    elif de < 1.0:
        f += 2
    elif de < 2.0:
        f += 1
    if pm is None:
        f += 1
    elif pm > 0.15:
        f += 4
    elif pm > 0:
        f += 2
    elif pm > -0.1:
        f += 1
    floored = False
    if pe is None and (pm is not None and pm < 0):
        f = min(f, 3)  # fundamentals floor: no earnings and losing money caps this factor
        floored = True
    fund_reason = "PE %s, margin %s, debt/equity %s.%s" % (
        _num(pe), _pctf(pm), _num(de), " Floored: unprofitable with no PE." if floored else "")
    fund = add("Fundamental Health", f, 15, fund_reason)

    # 3. INSIDER CONVICTION (15)
    cb = sig.get("ins_cbuys") or 0; cs = sig.get("ins_csells") or 0
    profitable = bool(sig.get("profitable"))
    ic = 8 if cb >= 3 else (5 if cb == 2 else (3 if cb == 1 else 0))
    ic -= 5 if cs >= 3 else (3 if cs == 2 else (1 if cs == 1 else 0))
    if (sig.get("ins_buy_value") or 0) > 1000000:
        ic += 3
    if (sig.get("ins_sell_value") or 0) > 20000000 and not profitable:
        ic -= 3
    ic = max(0, min(15, ic))
    ins_reason = "%d C-level buy(s), %d sell(s)." % (cb, cs)
    if cb == 0 and cs == 0:
        ins_reason = "No recent C-level insider transactions."
    ins = add("Insider Conviction", ic, 15, ins_reason)

    # 4. CONGRESSIONAL HEAT (15)
    if not sig.get("cong_has_data"):
        cong = add("Congressional Heat", 8, 15, "No congressional trade data; neutral default applied.")
    else:
        cbn = sig.get("cong_buys") or 0; csn = sig.get("cong_sells") or 0
        h = 8 if cbn >= 3 else (5 if cbn == 2 else (3 if cbn == 1 else 0))
        h -= 3 if csn >= 2 else (1 if csn == 1 else 0)
        if sig.get("cong_committee"):
            h += 3
        if sig.get("cong_recent"):
            h += 2
        if sig.get("cong_size_big"):
            h += 2
        h = max(0, min(15, h))
        cong = add("Congressional Heat", h, 15,
                   "%d lawmaker buy(s), %d sell(s)%s." % (cbn, csn,
                   ", relevant committee" if sig.get("cong_committee") else ""))

    # 5. ANALYST CONVICTION (10)
    if not sig.get("analyst_has_data"):
        anal = add("Analyst Conviction", 5, 10, "No analyst coverage data; neutral default applied.")
    else:
        ar = n(sig.get("analyst_rating"))
        a = 5 if (ar or 0) >= 0.75 else (4 if (ar or 0) >= 0.6 else (2 if (ar or 0) >= 0.4 else 0))
        cov = sig.get("analyst_coverage") or 0
        a += 3 if cov >= 20 else (2 if cov >= 10 else (1 if cov >= 3 else 0))
        up = n(sig.get("analyst_upside"))
        a += 2 if (up or 0) > 15 else (1 if (up or 0) > 0 else 0)
        a += 2 if (sig.get("analyst_recent") or 0) > 0 else (-2 if (sig.get("analyst_recent") or 0) < 0 else 0)
        a = max(0, min(10, a))
        anal = add("Analyst Conviction", a, 10,
                   "%d analysts, target upside %s." % (cov, _pct(up)))

    # 6. RISK-ADJUSTED RETURN (10)
    beta = n(sig.get("beta"))
    if r1m is None:
        # Return data missing: score neutral rather than zero so a healthy name is not punished for
        # absent history. Half credit reflects genuine uncertainty, not a bad signal.
        ra = 5
        rar_reason = "1m return not available; scored neutral. Beta %s." % _num(beta)
    elif beta and beta > 0:
        rar_val = r1m / beta
        ra = 10 if rar_val > 5 else (7 if rar_val > 2 else (4 if rar_val > 0 else 0))
        if beta > 2:
            ra = min(ra, 7)
        rar_reason = "1m return %s vs beta %s." % (_pct(r1m), _num(beta))
    else:
        ra = 7 if r1m > 3 else (4 if r1m > 0 else 0)
        rar_reason = "No beta; using raw 1m return %s." % _pct(r1m)
    rar = add("Risk-Adjusted Return", ra, 10, rar_reason)

    # 7. NEWS & FILING SENTIMENT (10)
    if not sig.get("news_has_data"):
        news = add("News & Filing Sentiment", 5, 10, "No recent news; neutral default applied.")
    else:
        ns = n(sig.get("news_sentiment"))
        nv = 10 if (ns or 0) > 0.35 else (7 if (ns or 0) > 0.1 else (5 if (ns or 0) >= -0.1 else (2 if (ns or 0) >= -0.35 else 0)))
        if sig.get("has_catalyst"):
            nv = min(10, nv + 3)
        news = add("News & Filing Sentiment", nv, 10,
                   "Headline sentiment %s%s." % (_num(ns), ", catalyst present" if sig.get("has_catalyst") else ""))

    # 8. ALIGNMENT BONUS (10): reward when many factors are above their midpoint
    core = [(mom, 15), (fund, 15), (ins, 15), (cong, 15), (anal, 10), (rar, 10), (news, 10)]
    above = sum(1 for earned, poss in core if earned >= poss * 0.5)
    align = 10 if above >= 6 else (7 if above >= 4 else (4 if above >= 2 else 0))
    add("Alignment Bonus", align, 10, "%d of 7 factors above midpoint." % above)

    total = sum(fc["earned"] for fc in factors)
    total = max(0, min(100, int(round(total))))

    # Verdict from score, then overrides.
    if total >= 65:
        verdict = "APPROVE"
    elif total >= 40:
        verdict = "WATCH"
    else:
        verdict = "PASS"

    overrides = []
    def cap_watch(reason):
        # Only downgrade, never upgrade past APPROVE.
        nonlocal verdict
        if verdict == "APPROVE":
            verdict = "WATCH"
            overrides.append(reason)

    if fund <= 3:
        cap_watch("Fundamentals floor: weak or missing fundamentals cap this at WATCH.")
    if (sig.get("analyst_recent") or 0) < 0:
        cap_watch("Recent analyst downgrade caps this at WATCH.")
    if cs >= 3 and not profitable:
        cap_watch("Insider cluster selling at an unprofitable company caps this at WATCH.")
    if (n(sig.get("chg_today")) or 0) <= -8:
        cap_watch("Sharp drop today: holding at WATCH until the cause is understood.")
    # Congressional boost: floor at WATCH (never below) when strong lawmaker buying with committee tie.
    if sig.get("cong_has_data") and (sig.get("cong_buys") or 0) >= 3 and sig.get("cong_committee"):
        if verdict == "PASS":
            verdict = "WATCH"
            overrides.append("Congressional support: strong committee-linked buying floors this at WATCH.")

    return {"score": total, "verdict": verdict, "factors": factors, "overrides": overrides}


def compute_verdict_conditions(v2, price, avg_volume, r1m):
    """Generate up to 3 concrete, trackable conditions that would likely flip the verdict.

    Answers the audit: "if you can't name the catalysts, you have a mood, not a position." Uses
    the real score gap to APPROVE (65) or PASS (40), plus actual price and volume levels, so the
    conditions are specific and falsifiable, not vague. Returns a list of strings.
    """
    conditions = []
    try:
        score = v2.get("score", 0)
        verdict = v2.get("verdict")
        factors = {f["name"]: f for f in v2.get("factors", [])}
        p = _alpha_num(price)
        vol = _alpha_num(avg_volume)

        def price_level(pct):
            if p is None:
                return None
            return round(p * (1 + pct / 100.0), 2)

        def vol_str():
            if vol and vol > 0:
                return "on volume above %s (1.5x its average)" % _human_int(vol * 1.5)
            return "on above-average volume"

        if verdict in ("WATCH", "PASS"):
            gap_to_approve = 65 - score
            # Price breakout condition, scaled to how far the score must climb.
            lvl = price_level(max(4, gap_to_approve * 0.4))
            if lvl:
                conditions.append("Likely to move toward APPROVE if price closes above $%s %s." % (_human_num(lvl), vol_str()))
            # Weakest factor naming: what specific signal must improve.
            weak = sorted(v2.get("factors", []), key=lambda f: f["earned"] / max(1, f["possible"]))
            if weak:
                w = weak[0]["name"]
                fixes = {
                    "Insider Conviction": "a C-level executive discloses an open-market purchase",
                    "Congressional Heat": "a committee-relevant lawmaker discloses a new purchase",
                    "Fundamental Health": "the next earnings report shows a return to profitability",
                    "Analyst Conviction": "an analyst issues an upgrade or raises the price target",
                    "News & Filing Sentiment": "a positive catalyst (approval, contract, or clearance) is filed",
                    "Momentum Quality": "the stock strings together a multi-week uptrend",
                    "Risk-Adjusted Return": "returns improve without a jump in volatility",
                }
                if w in fixes:
                    conditions.append("The weakest factor is %s; it would strengthen if %s." % (w, fixes[w]))

        if verdict in ("APPROVE", "WATCH"):
            gap_to_pass = score - 40
            lvl = price_level(-max(5, gap_to_pass * 0.4))
            if lvl:
                conditions.append("Likely to weaken toward PASS if price breaks below $%s %s." % (_human_num(lvl), vol_str()))

        # Always give a concrete downside tripwire tied to insiders when relevant.
        if verdict == "APPROVE":
            conditions.append("A cluster of C-level insider selling, or a sharp single-day drop, would cap this at WATCH.")
    except Exception:
        pass
    return conditions[:3]


def _human_int(v):
    v = _alpha_num(v)
    if v is None:
        return "n/a"
    if v >= 1000000:
        return "%.1fM" % (v / 1000000.0)
    if v >= 1000:
        return "%.0fK" % (v / 1000.0)
    return str(int(v))


def _human_num(v):
    v = _alpha_num(v)
    if v is None:
        return "n/a"
    return ("%.2f" % v).rstrip("0").rstrip(".") if v < 100 else "{:,.0f}".format(v)


def compute_verdict_conditions_unused_guard():
    return None


def _pct(v):
    v = _alpha_num(v)
    if v is None:
        return "n/a"
    return ("+%.1f%%" % v) if v >= 0 else ("%.1f%%" % v)


def _pctf(v):
    v = _alpha_num(v)
    return "n/a" if v is None else ("%.1f%%" % (v * 100))


def _num(v):
    v = _alpha_num(v)
    return "n/a" if v is None else ("%.2f" % v)



# =========================================================================== #
# TECHNICAL MOMENTUM v2
#
# The old factor scored 20 points -- a fifth of the whole Alpha Score -- off
# `eff_chg`, today's single-day percent move. A stock down 2% at 11am on nothing
# lost a fifth of its score. That is noise, not momentum.
#
# Worse, it was pointed the wrong way. The momentum literature (Jegadeesh &
# Titman, and the thirty years since) consistently finds SHORT-term price moves
# mean-REVERT, while INTERMEDIATE-term moves -- roughly 3 to 12 months --
# persist. Rewarding a stock for being up today is closer to a contrarian
# signal than a momentum one.
#
# So v2 measures what actually persists:
#   - 60-day return          .... intermediate-term momentum, the real effect
#   - 20-day relative strength vs SPY .... is it beating the market, or just rising with it
#   - price vs 50-day SMA    .... trend confirmation
#   - price vs 200-day SMA   .... regime filter
#
# Today's move is deliberately worth nothing. It is in the breakdown text for
# the user to see, and it does not touch the score.
#
# When history is unavailable it returns None rather than a middling default,
# so the caller can say "unavailable" instead of quietly inventing 10/20.
# =========================================================================== #

def _mom_closes(symbol, period="1y"):
    """Daily closes ascending, cached 1h. Uses the existing fallback chain, so this keeps working
    when Yahoo is down -- the same reason the rest of the report survives."""
    ckey = "mom_" + str(symbol) + "_" + period
    cached = CACHE.get(ckey)
    if cached and (time.time() - cached[1]) < 3600:
        return cached[0]
    closes = []
    try:
        rows = fetch_with_fallback(symbol, period=period, interval="1d")
        for r in (rows or []):
            c = _alpha_num(r.get("close"))
            if c and c > 0:
                closes.append(float(c))
    except Exception as e:
        logger.warning("_mom_closes %s: %s" % (symbol, e))
    CACHE[ckey] = (closes, time.time())
    return closes


def _pct_change_over(closes, n):
    """Percent change across the last n bars. None when there is not enough history."""
    if not closes or len(closes) < n + 1:
        return None
    old = closes[-(n + 1)]
    new = closes[-1]
    if not old or old <= 0:
        return None
    return (new - old) / old * 100.0


def _sma_of(closes, n):
    if not closes or len(closes) < n:
        return None
    window = closes[-n:]
    return sum(window) / float(n)


def compute_momentum(symbol):
    """Technical Momentum, 0-20. Returns {'points', 'note', 'detail'} or None if no history.

    Point budget:
      60-day return .................. 7
      20-day relative strength vs SPY  7
      above 50-day SMA ............... 4
      above 200-day SMA .............. 2
    Today's move: 0. Reported, never scored.
    """
    closes = _mom_closes(symbol)
    if len(closes) < 25:
        return None

    pts = 0
    detail = {}
    bits = []

    # 1. Intermediate-term momentum (60 trading days ~ 3 months). The effect that actually persists.
    r60 = _pct_change_over(closes, 60)
    detail["ret_60d"] = round(r60, 2) if r60 is not None else None
    if r60 is None:
        # Not enough history for the real window; fall back to what we have and say so.
        r20_only = _pct_change_over(closes, 20)
        if r20_only is not None:
            pts += 4 if r20_only > 0 else 1
            bits.append("only %d days of history, using 20-day trend" % len(closes))
    else:
        if r60 >= 20:
            pts += 7
        elif r60 >= 10:
            pts += 6
        elif r60 >= 3:
            pts += 5
        elif r60 > -3:
            pts += 3
        elif r60 > -12:
            pts += 1
        else:
            pts += 0
        bits.append("%s %.1f%% over 3 months" % ("up" if r60 >= 0 else "down", abs(r60)))

    # 2. Relative strength. Beating the market is skill; rising with it is beta. This is the factor
    #    that separates the two, and it is the one most retail scores leave out.
    r20 = _pct_change_over(closes, 20)
    spy = _mom_closes("SPY")
    b20 = _pct_change_over(spy, 20) if spy else None
    rs = None
    if r20 is not None and b20 is not None:
        rs = r20 - b20
        detail["rel_strength_20d"] = round(rs, 2)
        if rs >= 8:
            pts += 7
        elif rs >= 3:
            pts += 6
        elif rs >= 0:
            pts += 4
        elif rs > -3:
            pts += 3
        elif rs > -8:
            pts += 1
        else:
            pts += 0
        bits.append("%s the market by %.1f%% over 20 days"
                    % ("beating" if rs >= 0 else "lagging", abs(rs)))
    detail["ret_20d"] = round(r20, 2) if r20 is not None else None

    # 3 & 4. Trend and regime. Cheap, robust, and they keep the factor from rewarding a falling knife
    #        that happens to have bounced.
    last = closes[-1]
    s50 = _sma_of(closes, 50)
    s200 = _sma_of(closes, 200)
    if s50:
        detail["above_50d_sma"] = bool(last > s50)
        if last > s50:
            pts += 4
            bits.append("above its 50-day average")
        else:
            bits.append("below its 50-day average")
    if s200:
        detail["above_200d_sma"] = bool(last > s200)
        if last > s200:
            pts += 2
        else:
            bits.append("below its 200-day average")

    pts = max(0, min(20, pts))
    note = "; ".join(bits) if bits else "limited price history"
    return {"points": pts, "note": note, "detail": detail}


def compute_alpha_score(eff_chg, pe, debt_to_equity, ins_buys, ins_sells, cong_buys, cong_sells, news, sector, symbol=None):
    """Return {'score': int 0..100, 'breakdown': [str, ...]}. Designed to never raise."""
    breakdown = []

    # a. Technical Momentum (0 to 20) -- multi-window, see compute_momentum above.
    # Today's move is shown for context but is NOT scored: short-horizon moves mean-revert, so
    # rewarding them was actively counterproductive. Intermediate-term trend and relative strength
    # are what carry signal.
    ec = _alpha_num(eff_chg)
    if ec is None:
        ec = 0.0
    if ec > 0:
        move = "up %s%% today" % ec
    elif ec < 0:
        move = "down %s%% today" % abs(ec)
    else:
        move = "flat today"

    mom = None
    if symbol:
        try:
            mom = compute_momentum(symbol)
        except Exception as _me:
            logger.warning("compute_momentum %s: %s" % (symbol, _me))
            mom = None

    if mom:
        tech = mom["points"]
        breakdown.append("Technical Momentum: +%d pts (%s; %s)" % (tech, mom["note"], move))
    else:
        # No usable price history. Say so rather than inventing a middling default -- an absent
        # signal must not quietly prop the score up.
        tech = 0
        breakdown.append("Technical Momentum: +0 pts (not enough price history to judge trend; %s)" % move)

    # b. Fundamental Health (0 to 20): PE up to 10, debt to equity up to 10
    pe_num = _alpha_num(pe)
    if pe_num is None:
        pe_pts, pe_note = 4, "PE not available"
    elif pe_num <= 0:
        pe_pts, pe_note = 2, "negative earnings"
    elif pe_num < 15:
        pe_pts, pe_note = 10, "PE %.1f, inexpensive" % pe_num
    elif pe_num < 25:
        pe_pts, pe_note = 7, "PE %.1f, fair" % pe_num
    elif pe_num < 40:
        pe_pts, pe_note = 4, "PE %.1f, rich" % pe_num
    else:
        pe_pts, pe_note = 2, "PE %.1f, very rich" % pe_num
    de_num = _alpha_num(debt_to_equity)
    if de_num is None:
        de_pts, de_note = 4, "debt to equity not available"
    elif de_num < 0:
        de_pts, de_note = 2, "negative equity"
    elif de_num < 0.5:
        de_pts, de_note = 10, "low debt"
    elif de_num < 1.0:
        de_pts, de_note = 7, "moderate debt"
    elif de_num < 2.0:
        de_pts, de_note = 4, "elevated debt"
    else:
        de_pts, de_note = 2, "high debt"
    fund = pe_pts + de_pts
    breakdown.append("Fundamental Health: +%d pts (%s, %s)" % (fund, pe_note, de_note))

    # c. Insider Sentiment (0 to 20)
    ib = ins_buys or 0
    isl = ins_sells or 0
    if ib >= 2:
        ins = 12
    elif ib == 1:
        ins = 6
    else:
        ins = 0
    if isl >= 3:
        ins -= 6
    elif isl >= 1:
        ins -= 3
    ins = max(0, min(20, ins))
    if ib >= 1 and isl == 0:
        ins_note = "%d executive buy(s)" % ib
    elif ib >= 1 and isl >= 1:
        ins_note = "%d buy(s) against %d sell(s) by executives" % (ib, isl)
    elif isl >= 1:
        ins_note = "%d executive sell(s), no buys" % isl
    else:
        ins_note = "no notable executive trades"
    breakdown.append("Insider Sentiment: +%d pts (%s)" % (ins, ins_note))

    # d. Congressional Heat (0 to 20)
    cb = cong_buys or 0
    cs = cong_sells or 0
    if cb >= 2:
        cong = 12
    elif cb == 1:
        cong = 6
    else:
        cong = 0
    if cs >= 2:
        cong -= 6
    elif cs == 1:
        cong -= 3
    cong = max(0, min(20, cong))
    if cb >= 1 and cs == 0:
        cong_note = "%d lawmaker buy(s)" % cb
    elif cb >= 1 and cs >= 1:
        cong_note = "%d buy(s) against %d sell(s) by lawmakers" % (cb, cs)
    elif cs >= 1:
        cong_note = "%d lawmaker sell(s), no buys" % cs
    else:
        cong_note = "no recent lawmaker trades"
    breakdown.append("Congressional Heat: +%d pts (%s)" % (cong, cong_note))

    # e. Sector Tailwinds (0 to 10)
    etf = SECTOR_ETF.get(sector or "")
    if not etf:
        sector_pts, sector_note = 5, "sector not mapped, neutral"
    else:
        etf_chg = _five_day_change(etf)
        spx_chg = _five_day_change("^GSPC")
        if etf_chg is None or spx_chg is None:
            sector_pts, sector_note = 5, "%s data unavailable" % etf
        elif etf_chg > spx_chg:
            sector_pts, sector_note = 10, "%s up %s%% over 5 days, leading the market" % (etf, etf_chg)
        elif etf_chg > 0:
            sector_pts, sector_note = 5, "%s up %s%% over 5 days, trailing the market" % (etf, etf_chg)
        else:
            sector_pts, sector_note = 0, "%s down over 5 days" % etf
    breakdown.append("Sector Tailwinds: +%d pts (%s)" % (sector_pts, sector_note))

    # f. News Sentiment (0 to 10)
    pos_words = ["buy", "beat", "upgrade", "positive"]
    neg_words = ["sell", "miss", "downgrade", "negative"]
    pos = neg = 0
    for n in (news or []):
        head = str(n.get("headline", "")).lower()
        for w in pos_words:
            pos += len(re.findall(r"\b" + w + r"(s|es|ed|ing)?\b", head))
        for w in neg_words:
            neg += len(re.findall(r"\b" + w + r"(s|es|ed|ing)?\b", head))
    total_words = pos + neg
    if not news:
        news_pts, news_note = 5, "no recent headlines"
    elif total_words == 0:
        news_pts, news_note = 5, "headlines are neutral"
    else:
        ratio = pos / float(total_words)
        if ratio > 0.6:
            news_pts, news_note = 10, "headlines lean positive"
        elif ratio >= 0.4:
            news_pts, news_note = 5, "headlines are mixed"
        else:
            news_pts, news_note = 0, "headlines lean negative"
    breakdown.append("News Sentiment: +%d pts (%s)" % (news_pts, news_note))

    total = tech + fund + ins + cong + sector_pts + news_pts
    total = max(0, min(100, int(round(total))))
    return {"score": total, "breakdown": breakdown}
# ============ END APEX Q ALPHA SCORE ============


# ---------- Committee weight and cluster trading ----------
# Curated static map of the most active congressional traders to their committee assignments,
# from public records. When a lawmaker on a committee tied to a stock's sector trades that stock,
# it earns a High Clout flag: the person sits closer to the information than an average member.
COMMITTEE_ASSIGNMENTS = {
    "Nancy Pelosi": ["Intelligence", "Technology"],
    "Dan Crenshaw": ["Energy and Commerce", "Budget"],
    "Ro Khanna": ["Armed Services", "Oversight", "Technology"],
    "Michael McCaul": ["Foreign Affairs", "Homeland Security"],
    "Josh Gottheimer": ["Financial Services", "Intelligence"],
    "Marjorie Taylor Greene": ["Homeland Security", "Oversight"],
    "Tommy Tuberville": ["Armed Services", "Agriculture", "Health"],
    "Sheldon Whitehouse": ["Budget", "Environment", "Finance"],
    "Ron Wyden": ["Finance", "Budget", "Energy"],
    "Mark Green": ["Homeland Security", "Armed Services"],
    "Virginia Foxx": ["Education", "Oversight"],
    "Kathy Manning": ["Foreign Affairs", "Education"],
    "Earl Blumenauer": ["Ways and Means", "Budget"],
    "John Boozman": ["Agriculture", "Appropriations", "Environment"],
    "Shelley Moore Capito": ["Environment", "Appropriations", "Commerce"],
    "Thomas Carper": ["Environment", "Finance"],
    "Susan Collins": ["Appropriations", "Health", "Intelligence"],
    "Patrick Fallon": ["Armed Services", "Oversight"],
    "Bill Hagerty": ["Foreign Relations", "Banking", "Appropriations"],
    "Debbie Wasserman Schultz": ["Appropriations", "Oversight"],
    "Gilbert Cisneros": ["Armed Services", "Veterans"],
    "Katherine Clark": ["Appropriations"],
    "Lois Frankel": ["Appropriations", "Foreign Affairs"],
    "Garret Graves": ["Transportation", "Natural Resources"],
    "David Rouzer": ["Agriculture", "Transportation"],
}

# Which committees plausibly grant an information edge in which sector.
SECTOR_COMMITTEE_MAP = {
    "Technology": ["Technology", "Intelligence", "Commerce"],
    "Energy": ["Energy", "Energy and Commerce", "Environment", "Natural Resources"],
    "Healthcare": ["Health", "Education"],
    "Financial Services": ["Financial Services", "Finance", "Banking", "Ways and Means"],
    "Financials": ["Financial Services", "Finance", "Banking", "Ways and Means"],
    "Industrials": ["Armed Services", "Transportation", "Homeland Security"],
    "Defense": ["Armed Services", "Homeland Security", "Foreign Affairs", "Foreign Relations"],
    "Consumer Defensive": ["Agriculture"],
    "Consumer Staples": ["Agriculture"],
    "Basic Materials": ["Natural Resources", "Environment"],
    "Utilities": ["Energy", "Environment"],
    "Real Estate": ["Financial Services", "Banking"],
    "Communication Services": ["Commerce", "Technology"],
}


def _committees_for(politician):
    if not politician:
        return []
    p = politician.strip()
    if p in COMMITTEE_ASSIGNMENTS:
        return COMMITTEE_ASSIGNMENTS[p]
    for name, coms in COMMITTEE_ASSIGNMENTS.items():
        if name.lower() in p.lower() or p.lower() in name.lower():
            return coms
    return []


def is_high_clout(politician, sector):
    """True when the trader sits on a committee tied to the stock's sector."""
    if not sector:
        return False
    coms = _committees_for(politician)
    if not coms:
        return False
    relevant = SECTOR_COMMITTEE_MAP.get(sector, [])
    for c in coms:
        for rc in relevant:
            if rc.lower() in c.lower() or c.lower() in rc.lower():
                return True
    return False


def _is_operating_exec(title):
    """True only for operating C-suite who run the company day to day: CEO, CFO, COO, CTO,
    President, Founder. Directors and 10% holders are deliberately excluded here. A board
    director buying is a real but weaker signal than an operating executive buying, so only
    operating execs count toward the C-Suite Accumulation headline."""
    import re as _re
    t = str(title or "").upper()
    if "DIRECTOR" in t and not any(k in t for k in ["CHIEF", "PRESIDENT", "FOUNDER"]):
        # Pure director (or "Officer and Director" leans director unless a chief title present).
        if "OFFICER" not in t or "CHIEF" not in t:
            pass  # fall through to the positive checks below; only counts if a chief title exists
    if "CHIEF" in t and "OFFICER" in t:
        return True
    if _re.search(r"\bCEO\b|\bCFO\b|\bCOO\b|\bCTO\b", t):
        return True
    if "PRESIDENT" in t and "VICE" not in t:
        return True
    if "FOUNDER" in t:
        return True
    return False


def detect_insider_cluster(ticker, company_name, insider_list):
    """Corporate Cluster Buy engine. Groups open market executive buys in a rolling 14 day window.
    When three or more distinct executives buy the same stock, the cluster becomes a high signal
    event. Returns the active cluster dict (3+ insiders) for the report, or None. Idempotent per
    name so re-analyzing the same stock does not double count, and only escalating severity
    re-alerts. All failures are logged and swallowed, never surfaced to the report."""
    conn = get_db()
    if conn is None:
        return None
    try:
        import datetime as _dt
        today = _dt.date.today()
        cutoff = today - _dt.timedelta(days=14)
        # Qualifying buys: real open market executive purchases only.
        buys = []
        for t in (insider_list or []):
            # Recognize a buy from either data source: SEC sets action "Purchase"/kind "buy",
            # the older yfinance path set action "A". Only open market buys by operating executives
            # count toward the cluster. Directors are tracked separately as a lighter signal.
            _is_buy = t.get("kind") == "buy" and t.get("action") in ("A", "Purchase", "Buy")
            if _is_buy and _is_operating_exec(t.get("title")):
                buys.append(t)
        cur = conn.cursor()
        # Auto resolve any stale cluster for this ticker whose window has fully aged out.
        cur.execute("UPDATE insider_clusters SET is_resolved=true WHERE ticker=%s AND is_resolved=false "
                    "AND window_end_date < %s", (ticker, cutoff))
        conn.commit()
        if not buys:
            cur.execute("SELECT unique_insiders_count, total_capital_deployed, executive_roles, "
                        "window_start_date FROM insider_clusters WHERE ticker=%s AND is_resolved=false "
                        "AND unique_insiders_count >= 3 ORDER BY created_at DESC LIMIT 1", (ticker,))
            row = cur.fetchone()
            cur.close(); conn.close()
            if row:
                return {"unique_insiders_count": row[0], "total_capital_deployed": float(row[1] or 0),
                        "executive_roles": row[2] or [], "window_start_date": row[3].isoformat() if row[3] else None}
            return None
        # Find or open the active cluster for this ticker.
        cur.execute("SELECT id, unique_insiders_count, total_capital_deployed, executive_roles, "
                    "insider_names, last_alert_count FROM insider_clusters WHERE ticker=%s "
                    "AND is_resolved=false AND window_end_date >= %s ORDER BY created_at DESC LIMIT 1",
                    (ticker, cutoff))
        existing = cur.fetchone()
        if existing:
            cid, count, capital, roles, names, last_alert = existing
            roles = roles or []
            names = names or []
            capital = float(capital or 0)
        else:
            cid = None
            count, capital, roles, names, last_alert = 0, 0.0, [], [], 0
        # Fold in any buyer not already counted (dedupe by name).
        changed = False
        for b in buys:
            nm = (b.get("name") or "").strip()
            if not nm or nm in names:
                continue
            names.append(nm)
            roles.append(_short_role(b.get("title")))
            # Capital is approximate: shares times a price if present, else shares as a proxy count.
            px = b.get("price") or 0
            capital += (b.get("shares") or 0) * (px if px else 0)
            changed = True
        unique_count = len(names)
        if cid is None:
            cur.execute("INSERT INTO insider_clusters (ticker, company_name, unique_insiders_count, "
                        "total_capital_deployed, executive_roles, insider_names, window_start_date, "
                        "window_end_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                        (ticker, company_name, unique_count, capital, roles, names, today, today))
            cid = cur.fetchone()[0]
        elif changed:
            cur.execute("UPDATE insider_clusters SET unique_insiders_count=%s, total_capital_deployed=%s, "
                        "executive_roles=%s, insider_names=%s, window_end_date=%s WHERE id=%s",
                        (unique_count, capital, roles, names, today, cid))
        conn.commit()
        # High signal: 3+ insiders, alert only on first crossing or on severity escalation.
        active = None
        if unique_count >= 3:
            active = {"unique_insiders_count": unique_count, "total_capital_deployed": capital,
                      "executive_roles": roles, "window_start_date": str(today)}
            escalated = (last_alert < 3 and unique_count >= 3) or (last_alert < 5 and unique_count >= 5)
            if escalated:
                sev = "CRITICAL" if unique_count >= 5 else "HIGH"
                msg = "C Suite accumulation: %d executives bought %s in 14 days" % (unique_count, ticker)
                reason = "%d distinct executives have made open market purchases of %s within a 14 day window. %s signal." % (unique_count, ticker, sev)
                _cluster_alert_watchlist(conn, ticker, msg, reason)
                cur.execute("UPDATE insider_clusters SET last_alert_count=%s WHERE id=%s", (unique_count, cid))
                conn.commit()
        cur.close(); conn.close()
        return active
    except Exception as e:
        logger.error("detect_insider_cluster %s: %s" % (ticker, e))
        try:
            conn.close()
        except Exception:
            pass
        return None


def _short_role(title):
    import re as _re
    t = str(title or "").upper()
    # Word-boundary matching. The old substring check mislabeled DIRECTOR as CTO, because
    # "DIRECTOR" literally contains the letters C-T-O. Check the full titles first, then use
    # word boundaries for the acronyms so a substring inside another word never matches.
    if "CHIEF EXECUTIVE" in t or _re.search(r"\bCEO\b", t): return "CEO"
    if "CHIEF FINANCIAL" in t or _re.search(r"\bCFO\b", t): return "CFO"
    if "CHIEF OPERATING" in t or _re.search(r"\bCOO\b", t): return "COO"
    if "CHIEF TECHNOLOGY" in t or "CHIEF TECHNICAL" in t or _re.search(r"\bCTO\b", t): return "CTO"
    if "CHIEF" in t and "OFFICER" in t: return "Chief Officer"
    if "DIRECTOR" in t: return "Director"
    for r in ["PRESIDENT", "CHAIRMAN", "CHAIR", "FOUNDER", "EVP", "SVP", "VICE PRESIDENT", "OFFICER"]:
        if r in t:
            return r.title() if len(r) > 3 else r
    return (str(title)[:20] if title else "Executive")


def _cluster_alert_watchlist(conn, ticker, message, reason):
    """Log a cluster alert and push to every user watching this ticker."""
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT w.user_id, u.email, COALESCE(u.onesignal_token,'') "
                    "FROM watchlist w JOIN users u ON u.id = w.user_id WHERE UPPER(w.symbol)=%s",
                    (ticker.upper(),))
        watchers = cur.fetchall()
        for uid, email, token in watchers:
            cur.execute("SELECT 1 FROM alert_log WHERE user_id=%s AND alert_type='insider_cluster' "
                        "AND symbol=%s AND triggered_at > NOW() - INTERVAL '24 hours' AND message=%s LIMIT 1",
                        (uid, ticker, message))
            if cur.fetchone():
                continue
            cur.execute("INSERT INTO alert_log (user_id, alert_type, symbol, message, reason, link) "
                        "VALUES (%s,'insider_cluster',%s,%s,%s,%s)",
                        (uid, ticker, message, reason, "/?symbol=" + ticker))
            if token:
                _onesignal_push(token, message, reason, "https://www.apexq.io/?symbol=" + ticker)
            if email:
                cur.execute("INSERT INTO email_queue (user_id, subject, body) VALUES (%s,%s,%s)",
                            (uid, "Apex Q Cluster Alert: " + ticker,
                             message + "\n\n" + reason + "\n\nEducational research framework only. Not personalized financial advice."))
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error("cluster alert watchlist: %s" % e)


def detect_insider_cluster_safe(*a, **k):
    return detect_insider_cluster(*a, **k)


def compute_full_report(symbol):
    symbol = _china_symbol(symbol)
    cached = get_cache(f"full_{symbol}")
    if cached:
        return cached

    try:
        ticker = yf.Ticker(symbol)
        try:
            hist = ticker.history(period="5d", timeout=15)
        except Exception as _he:
            logger.warning("yfinance history failed for %s: %s" % (symbol, _he))
            hist = None
        try:
            info = ticker.info or {}
        except Exception:
            info = {}

        # Yahoo blocks cloud IPs and breaks periodically. When its history is empty, do NOT give up:
        # rebuild a minimal history from the Finnhub/FMP fallback chain and pull fundamentals from FMP,
        # so the report still works on the data sources we already pay for. This is what keeps the app
        # alive when Yahoo is down. All downstream scoring reads `hist` and `info` unchanged.
        if hist is None or (hasattr(hist, "empty") and hist.empty):
            rows = fetch_with_fallback(symbol, period="6mo", interval="1d")
            if not rows:
                # Last resort: all history sources failed (Yahoo blocked, Finnhub candle endpoint
                # deprecated, FMP history down or over quota). A single current quote is the most
                # reliable call there is, so build a minimal one-bar history from FMP's quote so the
                # engine still runs instead of returning NOT FOUND on a major, liquid stock.
                q = fmp_get("/api/v3/quote/%s" % symbol)
                if isinstance(q, list) and q:
                    qd = q[0]
                    px = qd.get("price")
                    if px:
                        prev = qd.get("previousClose") or px
                        rows = [
                            {"time": int(time.time()) - 86400, "open": prev, "high": prev, "low": prev, "close": prev, "volume": qd.get("volume", 0) or 0},
                            {"time": int(time.time()), "open": qd.get("open", px) or px, "high": qd.get("dayHigh", px) or px, "low": qd.get("dayLow", px) or px, "close": px, "volume": qd.get("volume", 0) or 0},
                        ]
                        if not info:
                            info = {}
                        info.setdefault("longName", qd.get("name"))
                        info.setdefault("regularMarketPrice", px)
                        info.setdefault("marketCap", qd.get("marketCap"))
                        info.setdefault("trailingPE", qd.get("pe"))
                        logger.warning("analyze: served %s from FMP quote (all history sources down)" % symbol)
                if not rows:
                    logger.error("analyze: ALL sources failed for %s (yahoo+finnhub+fmp history+fmp quote)" % symbol)
                    return None
            import pandas as _pd
            _df = _pd.DataFrame([{
                "Open": r.get("open"), "High": r.get("high"), "Low": r.get("low"),
                "Close": r.get("close"), "Volume": r.get("volume", 0)
            } for r in rows])
            try:
                _df.index = _pd.to_datetime([r.get("time") or r.get("date") for r in rows], unit="s", errors="coerce")
            except Exception:
                try:
                    _df.index = _pd.to_datetime([r.get("time") or r.get("date") for r in rows])
                except Exception:
                    pass
            hist = _df
            if not info:
                info = {}
            try:
                prof = fmp_get("/api/v3/profile/%s" % symbol)
                if isinstance(prof, list) and prof:
                    p = prof[0]
                    info.setdefault("longName", p.get("companyName"))
                    info.setdefault("sector", p.get("sector"))
                    info.setdefault("industry", p.get("industry"))
                    info.setdefault("longBusinessSummary", p.get("description"))
                    info.setdefault("beta", p.get("beta"))
                    if p.get("price"):
                        info.setdefault("regularMarketPrice", p.get("price"))
            except Exception as _pe:
                logger.warning("FMP profile fallback failed for %s: %s" % (symbol, _pe))
            logger.warning("analyze: served %s via fallback (yahoo unavailable)" % symbol)

        if hist is None or (hasattr(hist, "empty") and hist.empty):
            return None

        # BACKEND UPGRADE: Finnhub real-time price fallback.
        # Tries Finnhub for a live quote first; falls back to yfinance delayed data.
        # All calculations, scoring, and caching below are unchanged.
        fq = fetch_finnhub_quote(symbol)
        if fq and fq.get("price"):
            cur = fq["price"]
            prev = fq["prev_close"]
            chg = fq["change_pct"]
            price_source = "finnhub"
        else:
            cur = fmt_price(hist["Close"].iloc[-1])
            prev = fmt_price(hist["Close"].iloc[-2]) if len(hist) > 1 else cur
            chg = round(((cur - prev) / prev) * 100, 2)
            price_source = "yfinance"
        pe_raw = info.get("trailingPE")
        pe = round(float(pe_raw), 2) if pe_raw else "N/A"
        tgt_raw = info.get("targetMeanPrice")
        tgt = round(float(tgt_raw), 2) if tgt_raw else "N/A"
        rec = info.get("recommendationKey", "hold").upper()
        # CHUNK: know early if earnings just landed, so a likely stale target gets reduced weight below.
        earn = earnings_flag(info)

        # CHUNK: ETF branch. A fund is judged on cost and holdings, not the stock engine, so build a
        # tailored report and return before any stock scoring runs. The stock path below is untouched.
        if str(info.get("quoteType", "")).upper() == "ETF":
            etf_result = build_etf_report(symbol, ticker, info, hist, cur, chg)
            set_cache(f"full_{symbol}", etf_result)
            return etf_result

        score = 0
        ext = extended_hours(info, cur)
        # Pre and post market moves are real and often the freshest signal, so scoring reads the
        # effective extended price and a change recalculated from the prior close. The displayed
        # price stays the regular session close.
        eff_px = ext["price"] if ext else cur
        eff_chg = round(((eff_px - prev) / prev) * 100, 2) if (ext and prev) else chg
        sharp_drop = eff_chg <= -8

        if eff_chg > 2:
            score += 2
        elif eff_chg > 0:
            score += 1
        elif eff_chg <= -10:
            score -= 5
        elif eff_chg <= -5:
            score -= 3
        elif eff_chg <= -3:
            score -= 2
        else:
            score -= 1

        if rec in ["BUY", "STRONG_BUY"]:
            score += 2
        elif rec in ["SELL", "STRONG_SELL"]:
            score -= 2

        # On a sharp single-day drop the analyst target is almost certainly stale, set before
        # the news broke. The huge upside it implies is an illusion created by the falling price,
        # so it should not add to the score.
        if tgt and eff_px and not sharp_drop:
            try:
                up = ((float(tgt) - eff_px) / eff_px) * 100
                # CHUNK: a target right after earnings or far out of line is likely stale, so a big
                # upside carries reduced weight, not the full bonus. It still counts, just less.
                target_stale = (earn == "recent") or (up >= 40)
                if up > 10:
                    score += 1 if target_stale else 2
                elif up > 0:
                    score += 1
                elif up < -5:
                    score -= 1
            except:
                pass

        # Congressional from Quiver
        congressional = []
        _sector_for_cong = info.get("sector", "")
        if QUIVER_KEY:
            try:
                url = f"https://api.quiverquant.com/beta/historical/congresstrading/{symbol}"
                h = {"Authorization": f"Token {QUIVER_KEY}", "Accept": "application/json"}
                r = requests.get(url, headers=h, timeout=8)
                if r.status_code == 200:
                    for t in r.json()[:8]:
                        pol = t.get("Representative", "Unknown")
                        tdate = t.get("TransactionDate", "") or ""
                        rdate = t.get("ReportDate", "") or t.get("Reported", "") or ""
                        coms = _committees_for(pol)
                        relevant = is_high_clout(pol, _sector_for_cong)
                        # Filing lag: how many days between trading and disclosing. The audit called
                        # this out; a long lag means the public learned late.
                        lag = None
                        try:
                            if tdate and rdate:
                                _d1 = datetime.strptime(tdate[:10], "%Y-%m-%d")
                                _d2 = datetime.strptime(rdate[:10], "%Y-%m-%d")
                                lag = (_d2 - _d1).days
                        except Exception:
                            lag = None
                        if not _valid_trade_date(tdate, rdate):
                            continue  # quarantine impossible dates (future-dated or filed before trade)
                        congressional.append({
                            "politician": pol, "party": t.get("Party", ""),
                            "action": t.get("Transaction", "Unknown"),
                            "amount": t.get("Range", ""), "date": tdate,
                            "report_date": rdate, "filing_lag_days": lag,
                            "committees": coms[:3], "committee_relevant": relevant,
                        })
            except Exception as e:
                logger.error(f"Congressional error: {e}")

        # Merge in our OWN Senate eFD data alongside Quiver. This is the migration step: our data
        # adds to Quiver's coverage rather than replacing it, so the app only gets better. We prefer
        # our own record when the same trade appears in both (deduped by politician+date+action), and
        # tag the source so we can watch our coverage grow toward eventually cutting Quiver.
        try:
            _sconn = get_db()
            if _sconn is not None:
                _scur = _sconn.cursor()
                _scur.execute(
                    "SELECT politician_name, party, state, transaction_type, amount, trade_date, filing_date "
                    "FROM congressional_trades_senate WHERE ticker = %s AND ticker IS NOT NULL "
                    "AND transaction_type IS NOT NULL ORDER BY trade_date DESC NULLS LAST LIMIT 20",
                    (symbol.upper(),))
                _seen = set()
                for _c in congressional:
                    _seen.add((str(_c.get("politician", "")).strip().lower(), str(_c.get("date", ""))[:10],
                               "purchase" if "purchase" in str(_c.get("action", "")).lower() else "sale"))
                for _r in _scur.fetchall():
                    _pol, _party, _state, _ttype, _amt, _td, _fd = _r
                    _act = "Purchase" if _ttype == "buy" else ("Sale" if _ttype == "sell" else (_ttype or "Unknown").title())
                    _tds = _td.isoformat() if _td else ""
                    _sig = (str(_pol or "").strip().lower(), str(_tds)[:10],
                            "purchase" if _ttype == "buy" else "sale")
                    if _sig in _seen:
                        continue  # already have it from Quiver; do not double count
                    if not _valid_trade_date(_tds, _fd):
                        continue  # quarantine impossible dates
                    _seen.add(_sig)
                    _coms = _committees_for(_pol)
                    _lag = None
                    try:
                        if _td and _fd:
                            _lag = (_fd - _td).days
                    except Exception:
                        _lag = None
                    congressional.append({
                        "politician": _pol, "party": _party if _party and _party != "Unknown" else "",
                        "state": _state if _state and _state != "Unknown" else "",
                        "action": _act, "amount": _amt or "", "date": _tds,
                        "report_date": _fd.isoformat() if _fd else "", "filing_lag_days": _lag,
                        "committees": _coms[:3], "committee_relevant": is_high_clout(_pol, _sector_for_cong),
                        "source": "apexq_senate",
                    })
                _scur.close(); _sconn.close()
                logger.info("congressional: merged own Senate data for %s" % symbol)
        except Exception as _se:
            logger.error("senate merge for %s: %s" % (symbol, _se))
            try:
                _sconn.close()
            except Exception:
                pass

        # Merge in our OWN House Clerk data too, completing the congressional picture from first-party
        # sources. Same merge-not-replace pattern as Senate: adds coverage, deduped, tagged as ours.
        try:
            _hconn = get_db()
            if _hconn is not None:
                _hcur = _hconn.cursor()
                _hcur.execute(
                    "SELECT politician_name, party, state, transaction_type, amount, trade_date, filing_date "
                    "FROM congressional_trades_house WHERE ticker = %s AND ticker IS NOT NULL "
                    "AND transaction_type IS NOT NULL AND transaction_type != 'other' "
                    "ORDER BY trade_date DESC NULLS LAST LIMIT 20", (symbol.upper(),))
                _hseen = set()
                for _c in congressional:
                    _hseen.add((str(_c.get("politician", "")).strip().lower(), str(_c.get("date", ""))[:10],
                                "purchase" if "purchase" in str(_c.get("action", "")).lower() else "sale"))
                for _r in _hcur.fetchall():
                    _pol, _party, _state, _ttype, _amt, _td, _fd = _r
                    _act = "Purchase" if _ttype == "buy" else ("Sale" if _ttype == "sell" else (_ttype or "Unknown").title())
                    _tds = _td.isoformat() if _td else ""
                    _sig = (str(_pol or "").strip().lower(), str(_tds)[:10],
                            "purchase" if _ttype == "buy" else "sale")
                    if _sig in _hseen:
                        continue
                    if not _valid_trade_date(_tds, _fd):
                        continue  # quarantine impossible dates
                    _hseen.add(_sig)
                    _coms = _committees_for(_pol)
                    _lag = None
                    try:
                        if _td and _fd:
                            _lag = (_fd - _td).days
                    except Exception:
                        _lag = None
                    congressional.append({
                        "politician": _pol, "party": _party if _party and _party != "Unknown" else "",
                        "state": _state if _state and _state != "Unknown" else "",
                        "action": _act, "amount": _amt or "", "date": _tds,
                        "report_date": _fd.isoformat() if _fd else "", "filing_lag_days": _lag,
                        "committees": _coms[:3], "committee_relevant": is_high_clout(_pol, _sector_for_cong),
                        "source": "apexq_house",
                    })
                _hcur.close(); _hconn.close()
                logger.info("congressional: merged own House data for %s" % symbol)
        except Exception as _he:
            logger.error("house merge for %s: %s" % (symbol, _he))
            try:
                _hconn.close()
            except Exception:
                pass

        cong_buys = len([t for t in congressional if "purchase" in str(t.get("action", "")).lower()])
        cong_sells = len([t for t in congressional if "sale" in str(t.get("action", "")).lower()])
        cong_committee_relevant = any(t.get("committee_relevant") for t in congressional)
        # Recency: any trade within 30 days.
        cong_recent = False
        for t in congressional:
            try:
                if t.get("date") and (datetime.now() - datetime.strptime(t["date"][:10], "%Y-%m-%d")).days <= 30:
                    cong_recent = True
                    break
            except Exception:
                pass
        cong_size_big = any("50,000" in str(t.get("amount", "")) or "100,000" in str(t.get("amount", "")) or "1,000,000" in str(t.get("amount", "")) for t in congressional)
        cong_net = cong_buys - cong_sells
        if cong_net >= 2:
            score += 2
        elif cong_net == 1:
            score += 1
        elif cong_net <= -2:
            score -= 1

        # Insider activity. Primary source is yfinance (free, same source as the price data
        # that already works), with Quiver as a fallback if a key is present.
        insider = []
        CLEVEL = ["CHIEF", "CEO", "CFO", "COO", "CTO", "PRESIDENT", "CHAIR", "DIRECTOR", "OFFICER", "FOUNDER", "10%", "VICE PRESIDENT", "EVP", "SVP"]

        # Prefer our own SEC EDGAR data. It is sourced directly from government filings, carries real
        # per share prices (which yfinance's insider feed lacks), and is already deduplicated. If our
        # table has recent transactions for this ticker we use them; otherwise we fall back to the
        # yfinance path below, so nothing breaks while the SEC tables are still filling in.
        _sec_insider_used = False
        try:
            _sconn = get_db()
            if _sconn is not None:
                _scur = _sconn.cursor()
                _scur.execute(
                    "SELECT insider_name, insider_title, transaction_code, shares, price, "
                    "transaction_date, ownership_type FROM insider_transactions_sec "
                    "WHERE UPPER(derived_ticker) = %s AND transaction_date IS NOT NULL "
                    "ORDER BY transaction_date DESC LIMIT 12", (symbol.upper(),))
                _srows = _scur.fetchall()
                _scur.close(); _sconn.close()
                if _srows:
                    _sec_seen = set()
                    for _r in _srows:
                        _nm, _title, _code, _shares, _price, _tdate, _own = _r
                        # transaction_code is stored as "RAWCODE:category", e.g. "P:buy".
                        _cat = _code.split(":")[-1] if _code and ":" in _code else (_code or "")
                        _cat = _cat.lower()
                        if _cat == "buy":
                            _kind, _action = "buy", "Purchase"
                        elif _cat == "sell":
                            _kind, _action = "sell", "Sale"
                        elif _cat == "grant":
                            _kind, _action = "grant", "Grant"
                        elif _cat == "option_exercise":
                            _kind, _action = "option", "Option Exercise"
                        elif _cat == "tax_withholding":
                            _kind, _action = "tax", "Tax Withholding"
                        else:
                            _kind, _action = "other", (_code or "Other")
                        _sig = (str(_nm).strip().lower(), str(_shares), str(_tdate), _action)
                        if _sig in _sec_seen:
                            continue
                        _sec_seen.add(_sig)
                        _tu = str(_title or "").upper()
                        _is_cl = any(k in _tu for k in CLEVEL) and "10%" not in _tu
                        _is_holder = "10%" in _tu
                        insider.append({
                            "name": str(_nm or "Unknown"),
                            "title": str(_title or ""),
                            "action": _action,
                            "kind": _kind,
                            "desc": _action,
                            "shares": int(float(_shares)) if _shares is not None else 0,
                            "price": float(_price) if _price is not None else 0,
                            "date": str(_tdate)[:10] if _tdate else "",
                            "is_clevel": _is_cl,
                            "is_holder": _is_holder,
                        })
                    if insider:
                        _sec_insider_used = True
                        logger.info("insider data from SEC table for %s (%s rows)" % (symbol, len(insider)))
        except Exception as _se:
            logger.error("SEC insider read for %s: %s" % (symbol, _se))

        def classify_kind(text):
            # Read what the filing actually is. Only real open market buys and sells move the
            # verdict. Grants, option exercises, and tax withholding are routine and stay neutral.
            t = str(text).lower()
            if any(w in t for w in ["award", "grant", "gift", "bonus"]):
                return "grant"
            if any(w in t for w in ["exercise", "conversion", "convert", "option", "derivative"]):
                return "option"
            if any(w in t for w in ["tax", "withh", "surrender", "forfeit"]):
                return "tax"
            if any(w in t for w in ["sale", "sold", "sell"]):
                return "sell"
            if any(w in t for w in ["purchase", "bought"]):
                return "buy"
            if "dispos" in t:
                return "sell"
            if "acqui" in t:
                return "buy"
            return "other"

        try:
            it = None
            if not _sec_insider_used:
                it = ticker.insider_transactions
            if it is not None and not it.empty:
                def pick(row, *names):
                    for n in names:
                        if n in row and row.get(n) is not None:
                            return row.get(n)
                    return None
                _seen_txns = set()
                for _, rrow in it.head(12).iterrows():
                    row = rrow.to_dict()
                    name = pick(row, "Insider", "Name") or "Unknown"
                    pos = pick(row, "Position", "Title", "Relation") or ""
                    shares = pick(row, "Shares") or 0
                    date_raw = pick(row, "Start Date", "Date", "startDate")
                    # Read the actual filing type. Prefer the transaction description, fall back
                    # to scanning the whole row, then map to buy or sell only for real trades.
                    desc = pick(row, "Transaction", "Text") or ""
                    basis = str(desc) if str(desc).strip() else " ".join(str(v) for v in row.values())
                    kind = classify_kind(basis)
                    action = "D" if kind == "sell" else ("A" if kind == "buy" else "")
                    title_up = str(pos).upper()
                    name_up = str(name).upper()
                    is_cl = any(c in title_up for c in CLEVEL)
                    fundish = any(w in name_up for w in ["L.P", "PARTNERS", "MANAGEMENT", "CAPITAL", " FUND", "FUND ", "LLC", "TRUST", "HOLDINGS", "ADVISOR", "ASSOCIATES", "GROUP"])
                    is_holder = (not is_cl) or fundish or ("10%" in title_up)
                    try:
                        shares_val = int(float(shares))
                    except Exception:
                        shares_val = 0
                    date_str = str(date_raw)[:10] if date_raw is not None else ""
                    # Dedupe: yfinance sometimes returns the same transaction twice. Key by insider,
                    # shares, date, and action so an identical row is never counted or shown twice.
                    _sig = (str(name).strip().lower(), str(shares), date_str, action)
                    if _sig in _seen_txns:
                        continue
                    _seen_txns.add(_sig)
                    insider.append({
                        "name": flip_name(name),
                        "title": str(pos),
                        "action": action,
                        "kind": kind,
                        "desc": str(desc)[:70],
                        "shares": shares_val,
                        "price": 0,
                        "date": date_str,
                        "is_clevel": is_cl,
                        "is_holder": is_holder,
                    })
        except Exception as e:
            logger.error("yfinance insider error: %s" % e)

        # Grant detection fallback. When the data source gives no transaction wording (a blank
        # description leaves a row as "other"), use the unmistakable fingerprint of a board grant:
        # several insiders receiving the same share count on the same day. That pattern is annual
        # director or executive stock pay, not selling, so label it GRANT and keep it neutral.
        from collections import defaultdict
        groups = defaultdict(list)
        for t in insider:
            if t.get("kind") == "other" and t.get("shares"):
                groups[(t.get("date"), t.get("shares"))].append(t)
        for keypair, rows in groups.items():
            if len(rows) >= 3:
                for t in rows:
                    t["kind"] = "grant"

        if not insider and QUIVER_KEY:
            try:
                url = f"https://api.quiverquant.com/beta/historical/insiders/{symbol}"
                h = {"Authorization": f"Token {QUIVER_KEY}", "Accept": "application/json"}
                r = requests.get(url, headers=h, timeout=8)
                if r.status_code == 200:
                    for t in r.json()[:10]:
                        title = str(t.get("Title", "")).upper()
                        ad = t.get("AcquiredDisposed", "")
                        k = "sell" if ad == "D" else ("buy" if ad == "A" else "other")
                        nm_q = str(t.get("Name", "")).upper()
                        is_cl_q = any(c in title for c in CLEVEL)
                        fundish_q = any(w in nm_q for w in ["L.P", "PARTNERS", "MANAGEMENT", "CAPITAL", " FUND", "FUND ", "LLC", "TRUST", "HOLDINGS", "ADVISOR", "ASSOCIATES", "GROUP"])
                        insider.append({"name": t.get("Name", "Unknown"), "title": t.get("Title", ""), "action": ad, "kind": k, "desc": "", "shares": t.get("Shares", 0), "price": fmt_price(t.get("Price", 0)), "date": t.get("Date", ""), "is_clevel": is_cl_q, "is_holder": (not is_cl_q) or fundish_q or ("10%" in title)})
            except Exception as e:
                logger.error(f"Insider Quiver fallback error: {e}")

        insider = insider[:8]

        ins_buys = len([t for t in insider if t.get("is_clevel") and t.get("action") == "A"])
        ins_sells = len([t for t in insider if t.get("is_clevel") and t.get("action") == "D"])

        # Estimate the dollar value of each sale using the current price, attach it to every row
        # so it can be shown, and total it.
        price_for_value = cur if isinstance(cur, (int, float)) and cur > 0 else 0
        for t in insider:
            try:
                t["value"] = int(t.get("shares") or 0) * price_for_value
            except Exception:
                t["value"] = 0
        exec_sell_value = sum(t.get("value", 0) for t in insider if t.get("is_clevel") and t.get("action") == "D")
        all_sell_values = [t.get("value", 0) for t in insider if t.get("action") == "D"]
        total_sell_value = sum(all_sell_values)
        max_single_sell = max(all_sell_values) if all_sell_values else 0
        # Split the selling. The headline counts only the company's own people (executives,
        # officers, directors). Outside holders like activist funds are counted separately so
        # a fund trimming a position never inflates the insider figure.
        insider_sell_value = sum(t.get("value", 0) for t in insider if t.get("action") == "D" and not t.get("is_holder"))
        holder_sell_value = sum(t.get("value", 0) for t in insider if t.get("action") == "D" and t.get("is_holder"))

        mc_num = info.get("marketCap") if isinstance(info.get("marketCap"), (int, float)) else 0
        big_block = max_single_sell >= 100000000 or (mc_num > 0 and max_single_sell >= 0.01 * mc_num)

        # CHUNK: read insider selling in context. Buying is always a strong positive, so it scores
        # straight away. Selling is softer, and after a big run it is usually profit taking. So in a
        # strong uptrend the whole selling penalty is held to at most one point and never overrides
        # an APPROVE. Flat or falling, selling keeps its full weight and can still cap the verdict.
        strong_uptrend = is_strong_uptrend(info, cur)

        if ins_buys >= 2:
            score += 3
        elif ins_buys == 1:
            score += 2

        sell_penalty = 0
        if ins_sells >= 4:
            sell_penalty += 4
        elif ins_sells >= 2:
            sell_penalty += 2
        elif ins_sells == 1:
            sell_penalty += 1
        if exec_sell_value >= 50000000:
            sell_penalty += 2
        elif exec_sell_value >= 20000000:
            sell_penalty += 1
        if big_block:
            sell_penalty += 1
        if strong_uptrend:
            sell_penalty = min(sell_penalty, 1)
        score -= sell_penalty

        heavy_insider_selling = ins_sells >= 3 or exec_sell_value >= 20000000

        conviction = score_to_conviction(score)

        if score >= 4:
            verdict = "APPROVE"
        elif score <= -2:
            verdict = "PASS"
        else:
            verdict = "WATCH"

        # Circuit breaker. Right after an unusually sharp single-day drop the situation is in
        # flux and the bullish signals are likely stale, so the honest call is to hold at WATCH
        # and tell the person to understand why it fell before considering anything.
        alert = None
        if sharp_drop:
            alert = "sharp_drop"
            verdict = "WATCH"

        # Insider selling cap, read in context. A cluster of executives selling refuses an APPROVE
        # while the people who know the company best head for the exit, unless the stock is in a
        # strong uptrend, where that selling is profit taking after a run rather than a warning.
        if heavy_insider_selling and verdict == "APPROVE" and not strong_uptrend:
            verdict = "WATCH"
            if alert is None:
                alert = "insider_selling"

        # The single live signal most responsible for this verdict, in plain words. Cache safe and
        # user neutral, so it is stored on the report and reused when a per user flip is detected.
        up_for_reason = None
        try:
            if isinstance(tgt, (int, float)) and eff_px:
                up_for_reason = ((float(tgt) - eff_px) / eff_px) * 100
        except Exception:
            up_for_reason = None
        verdict_reason = verdict_signal_reason(verdict, ins_buys, ins_sells, exec_sell_value, cong_buys, cong_sells, sharp_drop, eff_chg, rec, up_for_reason, heavy_insider_selling)

        # News, shared with the ETF report through one helper so both paths stay identical.
        news = build_news(symbol, ticker)

        market_cap = info.get("marketCap", "N/A")
        volume = int(hist["Volume"].iloc[-1]) if not hist.empty else 0
        beta = fmt_price(info.get("beta"))

        # Data validation guardrails. Catch bad data before it reaches the report rather than
        # displaying something obviously wrong (a 60% one-day move, a future-dated trade, a market
        # cap that does not reconcile). Quarantined items are dropped and logged, not shown.
        try:
            _validate_report_data(symbol, cur, chg, market_cap, info, congressional, insider)
        except Exception as _ve:
            logger.error("validation error for %s: %s" % (symbol, _ve))

        confidence, flags = run_referee(cur, chg, pe, tgt, rec, market_cap, volume, beta, hist, news, congressional, insider)

        # Macro overlay. A confirmed High-importance event inside 48 hours knocks confidence down one
        # level and raises a flag, because on those days the whole market can move regardless of what
        # this one stock's signals say. Estimated dates are excluded on purpose -- see econ module.
        # $100M+ DoD awards mapped to this ticker. Never allowed to break a report.
        _dod_awards = []
        try:
            _dod_awards = dod_contracts_for(symbol)
        except Exception as _dae:
            logger.warning("dod lookup skipped for %s: %s" % (symbol, _dae))

        # Earnings block. Never allowed to break a report.
        _earnings_block = None
        _maturity = None
        try:
            _earnings_block = earnings_for_report(symbol)
        except Exception as _eae:
            logger.warning("earnings lookup skipped for %s: %s" % (symbol, _eae))

        # New signal layers: 13D activists, SEC comment letters, LD-1 lobbying, clinical trials.
        # Each is independently guarded -- one bad table must never take down a report.
        _activist, _letters, _ld1, _trials = [], [], [], []
        _forward, _risk, _converge = None, None, None
        try:
            _activist = activist_for(symbol)
            _letters = comment_letters_for(symbol)
            _ld1 = lobbying_registrations_for(symbol)
            _trials = clinical_changes_for(symbol)
        except Exception as _se2:
            logger.warning("new signals skipped for %s: %s" % (symbol, _se2))

        # CONVERGENCE. Detected, shown, logged -- and deliberately NOT allowed to force APPROVE.
        # The bullish override the spec asked for would hard-code an untested hypothesis into a buy
        # signal. Every occurrence is written to the feature store instead, so the backtest can
        # decide in ~90 days whether it deserves to bind. The BEARISH cap does bind, because a wrong
        # cap costs a missed gain and a wrong bullish override costs cash.
        try:
            _converge = detect_convergence(symbol, (_sig_v2 if "_sig_v2" in dir() else {}) or {},
                                           _activist, _trials, _letters, congressional)
        except Exception as _ce:
            logger.warning("convergence skipped for %s: %s" % (symbol, _ce))
            _converge = None

        _econ_event = None
        try:
            confidence, flags, _econ_event = apply_econ_overlay(confidence, flags, info.get("sector", ""))
            # When a macro event is imminent, its coverage joins the news feed. These items carry the
            # same shape as every other article, so the existing renderer and modal make them
            # clickable with no frontend change -- the user can read WHY the market is on edge.
            if _econ_event:
                _mn = build_macro_news(_econ_event["key"])
                if _mn:
                    _econ_event = dict(_econ_event)
                    _econ_event["articles"] = _mn
                    news = _mn + [n for n in (news or []) if n.get("url") not in {a.get("url") for a in _mn}]
        except Exception as _ee:
            logger.warning("econ overlay skipped for %s: %s" % (symbol, _ee))

        # FMP second source. Additive and non blocking: display first so it can be verified,
        # then it will sharpen the verdict in a later step. Behind the 4 hour cache below.
        fmp = {"grades": [], "insider_stats": None}
        if FMP_KEY:
            ud = fmp_get("/api/v4/upgrades-downgrades?symbol=%s" % symbol)
            if not isinstance(ud, list) or not ud:
                ud = fmp_get("/api/v3/grade/%s" % symbol)
            if isinstance(ud, list):
                for g in ud[:5]:
                    firm = g.get("gradingCompany") or g.get("analystCompany") or g.get("company") or ""
                    prev = g.get("previousGrade") or ""
                    new = g.get("newGrade") or g.get("grade") or ""
                    action = str(g.get("action") or "").lower()
                    gdate = str(g.get("date") or g.get("publishedDate") or "")[:10]
                    gtarget = g.get("priceTarget") or g.get("newPriceTarget") or ""
                    if firm or new:
                        fmp["grades"].append({"firm": str(firm), "prev": str(prev), "new": str(new), "action": action, "date": gdate, "target": gtarget})
            st = fmp_get("/api/v4/insider-trading/statistics?symbol=%s" % symbol)
            if isinstance(st, list) and st:
                s0 = st[0] or {}
                buys = s0.get("purchases") or s0.get("totalPurchases") or s0.get("acquiredTransactions") or 0
                sells = s0.get("sales") or s0.get("totalSales") or s0.get("disposedTransactions") or 0
                ratio = s0.get("buySellRatio")
                fmp["insider_stats"] = {"buys": buys, "sells": sells, "ratio": ratio}

        # CHUNK: proactive Ask questions, chosen from this stock's live signals. Max 3 signal
        # questions plus the always-on plain English one, so never more than 4.
        suggested = []
        try:
            pe_num = float(pe)
        except (TypeError, ValueError):
            pe_num = None
        up_num = None
        try:
            if isinstance(tgt, (int, float)) and cur:
                up_num = round((tgt - cur) / cur * 100, 1)
        except Exception:
            up_num = None
        if pe_num is not None and pe_num > 60:
            suggested.append("Is this stock too expensive?")
        if pe_num is not None and 0 < pe_num < 12:
            suggested.append("Why is the PE so low?")
        if up_num is not None and up_num > 20:
            suggested.append("Why do analysts see so much upside?")
        if up_num is not None and up_num < -10:
            suggested.append("Why is it trading above analyst targets?")
        if verdict == "PASS":
            suggested.append("What would make this an APPROVE?")
        elif verdict == "WATCH":
            suggested.append("What would tip this to APPROVE or PASS?")
        if insider_sell_value > 10000000 or ins_sells >= 3:
            suggested.append("Why are executives selling?")
        if ins_buys >= 1:
            suggested.append("Why are executives buying their own stock?")
        if cong_buys >= 1:
            suggested.append("Why are lawmakers buying this?")
        if cong_sells >= 2:
            suggested.append("Why are lawmakers selling this?")
        if isinstance(chg, (int, float)) and chg < -5:
            suggested.append("Why did it drop so much today?")
        if isinstance(chg, (int, float)) and chg > 5:
            suggested.append("Why is it up so much today?")
        suggested = suggested[:3]
        suggested.append("Explain this verdict in plain English")

        # CHUNK: a plain-English note for the pre/post market move. Scoring already used the extended
        # price above, so the note says the verdict factors it in rather than ignoring it.
        # CHUNK: targets set before a just released earnings report are stale, so flag the upside as provisional.
        if earn == "recent" and tgt and tgt != "N/A" and eff_px:
            try:
                if ((float(tgt) - eff_px) / eff_px) * 100 > 0:
                    flags.append({"level": "warn", "text": "These analyst targets were likely set before the recent earnings report, so the upside shown may be stale until analysts revise it. Treat it as provisional."})
            except Exception:
                pass
        ext_note = ""
        if ext:
            direction = "up" if ext["change_pct"] >= 0 else "down"
            ext_note = "%s is %s %s percent in %s trading, at about $%s." % (
                symbol, direction, abs(ext["change_pct"]), ext["session"], ext["price"])
            if earn == "recent":
                ext_note += " This is right after an earnings report, and the verdict below already factors in this move. Big moves right after earnings often settle down, so treat this as fresh news to read alongside the verdict. See the news below."
            else:
                ext_note += " The verdict below already factors in this move."
        elif earn == "recent":
            ext_note = "%s reported earnings within about the last day. Check the news below for the latest, since results can shift the picture quickly." % symbol
        elif earn == "soon":
            ext_note = "%s is expected to report earnings within about a day. Results can move a stock sharply, so keep that in mind alongside the verdict below." % symbol

        # CHUNK: Feature 1 + Feature 4 — Apex Q Moat and Deep Fundamentals, read from the same
        # fundamentals so the engine fetches them once. Rule based, educational, never invented.
        prof_margin = info.get("profitMargins")
        roe = info.get("returnOnEquity")
        earn_growth = info.get("earningsGrowth")
        rev_growth = info.get("revenueGrowth")
        debt_eq = info.get("debtToEquity")

        moat_buys = len([t for t in insider if t.get("is_clevel") and t.get("action") == "A" and t.get("kind") != "grant"])
        have_moat_data = sum(1 for x in (prof_margin, roe, earn_growth, rev_growth) if isinstance(x, (int, float))) >= 2
        if have_moat_data:
            mscore = 0
            # PROFITABILITY. The old thresholds (>15% for full marks) were calibrated for
            # high-margin tech. A mature retailer like HD at 8-10% margin is actually excellent
            # for the industry. Lower thresholds so Consumer Staples, Industrials, and Retail
            # are not systematically penalized for having the margins their industries allow.
            if isinstance(prof_margin, (int, float)):
                mscore += 2 if prof_margin > 0.12 else (1 if prof_margin > 0.07 else 0)
            if isinstance(roe, (int, float)):
                mscore += 2 if roe > 0.15 else (1 if roe > 0.10 else 0)
            # GROWTH. Mature companies grow slowly by design. A business that has been the
            # dominant player in its category for decades should not score 0 just because
            # revenue grows 3% instead of 15%. Adjusted to reward stability, not just speed.
            if isinstance(earn_growth, (int, float)):
                mscore += 2 if earn_growth > 0.08 else (1 if earn_growth > 0 else 0)
            if isinstance(rev_growth, (int, float)):
                mscore += 2 if rev_growth > 0.06 else (1 if rev_growth > 0 else 0)
            # SCALE AND MARKET POSITION.
            # Mature market leaders (Home Depot, Walmart, Coca-Cola) have wide moats from brand,
            # scale, supply chain, and switching costs -- but they show it through SIZE and
            # CONSISTENCY, not growth rates. The old engine gave them 0 because they grow 3%.
            #
            # Special case: very high or undefined ROE. Home Depot's buybacks have made equity
            # negative, so ROE is technically infinite or missing. That IS a moat signal -- only
            # a company with extraordinary pricing power can sustain that kind of capital return.
            mkt = info.get("marketCap")
            # ROE undefined or extreme (>1.0 = 100%) is almost always a capital-efficiency moat
            roe_extreme = isinstance(roe, (int, float)) and roe > 1.0
            if roe_extreme:
                mscore += 2
            # Large + profitable + not a startup = structural advantage by survival
            if isinstance(mkt, (int, float)) and mkt > 50e9:
                if isinstance(prof_margin, (int, float)) and prof_margin > 0.05:
                    mscore += 1
            # Large + VERY large (>$200B) + profitable = almost certainly a moat business
            if isinstance(mkt, (int, float)) and mkt > 200e9:
                if isinstance(prof_margin, (int, float)) and prof_margin > 0.05:
                    mscore += 1   # extra point for mega-cap stability
            # INSIDER AND CONGRESSIONAL BUYING still count.
            if moat_buys >= 2:
                mscore += 1
            if cong_buys >= 2:
                mscore += 1
            m_rating = "Wide" if mscore >= 7 else ("Narrow" if mscore >= 4 else "None")
            pos_bits = []
            if isinstance(prof_margin, (int, float)) and prof_margin > 0.15:
                pos_bits.append("strong profitability")
            if isinstance(roe, (int, float)) and roe > 0.20:
                pos_bits.append("high return on equity")
            if isinstance(earn_growth, (int, float)) and earn_growth > 0.10:
                pos_bits.append("solid earnings growth")
            if isinstance(rev_growth, (int, float)) and rev_growth > 0.10:
                pos_bits.append("healthy revenue growth")
            if moat_buys >= 2:
                pos_bits.append("insider buying")
            if cong_buys >= 2:
                pos_bits.append("lawmaker buying")
            if m_rating == "Wide":
                m_reason = "Several durable strengths line up here" + ((", including " + ", ".join(pos_bits[:3])) if pos_bits else "") + ", which points to a real competitive advantage."
            elif m_rating == "Narrow":
                m_reason = "Some real strengths show up" + ((", such as " + ", ".join(pos_bits[:3])) if pos_bits else "") + ", but not deep enough to call the advantage wide."
            else:
                m_reason = "The fundamentals are mixed, with no clear durable edge standing out, so there is no real moat to point to yet."
            apex_moat = {"rating": m_rating, "score": mscore, "reason": m_reason}
            # Business rationale: WHY this kind of company can hold off competitors, by sector.
            # Answers the audit critique that "high ROE therefore moat" is not real analysis.
            if m_rating in ("Wide", "Narrow"):
                # BUGFIX: this used to read sector_name, which is not assigned until ~37 lines
                # below, so every Wide/Narrow moat stock (AAPL, NVDA) raised UnboundLocalError and
                # the whole report returned None -> "Could not pull data". Read from info directly.
                apex_moat["rationale"] = _moat_rationale(info.get("sector", ""))
        else:
            apex_moat = {"rating": None, "score": 0, "reason": "Not enough data to estimate a moat for this one."}

        revenue_growth = rev_growth if isinstance(rev_growth, (int, float)) else "N/A"
        profit_margin = prof_margin if isinstance(prof_margin, (int, float)) else "N/A"
        debt_to_equity = round(debt_eq / 100.0, 2) if isinstance(debt_eq, (int, float)) else "N/A"

        # CHUNK: Valuation Deep Dive — professional multiples, rounded, graceful N/A
        def _round_or_na(v, nd):
            return round(float(v), nd) if isinstance(v, (int, float)) else "N/A"
        peg_ratio = _round_or_na(info.get("pegRatio"), 2)
        price_to_book = _round_or_na(info.get("priceToBook"), 2)
        price_to_sales = _round_or_na(info.get("priceToSalesTrailing12Months"), 2)
        ev_to_ebitda = _round_or_na(info.get("enterpriseToEbitda"), 1)
        roe_field = roe if isinstance(roe, (int, float)) else "N/A"
        fcf = info.get("freeCashflow")
        mc_for_fcf = info.get("marketCap")
        if isinstance(fcf, (int, float)) and isinstance(mc_for_fcf, (int, float)) and mc_for_fcf:
            fcf_yield = round((fcf / mc_for_fcf) * 100, 2)
        else:
            fcf_yield = "N/A"

        # CHUNK: Sector Guide — what matters most when valuing this kind of company. Educational.
        SECTOR_GUIDE = {
            "Technology": "Tech companies are often valued on growth (PEG, EV/Revenue) and recurring revenue. High P/E can be normal if growth is strong.",
            "Financial Services": "Banks and financials are best valued using Price-to-Book and ROE, not P/E. Watch loan quality and net interest margin.",
            "Healthcare": "Healthcare companies range from stable pharma (use P/E, FCF yield) to high-growth biotech (use P/S, pipeline value). R&D spending is critical.",
            "Consumer Cyclical": "Consumer discretionary stocks are driven by economic cycles. Watch same-store sales, margins, and P/S for retail; P/E for established brands.",
            "Consumer Defensive": "Staples are steady and defensive. Reliable dividends and FCF yield matter most. Lower P/E is common.",
            "Communication Services": "This sector includes telecom (EV/EBITDA, dividend yield) and internet/media (P/E, user growth, ARPU).",
            "Energy": "Oil and gas companies are cyclical. Focus on EV/EBITDA, FCF yield, and debt levels. Commodity prices drive profits.",
            "Industrials": "Industrials are capital-intensive. P/E, EV/EBITDA, and order backlog matter. Watch FCF conversion.",
            "Basic Materials": "Mining and chemicals are commodity-driven. EV/EBITDA and P/B are key; watch global demand and cost control.",
            "Real Estate": "REITs are valued on P/FFO (Price to Funds From Operations), AFFO yield, and NAV. Occupancy and rent growth matter.",
            "Utilities": "Utilities are stable, income-focused. P/E, dividend yield, and EV/EBITDA are common. Debt and regulation are risks.",
        }
        sector_name = info.get("sector", "")
        # High Clout: flag congressional trades where the lawmaker sits on a committee tied to this
        # stock's sector, an information edge worth surfacing. Purely observational.
        try:
            for _ct in congressional:
                _ct["high_clout"] = is_high_clout(_ct.get("politician"), sector_name)
                _ct["committees"] = _committees_for(_ct.get("politician"))
        except Exception as _ce:
            logger.error("high_clout tag: %s" % _ce)
        sector_guide = SECTOR_GUIDE.get(sector_name, "Different industries use different metrics. Compare this stock to its peers in the same sector for the clearest picture.")

        # CHUNK: Analyst Consensus card. One educational object holding the consensus rating and count,
        # the target range, the Buy/Hold/Sell distribution, and the most recent rating actions. Every
        # piece is optional so a name with thin analyst coverage degrades to N/A instead of breaking.
        try:
            num_analysts = int(info.get("numberOfAnalystOpinions", 0) or 0)
        except (TypeError, ValueError):
            num_analysts = 0
        consensus_rating = _pretty_rating(info.get("recommendationKey")) or "N/A"
        th_raw = info.get("targetHighPrice")
        tl_raw = info.get("targetLowPrice")
        target_high = round(float(th_raw), 2) if isinstance(th_raw, (int, float)) else "N/A"
        target_low = round(float(tl_raw), 2) if isinstance(tl_raw, (int, float)) else "N/A"
        rating_distribution = None
        rt = info.get("recommendationTrend")
        if isinstance(rt, dict):
            rd0 = {k: int(rt.get(k, 0) or 0) for k in ("strongBuy", "buy", "hold", "sell", "strongSell")}
            if sum(rd0.values()) > 0:
                rating_distribution = rd0
        if rating_distribution is None:
            try:
                recdf = ticker.recommendations
                if recdf is not None and hasattr(recdf, "empty") and not recdf.empty:
                    row = recdf.iloc[0]
                    rcols = list(recdf.columns)
                    def _gi(c):
                        try:
                            return int(row[c]) if c in rcols else 0
                        except Exception:
                            return 0
                    rd1 = {"strongBuy": _gi("strongBuy"), "buy": _gi("buy"), "hold": _gi("hold"), "sell": _gi("sell"), "strongSell": _gi("strongSell")}
                    if sum(rd1.values()) > 0:
                        rating_distribution = rd1
            except Exception as e:
                logger.error("recommendations distribution %s: %s" % (symbol, e))
        recent_actions = []
        for g in fmp.get("grades", [])[:5]:
            recent_actions.append({
                "firm": g.get("firm", ""),
                "action": g.get("action", ""),
                "rating": g.get("new", ""),
                "target": g.get("target") or "N/A",
                "date": g.get("date", ""),
            })
        analyst_consensus = {
            "number_of_analysts": num_analysts,
            "consensus_rating": consensus_rating,
            "target_high": target_high,
            "target_low": target_low,
            "target_mean": tgt,
            "rating_distribution": rating_distribution,
            "recent_actions": recent_actions,
        }

        try:
            _alpha = compute_alpha_score(eff_chg, pe, debt_to_equity, ins_buys, ins_sells, cong_buys, cong_sells, news, sector_name, symbol=symbol)

            # FORWARD SIGNALS (+/-) and the RISK OVERLAY (deduct + cap). The overlay does not compete
            # for points inside the score -- it subtracts from the total and can cap the verdict,
            # because momentum should not be able to outvote a going-concern letter.
            try:
                _forward = compute_forward_signals(symbol, _activist, _ld1, _trials)
                _risk = compute_risk_overlay(symbol, _letters, _ld1, _trials,
                                             ins_csells=(ins_sells or 0))
                if isinstance(_alpha, dict) and _alpha.get("score") is not None:
                    _base = int(_alpha["score"])
                    _adj = _base + (_forward["points"] - 10) - _risk["deduction"]
                    _alpha["score"] = max(0, min(100, _adj))
                    _alpha.setdefault("breakdown", [])
                    _alpha["breakdown"].append(
                        "Forward Signals: %d/20 (%s)" % (
                            _forward["points"],
                            _forward["notes"][0] if _forward["notes"] else "no forward signals on file"))
                    if _risk["deduction"]:
                        _alpha["breakdown"].append(
                            "Risk Overlay: -%d pts (%s)" % (
                                _risk["deduction"],
                                _risk["notes"][0] if _risk["notes"] else "risk factors present"))
                    for _n in (_forward["notes"] + _risk["notes"]):
                        flags.append({"level": "warn" if _risk["notes"] and _n in _risk["notes"] else "info",
                                      "text": _n})
            except Exception as _le:
                logger.warning("layers skipped for %s: %s" % (symbol, _le))
            alpha_score = _alpha["score"]
            alpha_breakdown = _alpha["breakdown"]
        except Exception as e:
            logger.error("alpha score %s: %s" % (symbol, e))
            alpha_score = 0
            alpha_breakdown = []

        # ------------------------------------------------------------------------------------
        # CONVERGENCE + DATA MATURITY — applied HERE, and this placement is the whole point.
        #
        # Both of these used to run ~270 lines earlier, referencing `_alpha` before it existed.
        # That is an UnboundLocalError, and because it sat inside a try/except it was SWALLOWED:
        # the convergence bonus, the bearish WATCH cap, and the convergence event log had never
        # once executed. Identical failure to the sector_name bug -- a silent ordering error that
        # looks like a working feature.
        # ------------------------------------------------------------------------------------
        if _converge and _converge.get("converged"):
            try:
                alpha_score = max(0, min(100, int(alpha_score) + int(_converge["bonus"])))
                if isinstance(_alpha, dict):
                    _alpha["score"] = alpha_score
                # The cap binds. APPROVE is never forced on an unvalidated pattern.
                if _converge.get("cap") == "WATCH" and verdict == "APPROVE":
                    verdict = "WATCH"
                    flags.append({"level": "warn", "text": _converge["implication"]})
                log_convergence(symbol, _converge, alpha_score, verdict)
            except Exception as _ce2:
                logger.warning("convergence apply %s: %s" % (symbol, _ce2))

        # Maturity attaches ONLY inside a live earnings window -- no event, no badge, no wallpaper.
        # On PRELIMINARY, confidence steps down one LEVEL (it is a string, not a number).
        _maturity = None
        try:
            _maturity, confidence, flags = apply_maturity(
                symbol, _earnings_block, alpha_score, verdict, confidence, flags)
        except Exception as _me:
            logger.warning("maturity skipped for %s: %s" % (symbol, _me))

        # New seven-factor transparent engine. Runs alongside the legacy score; its richer factor
        # breakdown and aligned verdict are surfaced to the report. Built to never raise.
        try:
            _pm = info.get("profitMargins")
            _pm = float(_pm) if _pm is not None else None
            _beta_raw = info.get("beta")
            _beta_raw = float(_beta_raw) if _beta_raw is not None else None
            _profitable = (_pm is not None and _pm > 0)
            # compute_momentum() already calculated 20d and 60d returns from real price
            # history. Wire them into _sig so compute_alpha_v2 sees real numbers instead of
            # the locals().get("month_change") calls that were ALWAYS None -- month_change and
            # three_month_change are never assigned anywhere in the codebase.
            _mom_detail = {}
            _r1m = _alpha_num(_mom_detail.get("ret_20d"))   # 20d is our "1 month" proxy
            _r3m = _alpha_num(_mom_detail.get("ret_60d"))   # 60d is our "3 month" proxy
            _sig = {
                "r5": eff_chg, "r1m": _r1m,
                "r3m": _r3m,
                "up_days_5": locals().get("up_days_5"),
                "chg_today": eff_chg,
                "pe": pe, "profit_margin": _pm, "debt_to_equity": debt_to_equity,
                "ins_cbuys": ins_buys, "ins_csells": ins_sells,
                "ins_buy_value": locals().get("exec_buy_value"),
                "ins_sell_value": locals().get("exec_sell_value"),
                "profitable": _profitable,
                "cong_buys": cong_buys, "cong_sells": cong_sells,
                "cong_committee": bool(locals().get("cong_committee_relevant")),
                "cong_recent": bool(locals().get("cong_recent")),
                "cong_size_big": bool(locals().get("cong_size_big")),
                "cong_has_data": (cong_buys + cong_sells) > 0,
                "analyst_rating": _analyst_bullishness(locals().get("rating_distribution"), locals().get("consensus_rating")),
                "analyst_coverage": locals().get("num_analysts"),
                "analyst_upside": _target_upside(cur, tgt),
                "analyst_recent": _recent_rating_dir(locals().get("recent_actions")),
                "analyst_has_data": bool(locals().get("num_analysts") or locals().get("rating_distribution") or (tgt and tgt != "N/A")),
                "beta": _beta_raw,
                "news_sentiment": locals().get("news_sentiment_score"),
                "has_catalyst": bool(locals().get("has_catalyst")),
                "news_has_data": bool(news),
            }
            _v2 = compute_alpha_v2(_sig)
            alpha_score = _v2["score"]
            alpha_v2 = _v2
            try:
                _avgvol = info.get("averageVolume") or info.get("averageDailyVolume10Day")
                _v2["conditions"] = compute_verdict_conditions(_v2, cur, _avgvol, _r1m)
            except Exception:
                _v2["conditions"] = []
        except Exception as e:
            logger.error("alpha v2 %s: %s" % (symbol, e))
            alpha_v2 = None

        # Data validation guardrails: sanity-check the price move and reconcile market cap. A move
        # over 20% in a session without a catalyst is flagged (likely a bad tick); a mismatched
        # market cap is recomputed from shares. Accuracy over completeness.
        _price_ok, _price_pct = _price_move_sane(cur, locals().get("prev_close_val") or locals().get("prev") or cur, bool(locals().get("has_catalyst")))
        _reconciled_cap = _reconcile_market_cap(cur, info.get("sharesOutstanding"), info.get("marketCap"))
        _data_quality = "ok" if _price_ok else "price_move_flagged"

        result = {
            "symbol": symbol,
            "name": info.get("longName", symbol),
            "sector": info.get("sector", ""),
            "company_summary": info.get("longBusinessSummary") or "",
            "price": cur,
            "change_pct": chg,
            "price_source": price_source,
            "data_quality": _data_quality,
            "market_cap_reconciled": _reconciled_cap,
            "recommendation": rec,
            "verdict": (alpha_v2.get("verdict") if (alpha_v2 and alpha_v2.get("verdict")) else verdict),
            "legacy_verdict": verdict,
            "alert": alert,
            "verdict_signal_reason": verdict_reason,
            "conviction": conviction,
            "score": score,
            "alpha_score": alpha_score,
            "alpha_breakdown": alpha_breakdown,
            "alpha_v2": alpha_v2,
            "pe_ratio": pe,
            "analyst_target": tgt,
            "market_cap": market_cap,
            "volume": volume,
            "beta": beta,
            "confidence": confidence,
            "flags": flags,
            "economic_event": _econ_event,
            "dod_contracts": _dod_awards,
            "earnings": _earnings_block,
            "data_maturity": _maturity,
            "activist": _activist,
            "comment_letters": _letters,
            "lobbying_registrations": _ld1,
            "clinical_trials": _trials,
            "convergence": _converge,
            "forward_signals": _forward,
            "risk_overlay": _risk,
            "fmp": fmp,
            "insider_sell_value": insider_sell_value,
            "holder_sell_value": holder_sell_value,
            "insider_big_block": big_block,
            "apex_moat": apex_moat,
            "revenue_growth": revenue_growth,
            "profit_margin": profit_margin,
            "debt_to_equity": debt_to_equity,
            "peg_ratio": peg_ratio,
            "price_to_book": price_to_book,
            "price_to_sales": price_to_sales,
            "ev_to_ebitda": ev_to_ebitda,
            "roe": roe_field,
            "fcf_yield": fcf_yield,
            "sector_guide": sector_guide,
            "analyst_consensus": analyst_consensus,
            "extended": ext,
            "earnings": earn,
            "extended_note": ext_note,
            "news": news,
            "congressional": congressional,
            "insider": insider,
            "active_cluster": detect_insider_cluster(symbol, info.get("longName") or info.get("shortName") or symbol, insider),
            "suggested_questions": suggested,
            "data_timestamp": int(time.time()),
        }

        set_cache(f"full_{symbol}", result)

        # Every analyze writes today's analyst state to the snapshot table and enrolls the symbol in
        # the daily sweep, so the revision dataset accumulates from real traffic as well as from
        # cron. Wrapped tightly: a snapshot failure must never take down a report.
        try:
            snap_universe_add(symbol)
            record_recent_search(symbol)
            record_analyst_snapshot(symbol, info)
            # Point-in-time feature store: record what the engine knew and decided TODAY, before the
            # outcome exists. Labels get attached later by /cron/label-outcomes. This is the row a
            # future backtest reads, and it is the reason that backtest will be honest.
            record_feature_snapshot(symbol, result, info)
        except Exception as _se:
            logger.warning("snapshot capture skipped for %s: %s" % (symbol, _se))

        return result

    except Exception as e:
        # This catch-all hides the real cause of every failure inside this ~950-line function; it is
        # what disguised the sector_name UnboundLocalError as a data problem for weeks. The full
        # traceback now reaches the log, so the next bug takes minutes instead of a night.
        logger.error("Analyze error for %s: %s\n%s" % (symbol, e, traceback.format_exc()))
        return None


@app.route("/context")
def context():
    # Live market context. Runs separately so it can never slow or break the main report.
    symbol = request.args.get("symbol", "").strip().upper()
    if not symbol:
        return jsonify({"live": None})
    if not GEMINI_KEY:
        return jsonify({"live": None})

    cached = get_cache(f"ctx_{symbol}")
    if cached is not None:
        return jsonify({"live": cached})

    # CHUNK: ground the live context in real numbers so Gemini answers from the engine's facts,
    # not stale training data. No live data means no call.
    live_data = light_score(symbol)
    if live_data is None:
        return jsonify({"live": None})
    facts = f"Current facts for {symbol}: Price ${live_data.get('price')}, Change {live_data.get('change_pct')}%, PE ratio {live_data.get('pe_ratio')}, Analyst upside {live_data.get('upside')}%, Verdict {live_data.get('verdict')}."

    try:
        prompt = (
            "You are the live intelligence layer for an educational stock app built for everyday people, "
            "including beginners who have never invested before. The user is looking at " + symbol + ". "
            "Here are the engine's current live facts for this stock: " + facts + " "
            "Using ONLY these facts, return ONLY valid JSON, no markdown, no extra words, with these keys: "
            "current_context (2 to 3 plain sentences on what is happening with this company right now), "
            "why_it_matters (2 sentences on why a regular person with no finance background should care right now), "
            "watch_for (one specific thing to watch in the next 30 days that could move the price), "
            "simple_lesson (one sentence teaching a basic investing idea that applies to this exact situation, written for a smart teenager). "
            "Keep every sentence simple and free of jargon."
        )
        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=" + GEMINI_KEY
        payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.2, "maxOutputTokens": 600}}
        r = requests.post(url, json=payload, timeout=12)
        logger.info(f"Gemini status {r.status_code} for {symbol}")
        if r.status_code == 200:
            data = r.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            if "```" in text:
                for part in text.split("```"):
                    p = part.strip()
                    if p.startswith("json"):
                        p = p[4:].strip()
                    if p.startswith("{"):
                        text = p
                        break
            live = json.loads(text)
            set_cache(f"ctx_{symbol}", live)
            return jsonify({"live": live})
    except Exception as e:
        logger.error(f"Context error for {symbol}: {e}")

    set_cache(f"ctx_{symbol}", None)
    return jsonify({"live": None})


THEMES = {
  # STABILIZATION: global, crypto, and macro themes are disabled until rebuilt and tested.
  # They caused slow loads, permanent WAITING blocks, and Mandarin leaks. Re-enable by
  # removing the triple quotes below. Data preserved intact.
  """
  "crypto": {
    "name": "Crypto Majors",
    "explainer": "The largest cryptocurrencies by market value, priced in US dollars around the clock. Crypto trades every hour of every day, moves far more violently than stocks, and has no earnings, no CEO, and no balance sheet. The price is the entire story, driven by adoption, liquidity, and crowd conviction.",
    "why": "Digital assets have become a real allocation in millions of portfolios, and the majors are where the liquidity lives. Watching them alongside stocks shows how risk appetite is shifting across the whole market.",
    "unknown": "Most crypto tools show price and nothing else. Seeing the majors inside the same engine as stocks, with momentum and news in plain English, makes the volatility legible instead of terrifying.",
    "tickers": ["BTC-USD","ETH-USD","SOL-USD","BNB-USD","XRP-USD","ADA-USD","DOGE-USD","AVAX-USD","DOT-USD","LINK-USD","LTC-USD","MATIC-USD"]
  },
  "macro": {
    "name": "Macro: Forex, Commodities, and Rates",
    "explainer": "The prices that move everything else: metals and energy futures, major currency pairs, the ten year Treasury yield, and the broad commodity index. These are not companies. They are the weather system the entire market lives inside.",
    "why": "Stocks do not move in a vacuum. When the dollar, oil, copper, or interest rates shift, whole sectors reprice. Watching the macro board explains days when every stock moves together for no company specific reason.",
    "unknown": "Everyday investors rarely look at futures and yields because the tickers look like hieroglyphics. Named and explained in plain English, they become the most useful dashboard in investing.",
    "tickers": ["GC=F","SI=F","HG=F","CL=F","NG=F","EURUSD=X","USDJPY=X","GBPUSD=X","^TNX","BCOM"]
  },
  "japan": {
    "name": "Japan Blue Chips",
    "explainer": "Japan's flagship companies listed in Tokyo, from carmakers and game giants to the trading houses and chip equipment leaders. Prices are in yen and trade during Tokyo hours.",
    "why": "Japan is the third largest stock market on earth and has been waking up after decades of quiet, with corporate reforms and famous value investors moving in. It offers world class businesses at valuations US markets rarely see.",
    "unknown": "Most US investors could not name five Japanese stocks beyond Toyota and Sony, yet names like the trading houses quietly compound for decades.",
    "tickers": ["7203.T","6758.T","9984.T","8306.T","7974.T","6501.T","8058.T","6902.T"]
  },
  "india": {
    "name": "India Growth Leaders",
    "explainer": "India's largest listed companies on the National Stock Exchange, spanning banking, technology services, energy, and consumer businesses. Prices are in rupees and trade during Mumbai hours.",
    "why": "India is the fastest growing major economy with a young population moving into the middle class. Its market has compounded for years on domestic demand rather than exports, a different engine than the rest of Asia.",
    "unknown": "The companies powering that growth are household names to a billion people and nearly unknown to US investors.",
    "tickers": ["RELIANCE.NS","TCS.NS","HDFCBANK.NS","INFY.NS","ICICIBANK.NS","BHARTIARTL.NS","ITC.NS","LT.NS"]
  },
  "korea": {
    "name": "South Korea Tech and Industry",
    "explainer": "Korea's industrial and technology champions listed in Seoul, led by the memory chip giants that supply the world's AI buildout. Prices are in won and trade during Seoul hours.",
    "why": "Korean memory makers sit directly inside the AI supply chain, and the market often prices them far below US peers doing similar work. When memory cycles turn, these names move first.",
    "unknown": "Everyone knows the AI chip designers. Far fewer own the companies making the memory those chips cannot run without.",
    "tickers": ["005930.KS","000660.KS","373220.KS","005380.KS","035420.KS","051910.KS"]
  },
  "australia": {
    "name": "Australia Miners and Banks",
    "explainer": "Australia's market leaders listed in Sydney: the iron ore and mining giants that feed global construction, the big banks, and the health care standout. Prices are in Australian dollars and trade during Sydney hours.",
    "why": "Australia is the raw materials counter of the world economy. When China builds, these miners profit, and the big dividend paying banks anchor the other half of the market.",
    "unknown": "The miners are among the highest dividend payers on earth in good commodity years, something US income investors rarely discover.",
    "tickers": ["BHP.AX","RIO.AX","CBA.AX","CSL.AX","FMG.AX","WES.AX","NAB.AX"]
  },
  """
  "semi": {
    "name": "Semiconductor Equipment and Materials",
    "explainer": "These are the companies that build the machines and supply the special materials used to manufacture computer chips. They do not make the chips you hear about. They make the tools and the ingredients that every chipmaker needs to produce them.",
    "why": "Every AI chip, phone, and data center runs on chips, and not a single one gets made without this equipment. When chip demand rises, these suppliers sell more tools to everyone in the industry at once, so they ride the whole wave instead of betting on one winner.",
    "unknown": "People know the famous chip names but almost nobody knows the suppliers behind them. These companies rarely make headlines, yet they sit at the center of the entire supply chain.",
    "tickers": ["ENTG","ONTO","ACLS","KLIC","COHU","FORM","AEHR","UCTT","ICHR","CAMT"]
  },
  "power": {
    "name": "Power and Grid for the AI Era",
    "explainer": "These companies build the electrical equipment, power systems, and grid hardware that move and manage electricity. Think transformers, switchgear, power conversion, backup power, and the gear that keeps data centers running.",
    "why": "AI data centers use enormous amounts of electricity, and the grid was never built for this kind of demand. Someone has to supply all the new electrical equipment, and these are the companies that do it. The demand is physical and it is already here.",
    "unknown": "Power equipment is unglamorous and easy to ignore, so most investors skip past it while chasing the flashy AI software names. The picks and shovels of the power build out get far less attention than they deserve.",
    "tickers": ["GEV","POWL","AEIS","NVT","VRT","BE","FLNC","HUBB","AYI"]
  },
  "nuclear": {
    "name": "Nuclear and Uranium",
    "explainer": "This group covers companies that mine uranium, enrich nuclear fuel, and build the next generation of smaller, safer reactors. It is the full chain from the raw fuel in the ground to the reactor that turns it into power.",
    "why": "AI and data centers need huge amounts of steady around the clock electricity, and nuclear is one of the few sources that can deliver it without carbon. After years out of favor, nuclear is being rebuilt, and demand for fuel and reactors is climbing.",
    "unknown": "Nuclear spent decades as a feared and forgotten corner of the market, so most everyday investors never look at it. The shift back toward nuclear power is still early and under the radar for most people.",
    "tickers": ["LEU","OKLO","SMR","BWXT","UEC","DNN","UUUU","NXE","CCJ"]
  },
  "defense": {
    "name": "Defense Tech and Drones",
    "explainer": "These companies build modern military technology. Not just traditional weapons, but drones, unmanned systems, electronics, sensors, space hardware, and the gear that defines how conflicts are fought today.",
    "why": "Governments around the world are spending heavily to modernize their militaries, and the spending is shifting toward technology, drones, and space. That creates steady long term demand backed by national budgets rather than consumer moods.",
    "unknown": "People think of a few giant defense contractors and stop there. The smaller, faster companies building the actual drones, sensors, and space systems get far less coverage even as the money flows their way.",
    "tickers": ["KTOS","AVAV","MRCY","CW","RKLB","DRS","HII","LDOS"]
  },
  "automation": {
    "name": "Industrial Automation and Robotics",
    "explainer": "These companies make the machine vision, sensors, motion control, and robotic systems that let factories and warehouses run with less human labor. They are the brains and the eyes of modern automated production.",
    "why": "Labor is expensive and hard to find, and companies are racing to automate. Bringing factories back to the United States adds even more demand. Automation is a long steady trend rather than a quick fad.",
    "unknown": "Automation hardware is technical and quiet, so it rarely trends. Most people picture humanoid robots from movies and miss the real companies quietly automating the world right now.",
    "tickers": ["CGNX","NOVT","ZBRA","NDSN","TER","ITRI","AZTA"]
  },
  "cyber": {
    "name": "Cybersecurity",
    "explainer": "These companies protect computers, networks, and data from hackers and attacks. They cover things like finding weaknesses before criminals do, protecting accounts and identities, and stopping breaches across the systems businesses depend on.",
    "why": "Every business is now online, and attacks keep rising in cost and frequency. Security is not optional spending, it is something companies must keep paying for, which makes the demand sticky and recurring.",
    "unknown": "Everyone knows a couple of the biggest security names, but the mid sized specialists that protect specific weak points get overlooked even though they are deeply embedded in how companies operate.",
    "tickers": ["TENB","RPD","QLYS","VRNS","CYBR","S"]
  },
  "tech": {
    "name": "Technology",
    "explainer": "The broad technology sector covers software, hardware, and the digital tools that businesses and people run on every day. It is the largest and most watched part of the market.",
    "why": "Technology drives modern growth, and the shift to AI, cloud, and automation keeps pulling money into the sector. It is where many of the biggest long term winners are found.",
    "unknown": "Everyone watches a handful of giant tech names, but the sector is full of smaller software and tooling companies doing critical work that rarely makes the news.",
    "tickers": ["NOW","SNOW","DDOG","NET","MDB","TEAM","HUBS","WDAY","ESTC"]
  },
  "health": {
    "name": "Health Care",
    "explainer": "Health care covers companies that keep people healthy, from medical devices and diagnostics to treatments and the services that deliver care.",
    "why": "People need health care in every economy, good or bad, which makes demand steady. An aging population and constant medical innovation keep the sector growing for the long run.",
    "unknown": "Beyond the giant drug and insurance names, there is a deep bench of smaller device and diagnostics companies quietly solving specific problems that most investors never hear about.",
    "tickers": ["PODD","TNDM","PEN","GKOS","IRTC","TMDX","INSP","NTRA","HALO"]
  },
  "financials": {
    "name": "Financials",
    "explainer": "Financials are the companies that move money. Banks, payment and fintech firms, advisory boutiques, and the plumbing that markets run on.",
    "why": "Finance touches every other industry, so the sector reflects the whole economy. Rising activity, lending, and deal making all flow through these companies.",
    "unknown": "People think of the few giant banks and stop there, missing the boutique advisory firms, payment companies, and market infrastructure names that quietly earn steady fees.",
    "tickers": ["SOFI","AFRM","LPLA","JKHY","EVR","HLI","VIRT","FOUR","TW"]
  },
  "discretionary": {
    "name": "Consumer Discretionary",
    "explainer": "These are the things people buy when they have extra money. Restaurants, brands, retail, travel, and the products that are wants rather than needs.",
    "why": "When people feel good about money they spend more here, so the sector can run hard in good times. Strong brands build fierce loyalty and pricing power.",
    "unknown": "The famous names get all the attention, but the real growth often hides in smaller fast rising brands and restaurant chains before the crowd notices them.",
    "tickers": ["CROX","BOOT","CAVA","WING","TXRH","ELF","ONON","CELH"]
  },
  "comm": {
    "name": "Communication Services",
    "explainer": "This sector covers how we connect and what we watch. Media, advertising, streaming, gaming, and the platforms that carry attention and content.",
    "why": "Attention is the currency of the modern economy, and advertising and content spending follow it. The sector blends old media with fast moving digital platforms.",
    "unknown": "Past the giant platforms, there are overlooked advertising, media, and connectivity companies that profit from the same attention economy without the spotlight.",
    "tickers": ["CARG","YELP","MGNI","ROKU","TKO","LYV","CABO","IPG"]
  },
  "industrials": {
    "name": "Industrials",
    "explainer": "Industrials build and move the physical world. Construction, machinery, engineering, infrastructure, and the companies that put up buildings and power projects.",
    "why": "A wave of building is underway, from data centers to factories returning to the United States to upgrading old infrastructure. These are the companies doing that physical work.",
    "unknown": "The construction, engineering, and equipment firms behind the building boom get far less attention than the flashy names they are quietly building for.",
    "tickers": ["PWR","STRL","FIX","ACM","AGX","BLDR","AAON","MLI","HWM"]
  },
  "staples": {
    "name": "Consumer Staples",
    "explainer": "Staples are the things people buy no matter what. Food, drinks, household basics, and the stores and distributors that supply them.",
    "why": "Demand barely moves whether the economy is strong or weak, which makes these companies steady and defensive. They tend to hold up when the market gets scary.",
    "unknown": "Everyone knows the giant brands, but the food distributors, specialty grocers, and smaller brands that feed the country quietly grow without much notice.",
    "tickers": ["SFM","CHEF","PFGC","BRBR","FRPT","POST","COKE","CASY"]
  },
  "energy": {
    "name": "Energy",
    "explainer": "Energy covers companies that find, produce, and move oil and natural gas, plus the pipelines and services that support them.",
    "why": "The world still runs on energy, and demand for power and fuel keeps climbing. These companies can throw off strong cash and pay healthy dividends.",
    "unknown": "Beyond the supermajors everyone names, there are smaller producers and midstream pipeline companies that quietly generate serious cash flow.",
    "tickers": ["PR","AR","RRC","MGY","CHRD","DTM","AROC","KGS"]
  },
  "utilities": {
    "name": "Utilities",
    "explainer": "Utilities provide the electricity and power that everything depends on, including the companies that generate and sell it.",
    "why": "Electricity demand is surging because of AI data centers and electrification, and someone has to produce all that power. These companies sit right at the source.",
    "unknown": "Utilities were long seen as boring and slow, so most investors ignore them, even as the power producers behind the AI boom become some of the most important names in the market.",
    "tickers": ["VST","NRG","TLN","AES","PCG","CNP","NI","IDA"]
  },
  "realestate": {
    "name": "Real Estate",
    "explainer": "Real estate companies own and rent out property, but the modern sector is far more than apartments. It includes data centers, cell towers, storage, and the physical backbone of the digital world.",
    "why": "The AI and internet boom needs physical homes, the data centers, towers, and fiber that real estate companies own and lease. That ties old fashioned property to the newest technology.",
    "unknown": "People picture office buildings and malls and miss the specialized real estate companies that own the data centers and infrastructure quietly powering the digital economy.",
    "tickers": ["IRM","COLD","CUBE","LAMR","DBRG","UNIT","ADC","VICI"]
  },
  "materials": {
    "name": "Materials",
    "explainer": "Materials companies dig up and process the raw stuff everything is made from. Metals, chemicals, specialty alloys, and the critical minerals modern technology needs.",
    "why": "You cannot build chips, planes, electric cars, or weapons without these materials, and many of them are scarce or hard to source. Demand is rising as the world builds more advanced things.",
    "unknown": "Mining and chemicals sound dull, so most people skip the sector, missing the specialty metals and critical mineral companies that sit at the base of the entire supply chain.",
    "tickers": ["MP","ATI","CRS","KALU","CMC","ESI","AVNT","ALB"]
  }
}


def light_score(symbol):
    cached = get_cache("disc_" + symbol)
    if cached is not None:
        return cached
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="5d", timeout=10)
        if hist.empty:
            return None
        info = t.info

        # BACKEND UPGRADE: Finnhub real-time price fallback.
        # Tries Finnhub for a live quote first; falls back to yfinance delayed data.
        # All scoring logic below is unchanged.
        fq = fetch_finnhub_quote(symbol)
        if fq and fq.get("price"):
            cur = fq["price"]
            prev = fq["prev_close"]
            chg = fq["change_pct"]
            price_source = "finnhub"
        else:
            cur = fmt_price(hist["Close"].iloc[-1])
            prev = fmt_price(hist["Close"].iloc[-2]) if len(hist) > 1 else cur
            chg = round(((cur - prev) / prev) * 100, 2)
            price_source = "yfinance"
        pe_raw = info.get("trailingPE")
        pe = round(float(pe_raw), 2) if pe_raw else "N/A"
        tgt_raw = info.get("targetMeanPrice")
        tgt = round(float(tgt_raw), 2) if tgt_raw else "N/A"
        rec = info.get("recommendationKey", "hold").upper()
        if str(info.get("quoteType", "")).upper() == "ETF":
            res = {
                "symbol": symbol,
                "name": info.get("longName", symbol),
                "sector": info.get("category", "") or "ETF",
                "price": cur,
                "change_pct": chg,
                "pe_ratio": "N/A",
                "analyst_target": "N/A",
                "upside": None,
                "div_yield": None,
                "near_high": None,
                "market_cap": info.get("totalAssets", "N/A"),
                "conviction": "N/A",
                "score": 0,
                "verdict": "ETF",
                "price_source": price_source,
            }
            set_cache("disc_" + symbol, res)
            return res
        score = 0
        ext = extended_hours(info, cur)
        # Match the full report: scoring reads the effective extended price and a change recalculated
        # from the prior close when pre or post market data exists. The returned price stays the close.
        eff_px = ext["price"] if ext else cur
        eff_chg = round(((eff_px - prev) / prev) * 100, 2) if (ext and prev) else chg
        if eff_chg > 2:
            score += 2
        elif eff_chg > 0:
            score += 1
        elif eff_chg < -3:
            score -= 2
        else:
            score -= 1
        if rec in ["BUY", "STRONG_BUY"]:
            score += 2
        elif rec in ["SELL", "STRONG_SELL"]:
            score -= 2
        upside = None
        if tgt and cur and str(tgt) != "N/A":
            try:
                upside = round(((float(tgt) - cur) / cur) * 100, 1)
            except:
                upside = None
        # CHUNK: score the analyst upside off the effective extended price, with reduced weight on an
        # unusually large upside since that often means a stale or outlier target, matching the full report.
        if tgt and eff_px and str(tgt) != "N/A":
            try:
                up_s = ((float(tgt) - eff_px) / eff_px) * 100
                if up_s > 10:
                    score += 1 if up_s >= 40 else 2
                elif up_s > 0:
                    score += 1
                elif up_s < -5:
                    score -= 1
            except:
                pass
        if pe != "N/A":
            try:
                pn = float(pe)
                if pn < 20:
                    score += 1
                elif pn > 60:
                    score -= 1
            except:
                pass
        conviction = score_to_conviction(score)
        verdict = "APPROVE" if score >= 4 else ("PASS" if score <= -2 else "WATCH")
        # Same insider selling cap the full report uses, read in the same context, so the sector
        # list can never show APPROVE on a stock the full report would hold at WATCH, and never
        # caps a strong uptrend where insider selling is just profit taking after a run.
        if verdict == "APPROVE" and insider_selling_cap(t, cur, is_strong_uptrend(info, cur)):
            verdict = "WATCH"
        # Extra fields used by the Scans lenses, read from the same data we already have.
        div_yield = None
        drate = info.get("dividendRate")
        if drate and cur:
            try:
                div_yield = round(float(drate) / cur * 100, 2)
            except Exception:
                div_yield = None
        if div_yield is None:
            raw_dy = info.get("dividendYield")
            if raw_dy:
                try:
                    v = float(raw_dy)
                    div_yield = round(v * 100, 2) if v < 1 else round(v, 2)
                except Exception:
                    div_yield = None
        near_high = None
        hi = info.get("fiftyTwoWeekHigh")
        if hi and cur:
            try:
                near_high = round(cur / float(hi) * 100, 1)
            except Exception:
                near_high = None
        res = {
            "symbol": symbol,
            "name": info.get("longName", symbol),
            "sector": info.get("sector", ""),
            "price": cur,
            "change_pct": chg,
            "pe_ratio": pe,
            "analyst_target": tgt,
            "upside": upside,
            "div_yield": div_yield,
            "near_high": near_high,
            "market_cap": info.get("marketCap", "N/A"),
            "conviction": conviction,
            "score": score,
            "verdict": verdict,
            "price_source": price_source,
        }
        set_cache("disc_" + symbol, res)
        return res
    except Exception as e:
        logger.error("light_score %s: %s" % (symbol, e))
        return None


_TREND = {"data": None, "ts": 0}


SCAN_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AMD", "AVGO", "JPM",
    "BAC", "V", "MA", "UNH", "JNJ", "LLY", "XOM", "CVX", "WMT", "COST",
    "HD", "PG", "KO", "DIS", "NFLX", "CRM", "ORCL", "ADBE", "INTC", "QCOM",
    "VZ", "PFE", "MRK", "CAT", "BA", "NKE",
]

_SCAN = {"data": None, "ts": 0}


def scan_universe():
    # Scores a broad set of large, widely held US stocks once and caches the whole set for
    # half an hour. Each symbol is cached on its own too, so this shares work with the sector
    # lists and stays cheap after the first warmup.
    now = time.time()
    if _SCAN["data"] is not None and now - _SCAN["ts"] < 1800:
        return _SCAN["data"]
    rows = []
    for sym in SCAN_UNIVERSE:
        r = light_score(sym)
        if r:
            rows.append(r)
    _SCAN["data"] = rows
    _SCAN["ts"] = now
    return rows


def scan_insiders():
    # Names where two or more C level insiders made open market buys recently, a cluster signal.
    # Cached for half an hour. Reuses each name's light_score, so it shares warmup with the scan.
    cached = CACHE.get("scan_insiders")
    if cached and (time.time() - cached[1]) < 1800:
        return cached[0]

    def pick(row, *names):
        for n in names:
            if n in row and row.get(n) is not None:
                return row.get(n)
        return None

    results = []
    for symbol in SCAN_UNIVERSE:
        r = light_score(symbol)
        if not r:
            continue
        buy_count = 0
        try:
            it = yf.Ticker(symbol).insider_transactions
            if it is not None and hasattr(it, "empty") and not it.empty:
                for _, rrow in it.head(20).iterrows():
                    row = rrow.to_dict()
                    pos = pick(row, "Position", "Title", "Relation") or ""
                    desc = pick(row, "Transaction", "Text") or ""
                    basis = str(desc) if str(desc).strip() else " ".join(str(v) for v in row.values())
                    kind = classify_insider_kind(basis)
                    action = "D" if kind == "sell" else ("A" if kind == "buy" else "")
                    is_cl = any(c in str(pos).upper() for c in INSIDER_CLEVEL)
                    if is_cl and action == "A" and kind != "grant" and kind != "option":
                        buy_count += 1
        except Exception as e:
            logger.error("scan_insiders %s: %s" % (symbol, e))
            continue
        if buy_count >= 2:
            results.append(dict(r, reason="%d C level insiders bought shares recently, a cluster signal worth noting." % buy_count, insider_buys=buy_count))
    results.sort(key=lambda x: (x.get("insider_buys", 0), x.get("change_pct") or 0), reverse=True)
    set_cache("scan_insiders", results)
    return results


@app.route("/scan")
def scan():
    gate = usage_gate("scan")
    if gate is not None:
        return gate
    lens = (request.args.get("lens") or "strong").lower()
    rows = scan_universe()
    out = []

    def num(v):
        return isinstance(v, (int, float))

    if lens == "highs":
        cand = [r for r in rows if num(r.get("near_high")) and r["near_high"] >= 90]
        cand.sort(key=lambda r: r["near_high"], reverse=True)
        for r in cand[:12]:
            out.append(dict(r, reason="Trading at %s%% of its 52 week high, near the top of its range." % r["near_high"]))
    elif lens == "growth":
        cand = [r for r in rows if num(r.get("upside")) and r["upside"] > 0]
        cand.sort(key=lambda r: r["upside"], reverse=True)
        for r in cand[:12]:
            out.append(dict(r, reason="Analysts see about %s%% upside to their average price target." % r["upside"]))
    elif lens == "value":
        cand = []
        for r in rows:
            try:
                pen = float(r.get("pe_ratio"))
            except (TypeError, ValueError):
                continue
            if 3 <= pen <= 18:
                cand.append((pen, r))
        cand.sort(key=lambda x: x[0])
        for pen, r in cand[:12]:
            out.append(dict(r, reason="Priced at about %s times earnings, on the lower, more value leaning end." % pen))
    elif lens == "dividend":
        cand = [r for r in rows if num(r.get("div_yield")) and r["div_yield"] >= 1.5]
        cand.sort(key=lambda r: r["div_yield"], reverse=True)
        for r in cand[:12]:
            out.append(dict(r, reason="Pays about a %s%% dividend yield, toward the higher end of the group." % r["div_yield"]))
    elif lens == "insiders":
        out = scan_insiders()[:12]
    else:
        lens = "strong"
        cand = [r for r in rows if num(r.get("change_pct")) and r["change_pct"] > 0]
        cand.sort(key=lambda r: r["change_pct"], reverse=True)
        for r in cand[:12]:
            out.append(dict(r, reason="Up %s%% today, among the strongest movers in the group." % r["change_pct"]))

    return jsonify({"lens": lens, "items": out})


# ============ Congressional Insights, powered by Quiver historical congress trading ============
# Heavy data, so everything is capped and cached hard. Returns are best effort: a strict fetch
# budget keeps a cold load under the request timeout, and anything not computed shows as null
# rather than a guess. Nothing here is ever fabricated.
_CONGRESS_HIST = {}  # ticker -> {"ts", "pairs": [(date, close)], "last": float|None}


def get_all_congress_trades():
    # The bulk Quiver endpoint returns nothing on this plan, so build the same list by looping the
    # per-symbol endpoint that already powers the stock reports. This scopes the leaderboard to the
    # names Apex Q tracks rather than all of Congress, but every record is real Quiver data. Cached
    # for two hours. Same cache key and return type, so the leaderboard and detail routes are untouched.
    cached = CACHE.get("congress_all_trades")
    if cached and (time.time() - cached[1]) < 7200:
        return cached[0]
    trades = []
    if QUIVER_KEY:
        seen = set()
        h = {"Authorization": "Token " + QUIVER_KEY, "Accept": "application/json"}
        for symbol in SCAN_UNIVERSE:
            try:
                url = "https://api.quiverquant.com/beta/historical/congresstrading/" + symbol
                r = requests.get(url, headers=h, timeout=8)
                if r.status_code != 200:
                    logger.error("congress per-symbol %s status %s" % (symbol, r.status_code))
                    continue
                data = r.json()
                if not isinstance(data, list):
                    continue
                for t in data:
                    if not isinstance(t, dict):
                        continue
                    # The per-symbol endpoint omits the ticker since the query implies it, so stamp it
                    # on every record. Grouping, top tickers, and the returns math all read Ticker.
                    if not (t.get("Ticker") or t.get("ticker")):
                        t["Ticker"] = symbol
                    # Dedupe on a real id when present, otherwise a composite signature of the trade.
                    uid = t.get("id") or t.get("ID") or t.get("_id")
                    if uid is None:
                        uid = "|".join([
                            str(t.get("Ticker") or symbol),
                            str(t.get("Representative") or t.get("Name") or ""),
                            str(t.get("Transaction") or t.get("Action") or ""),
                            str(t.get("TransactionDate") or t.get("Date") or ""),
                            str(t.get("Range") or t.get("Amount") or ""),
                        ])
                    if uid in seen:
                        continue
                    seen.add(uid)
                    trades.append(t)
            except Exception as e:
                logger.error("congress per-symbol %s error: %s" % (symbol, e))
                continue
        logger.info("congress aggregated %s trades across %s symbols" % (len(trades), len(SCAN_UNIVERSE)))
    # Only cache a real result, so a transient Quiver miss does not freeze an empty board for 2 hours.
    if trades:
        set_cache("congress_all_trades", trades)
    return trades


def _ctrade_ticker(t):
    return str(t.get("Ticker") or t.get("ticker") or "").strip().upper()


def _ctrade_name(t):
    return str(t.get("Representative") or t.get("Name") or "").strip()


def _ctrade_action(t):
    return str(t.get("Transaction") or t.get("Action") or "")


def _ctrade_date(t):
    return str(t.get("TransactionDate") or t.get("Date") or "")[:10]


def _congress_hist(ticker_sym):
    now = time.time()
    c = _CONGRESS_HIST.get(ticker_sym)
    if c and now - c["ts"] < 7200:
        return c
    pairs, last = [], None
    try:
        hh = yf.Ticker(ticker_sym).history(period="1y", timeout=10)
        if hh is not None and not hh.empty:
            closes = hh["Close"].tolist()
            idx = hh.index.tolist()
            for i in range(len(idx)):
                try:
                    d = idx[i].date()
                except Exception:
                    continue
                cv = closes[i]
                if isinstance(cv, (int, float)):
                    pairs.append((d, float(cv)))
            if pairs:
                last = pairs[-1][1]
    except Exception:
        pairs, last = [], None
    rec = {"ts": now, "pairs": pairs, "last": last}
    _CONGRESS_HIST[ticker_sym] = rec
    return rec


def _return_since(pairs, trade_date_str, last_close):
    if not pairs or last_close is None:
        return None
    try:
        td = datetime.strptime(str(trade_date_str)[:10], "%Y-%m-%d").date()
    except Exception:
        return None
    buy = None
    for d, c in pairs:
        if d >= td:
            buy = c
            break
    if buy is None or buy <= 0:
        return None
    try:
        return (last_close / buy - 1.0) * 100.0
    except Exception:
        return None


@app.route("/congress/insights")
def congress_insights():
    cached = CACHE.get("congress_insights")
    if cached and (time.time() - cached[1]) < 3600:
        return jsonify(cached[0])
    trades = get_all_congress_trades()
    if not trades:
        return jsonify({"politicians": []})
    by_pol = {}
    for t in trades:
        name = _ctrade_name(t)
        if not name:
            continue
        p = by_pol.get(name)
        if p is None:
            p = {"name": name, "party": "", "state": "", "trades": [], "tickers": {}}
            by_pol[name] = p
        if not p["party"]:
            p["party"] = str(t.get("Party") or "").strip()
        if not p["state"]:
            p["state"] = str(t.get("State") or "").strip()
        p["trades"].append(t)
        tk = _ctrade_ticker(t)
        if tk:
            p["tickers"][tk] = p["tickers"].get(tk, 0) + 1
    pols = sorted(by_pol.values(), key=lambda x: len(x["trades"]), reverse=True)[:20]
    budget = 30
    out = []
    for p in pols:
        name = p["name"]
        rc = CACHE.get("congress_returns_" + name)
        if rc and (time.time() - rc[1]) < 3600:
            ret = rc[0]
        else:
            purchases = [t for t in p["trades"] if "purchase" in _ctrade_action(t).lower()]
            purchases.sort(key=lambda t: _ctrade_date(t), reverse=True)
            seen = []
            rets = []
            for t in purchases:
                tk = _ctrade_ticker(t)
                if not tk or tk in seen:
                    continue
                seen.append(tk)
                if len(seen) > 5 or budget <= 0:
                    break
                hrec = _congress_hist(tk)
                budget -= 1
                r1 = _return_since(hrec["pairs"], _ctrade_date(t), hrec["last"])
                if r1 is not None:
                    rets.append(r1)
            ret = round(sum(rets) / len(rets), 1) if rets else None
            set_cache("congress_returns_" + name, ret)
        top_tickers = sorted(p["tickers"].items(), key=lambda kv: kv[1], reverse=True)[:3]
        out.append({
            "name": name,
            "party": p["party"],
            "state": p["state"],
            "trade_count": len(p["trades"]),
            "photo_url": None,
            "top_tickers": [k for k, _ in top_tickers],
            "returns": ret,
        })
    out.sort(key=lambda x: (x["trade_count"], x["returns"] if x["returns"] is not None else -9999), reverse=True)
    payload = {"politicians": out}
    set_cache("congress_insights", payload)
    return jsonify(payload)


def _price_pairs(ticker_sym, start_date, end_date):
    """Sorted [(date, close)] for a ticker, start inclusive and end exclusive. Cached one hour.
    Returns [] on any failure so one bad ticker can never break a whole backtest."""
    ckey = "pp_" + ticker_sym + "_" + start_date.isoformat() + "_" + end_date.isoformat()
    cached = CACHE.get(ckey)
    if cached and (time.time() - cached[1]) < 3600:
        return cached[0]
    pairs = []
    try:
        hh = yf.Ticker(ticker_sym).history(start=start_date.isoformat(), end=end_date.isoformat(), timeout=10)
        if hh is not None and not hh.empty:
            closes = hh["Close"].tolist()
            idx = hh.index.tolist()
            for i in range(len(idx)):
                try:
                    d = idx[i].date()
                except Exception:
                    continue
                cv = closes[i]
                # cv == cv is False only for NaN, so this drops gaps without needing the math module.
                if isinstance(cv, (int, float)) and cv == cv and cv > 0:
                    pairs.append((d, float(cv)))
    except Exception as e:
        logger.error("_price_pairs %s: %s" % (ticker_sym, e))
        pairs = []
    pairs.sort(key=lambda x: x[0])
    if pairs:
        set_cache(ckey, pairs)
    return pairs


@app.route("/backtest-congress")
def backtest_congress():
    name = (request.args.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Provide a politician name."}), 400
    try:
        capital = float(request.args.get("capital") or 10000)
    except (TypeError, ValueError):
        capital = 10000.0
    if capital <= 0:
        capital = 10000.0

    today = datetime.now().date()
    end_str = (request.args.get("end") or "").strip()
    start_str = (request.args.get("start") or "").strip()
    try:
        end_date = datetime.strptime(end_str, "%Y-%m-%d").date() if end_str else today
    except ValueError:
        end_date = today
    try:
        start_date = datetime.strptime(start_str, "%Y-%m-%d").date() if start_str else (end_date - timedelta(days=365))
    except ValueError:
        start_date = end_date - timedelta(days=365)
    if start_date >= end_date:
        return jsonify({"error": "Start date must be before end date."}), 400

    ckey = "bt_" + "|".join([name.lower(), start_date.isoformat(), end_date.isoformat(), str(int(capital))])
    cached = CACHE.get(ckey)
    if cached and (time.time() - cached[1]) < 3600:
        return jsonify(cached[0])

    all_trades = get_all_congress_trades()
    if not all_trades:
        return jsonify({"error": "Congressional trade data is unavailable right now. Try again shortly."}), 503

    nlow = name.lower()
    purchases = []
    for t in all_trades:
        if _ctrade_name(t).lower() != nlow:
            continue
        if "purchase" not in _ctrade_action(t).lower():
            continue
        sym = _ctrade_ticker(t)
        if not sym:
            continue
        try:
            td = datetime.strptime(_ctrade_date(t), "%Y-%m-%d").date()
        except ValueError:
            continue
        if td < start_date or td > end_date:
            continue
        purchases.append({"symbol": sym, "date": td})

    if not purchases:
        return jsonify({"error": "No purchases found for %s between %s and %s." % (name, start_date.isoformat(), end_date.isoformat())}), 404

    purchases.sort(key=lambda p: p["date"])
    n = len(purchases)
    per = capital / float(n)

    # SPY supplies the canonical trading calendar and the benchmark line.
    fetch_end = end_date + timedelta(days=1)  # yfinance end is exclusive
    spy_pairs = _price_pairs("SPY", start_date, fetch_end)
    if not spy_pairs:
        return jsonify({"error": "Could not load S&P 500 prices for that window."}), 503
    spy_dates = [d for d, _ in spy_pairs]
    spy_map = dict(spy_pairs)
    spy_first = spy_pairs[0][1]
    spy_last = spy_pairs[-1][1]

    tickers = sorted(set(p["symbol"] for p in purchases))
    hist = {}
    for tk in tickers:
        hist[tk] = dict(_price_pairs(tk, start_date, fetch_end))

    trades_out = []
    active = []
    for p in purchases:
        tmap = hist.get(p["symbol"]) or {}
        buy_price = None
        buy_d = None
        for d in spy_dates:
            if d < p["date"]:
                continue
            if d in tmap:
                buy_price = tmap[d]
                buy_d = d
                break
        if buy_price is None or buy_price <= 0 or buy_d is None:
            continue
        shares = per / buy_price
        cur_price = None
        for d in reversed(spy_dates):
            if d in tmap:
                cur_price = tmap[d]
                break
        if cur_price is None:
            cur_price = buy_price
        p["buy_price"] = buy_price
        p["buy_date"] = buy_d
        p["shares"] = shares
        active.append(p)
        trades_out.append({
            "symbol": p["symbol"],
            "date": buy_d.isoformat(),
            "shares": round(shares, 4),
            "buy_price": round(buy_price, 2),
            "current_price": round(cur_price, 2),
            "current_value": round(shares * cur_price, 2),
            "return_pct": round((cur_price / buy_price - 1.0) * 100.0, 2),
        })

    if not active:
        return jsonify({"error": "Found purchases for %s but none had usable price history in that window." % name}), 404

    # Time series. The portfolio starts as the full capital in cash, and each pick deploys an equal
    # slice at its trade date, marked to market daily and forward filled. This keeps it dollar for
    # dollar comparable to the S&P line, which is fully invested from day one.
    chart = []
    last_price = {tk: None for tk in tickers}
    for d in spy_dates:
        for tk in tickers:
            if d in hist[tk]:
                last_price[tk] = hist[tk][d]
        deployed = 0
        holdings = 0.0
        for p in active:
            if p["buy_date"] <= d:
                deployed += 1
                lp = last_price.get(p["symbol"])
                if lp is not None:
                    holdings += p["shares"] * lp
        cash = capital - per * deployed
        if cash < 0:
            cash = 0.0
        port = cash + holdings
        spx_val = capital * (spy_map[d] / spy_first)
        chart.append({"date": d.isoformat(), "portfolio_value": round(port, 2), "sp500_value": round(spx_val, 2)})

    final_port = chart[-1]["portfolio_value"] if chart else capital
    total_return = (final_port / capital - 1.0) * 100.0
    sp500_return = (spy_last / spy_first - 1.0) * 100.0

    result = {
        "politician": name,
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "capital": round(capital, 2),
        "trades": trades_out,
        "chart_data": chart,
        "total_return": round(total_return, 2),
        "sp500_return": round(sp500_return, 2),
        "purchases_found": n,
        "purchases_simulated": len(active),
    }
    set_cache(ckey, result)
    return jsonify(result)


@app.route("/congress/politician")
def congress_politician():
    name = (request.args.get("name") or "").strip()
    if not name:
        return jsonify({"error": "no_name"}), 400
    ckey = "congress_pol_" + name
    cached = CACHE.get(ckey)
    if cached and (time.time() - cached[1]) < 3600:
        return jsonify(cached[0])
    trades = get_all_congress_trades()
    pol_trades = [t for t in trades if _ctrade_name(t) == name]
    if not pol_trades:
        return jsonify({"error": "not_found", "politician": {"name": name}, "portfolio": []}), 404
    party, state = "", ""
    for t in pol_trades:
        if not party:
            party = str(t.get("Party") or "").strip()
        if not state:
            state = str(t.get("State") or "").strip()
    by_tk = {}
    for t in pol_trades:
        tk = _ctrade_ticker(t)
        if not tk:
            continue
        g = by_tk.get(tk)
        if g is None:
            g = {"count": 0, "recent_action": "", "recent_date": ""}
            by_tk[tk] = g
        g["count"] += 1
        d = _ctrade_date(t)
        if d > g["recent_date"]:
            g["recent_date"] = d
            g["recent_action"] = _ctrade_action(t)
    # Cap to the 30 most recently traded tickers so a heavy trader's page stays responsive.
    tickers_sorted = sorted(by_tk.items(), key=lambda kv: kv[1]["recent_date"], reverse=True)[:30]
    portfolio = []
    for tk, g in tickers_sorted:
        r = light_score(tk)
        if not r:
            continue
        portfolio.append({
            "symbol": tk,
            "name": r.get("name", tk),
            "price": r.get("price"),
            "change_pct": r.get("change_pct"),
            "market_cap": r.get("market_cap"),
            "analyst_target": r.get("analyst_target"),
            "verdict": r.get("verdict"),
            "trade_count": g["count"],
            "recent_action": g["recent_action"],
            "recent_date": g["recent_date"],
        })
    rc = CACHE.get("congress_returns_" + name)
    ret = rc[0] if rc else None
    payload = {
        "politician": {"name": name, "party": party, "state": state, "trade_count": len(pol_trades), "photo_url": None, "returns": ret},
        "portfolio": portfolio,
    }
    set_cache(ckey, payload)
    return jsonify(payload)


def compare_reason(best, items):
    v = best.get("verdict", "WATCH")
    msg = best["symbol"] + " looks strongest in this group. "
    descr = {
        "APPROVE": "the engine leans positive on it",
        "WATCH": "the engine holds it at watch, but it scores above the others here",
        "PASS": "the engine is cautious on it, yet it still scores the least weak of the group",
    }
    dtext = descr.get(v, "it scores highest of the group")
    msg += dtext[0].upper() + dtext[1:] + ". "
    # CHUNK: only genuine positives count as reasons it is strongest. A down day is a caveat,
    # never a reason, so it is framed honestly instead of being listed as a plus.
    extras = []
    caveats = []
    up = best.get("upside")
    chg = best.get("change_pct")
    if isinstance(up, (int, float)) and up > 0:
        extras.append("analysts see about %s%% upside to its average target" % up)
    if isinstance(chg, (int, float)) and chg > 0:
        extras.append("it is up %s%% today" % chg)
    try:
        pe_v = round(float(best.get("pe_ratio")), 1)
        if pe_v <= 30:
            extras.append("it trades at a reasonable %s times earnings" % pe_v)
        elif pe_v > 45:
            caveats.append("its valuation is rich at about %s times earnings" % pe_v)
    except (TypeError, ValueError):
        pass
    if isinstance(chg, (int, float)) and chg < 0:
        caveats.append("it is actually down %s%% today, so the edge here is in its other signals, not today's move" % abs(chg))
    if extras:
        msg += "Among the reasons: " + ", ".join(extras) + ". "
    if caveats:
        msg += "Worth noting: " + ", and ".join(caveats) + ". "
    msg += "This weighs the same signals you see in each full report."
    return msg


@app.route("/compare")
def compare():
    raw = request.args.get("symbols", "")
    syms = [s.strip().upper() for s in raw.split(",") if s.strip()][:3]
    items = []
    warnings = []
    for s in syms:
        # CHUNK: Ask/Compare name resolution — let each box accept a company name
        sym = s
        if not looks_like_ticker(sym):
            sym = resolve_ticker(sym).upper()
        r = light_score(sym)
        if not r and sym != s:
            r = light_score(s)
        if r:
            items.append(r)
        else:
            warnings.append(s)
    strongest = None
    reason = ""
    if items:
        stock_items = [r for r in items if r.get("verdict") != "ETF"]
        etf_count = len(items) - len(stock_items)
        if stock_items:
            best = max(stock_items, key=lambda r: (r.get("score", 0), r.get("upside") or 0, r.get("change_pct") or 0))
            strongest = best["symbol"]
            reason = compare_reason(best, stock_items)
            if etf_count:
                reason += " The ETFs here are shown for context but are not ranked by the stock engine, since a fund is judged on what it costs to own and what it holds, not these signals."
        else:
            reason = ("These are all exchange traded funds. Apex Q does not rank funds with the stock engine, because a fund is judged on what it costs to own and what it holds. "
                      "Open each one for its expense ratio, top holdings, and sector mix.")
    return jsonify({"items": items, "strongest": strongest, "reason": reason, "warnings": warnings})


# CHUNK: Feature 3 — suggest peers in the same sector for the Compare tab. Sector does not change,
# so the answer is cached. Falls back to a default large-cap group when the sector is unknown.
PEERS_FALLBACK = ["AAPL", "MSFT", "NVDA", "AMZN"]


@app.route("/peers")
def peers():
    symbol = request.args.get("symbol", "").strip().upper()
    if not symbol:
        return jsonify({"peers": []})
    cached = get_cache("peers_" + symbol)
    if cached is not None:
        return jsonify({"peers": cached})
    sector = None
    try:
        _info = yf.Ticker(symbol).info or {}
        if str(_info.get("quoteType", "")).upper() == "ETF":
            set_cache("peers_" + symbol, [])
            return jsonify({"peers": []})
        sector = _info.get("sector") or _info.get("industry")
    except Exception as e:
        logger.error("peers sector lookup %s: %s" % (symbol, e))
    out = []
    if sector:
        for s in SCAN_UNIVERSE:
            if s == symbol:
                continue
            try:
                si = (yf.Ticker(s).info or {}).get("sector")
            except Exception:
                si = None
            if si and si == sector:
                out.append(s)
            if len(out) >= 4:
                break
    if not out:
        out = [s for s in PEERS_FALLBACK if s != symbol][:4]
    set_cache("peers_" + symbol, out)
    return jsonify({"peers": out})


_MOVERS = {"data": None, "ts": 0}


def get_movers_cached():
    # Biggest market gainers and decliners, refreshed at most every 30 minutes and shared by the
    # Home dashboard, the Discover tab, and the /movers route so all three show the same live data.
    # Primary source is FMP; if that is empty we derive movers from the universe we already score.
    now = time.time()
    if _MOVERS["data"] is not None and now - _MOVERS["ts"] < 1800:
        return _MOVERS["data"]

    def pct(v):
        try:
            return round(float(str(v).replace("%", "").replace("(", "-").replace(")", "").strip()), 2)
        except Exception:
            return None

    def grab(stable, legacy):
        # FMP moved these to the /stable path and marked the old /api/v3 ones legacy, so try the
        # current endpoint first and fall back to the legacy one if a key still maps to it.
        data = fmp_get(stable)
        if not isinstance(data, list) or not data:
            data = fmp_get(legacy)
        out = []
        if isinstance(data, list):
            for d in data[:10]:
                sym = d.get("symbol")
                if not sym or len(sym) > 6:
                    continue
                out.append({
                    "symbol": sym,
                    "name": d.get("name") or sym,
                    "change_pct": pct(d.get("changesPercentage") or d.get("changePercentage")),
                    "price": d.get("price"),
                })
        return out

    gainers = grab("/stable/biggest-gainers", "/api/v3/stock_market/gainers")
    losers = grab("/stable/biggest-losers", "/api/v3/stock_market/losers")

    # If FMP gives us nothing (legacy plan, daily cap, or a changed response shape), derive movers
    # from the universe we already score. Not the whole market, but always real and tappable.
    if not gainers and not losers:
        rows = [r for r in scan_universe() if isinstance(r.get("change_pct"), (int, float))]
        if rows:
            def mv(r):
                return {"symbol": r["symbol"], "name": r.get("name") or r["symbol"], "change_pct": r["change_pct"], "price": r.get("price")}
            up = sorted(rows, key=lambda r: r["change_pct"], reverse=True)
            down = sorted(rows, key=lambda r: r["change_pct"])
            gainers = [mv(r) for r in up if r["change_pct"] > 0][:5]
            losers = [mv(r) for r in down if r["change_pct"] < 0][:5]

    out = {"gainers": gainers, "losers": losers, "data_timestamp": int(time.time())}
    # Only cache a real result, so a transient FMP miss does not stick for half an hour.
    if gainers or losers:
        _MOVERS["data"] = out
        _MOVERS["ts"] = now
    return out


@app.route("/movers")
def movers():
    # The biggest gainers and decliners across the whole market, pulled live and refreshed every
    # half hour. Surfaces names well beyond the usual large caps, which fits the Discover idea.
    return jsonify(get_movers_cached())


def _alert_order(a):
    if a.get("flip"):
        return 0
    return 1 if a["kind"] == "caution" else 2


def build_alerts(uid):
    # Shared by /alerts and /dashboard. Scores each saved stock with the full report so the verdict
    # and its reason match the report view exactly, detects unacknowledged verdict flips read only,
    # and returns the same payload shape /alerts has always returned. Cached per symbol, and
    # watchlists are small, so the overlap with the dashboard pass stays cheap after the first warm.
    conn = get_db()
    if conn is None:
        return {"status": "error", "alerts": []}
    try:
        cur = conn.cursor()
        cur.execute("SELECT symbol, name FROM watchlist WHERE user_id = %s ORDER BY added_at DESC", (uid,))
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        logger.error("alerts list error: %s" % e)
        return {"status": "error", "alerts": []}
    finally:
        conn.close()

    if not rows:
        return {"status": "empty", "alerts": [], "total_saved": 0, "verdict_changes": 0}

    out = []
    for sym, nm in rows:
        r = compute_full_report(sym)
        if not r:
            continue
        chg = r.get("change_pct")
        v = r.get("verdict")
        name = r.get("name") or nm or sym
        # Verdict flip takes priority. This is read only for an existing baseline, so a flip keeps
        # showing in the feed until the user opens that stock's report and the frontend acknowledges
        # it. First sight establishes the baseline silently with no flip.
        prev = read_verdict(uid, sym)
        if prev is None:
            set_verdict(uid, sym, v)
            changed = False
        else:
            changed = (v != "ETF" and prev != v)
        if changed:
            out.append({
                "kind": "positive" if v == "APPROVE" else "caution",
                "flip": True,
                "previous_verdict": prev,
                "new_verdict": v,
                "verdict": v,
                "reason": r.get("verdict_signal_reason") or "the balance of signals shifted",
                "symbol": sym,
                "name": name,
                "change_pct": chg,
            })
            continue
        alert = None
        if isinstance(chg, (int, float)) and chg <= -5:
            alert = {"kind": "caution", "reason": "Down %s%% today, a sharp move worth checking." % abs(chg)}
        elif v == "PASS":
            alert = {"kind": "caution", "reason": "The engine has turned cautious on it. Open the full report for why."}
        elif v == "APPROVE":
            alert = {"kind": "positive", "reason": "The engine currently leans positive on it."}
        elif isinstance(chg, (int, float)) and chg >= 5:
            alert = {"kind": "positive", "reason": "Up %s%% today, a notable move." % chg}
        if alert:
            alert.update({"symbol": sym, "name": name, "change_pct": chg, "verdict": v})
            out.append(alert)

    out.sort(key=_alert_order)
    flips = sum(1 for a in out if a.get("flip"))
    return {"status": "ok", "alerts": out, "total_saved": len(rows), "verdict_changes": flips}


# ---------- Custom Alert Engine ----------
# Users build personalized, strictly educational alerts on congressional and insider activity,
# verdict and alpha changes, price crossings, 52 week extremes, new filings, and earnings. Every
# triggered alert carries a plain English reason and links to the relevant report. Delivery is
# best effort: a OneSignal push when a device token exists and an email row in the existing queue.
# No alert ever contains trading advice, only factual observation.

ALERT_LABELS = {
    "congress_buy": "When a politician buys a stock",
    "congress_sell": "When a politician sells a stock",
    "insider_buy": "When a company insider buys",
    "insider_sell": "When a company insider sells",
    "verdict_change": "When a stock's verdict changes",
    "price_cross": "When a stock crosses a price",
    "earnings_tomorrow": "When earnings are coming up",
    "alpha_change": "When a stock's Alpha Score shifts sharply",
    "52week_high": "When a stock hits a 52 week high",
    "52week_low": "When a stock hits a 52 week low",
    "filing_new": "When a new SEC filing appears",
}


def _onesignal_push(token, title, body, url):
    app_id = os.environ.get("ONESIGNAL_APP_ID", "").strip()
    api_key = os.environ.get("ONESIGNAL_API_KEY", "").strip()
    if not app_id or not api_key or not token:
        return
    try:
        requests.post(
            "https://onesignal.com/api/v1/notifications",
            headers={"Authorization": "Basic " + api_key, "Content-Type": "application/json"},
            json={
                "app_id": app_id,
                "include_player_ids": [token],
                "headings": {"en": title},
                "contents": {"en": body},
                "url": url,
            },
            timeout=10,
        )
    except Exception as e:
        logger.error("onesignal push: %s" % e)


def _deliver_alert(conn, user, alert_type, symbol, message, reason, link):
    """Log the trigger, then push and email best effort. Deduped so the same alert does not repeat
    within 24 hours for the same user, type, and symbol."""
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM alert_log WHERE user_id=%s AND alert_type=%s AND COALESCE(symbol,'')=%s "
        "AND triggered_at > NOW() - INTERVAL '24 hours' LIMIT 1",
        (user["id"], alert_type, symbol or ""),
    )
    if cur.fetchone():
        cur.close()
        return False
    cur.execute(
        "INSERT INTO alert_log (user_id, alert_type, symbol, message, reason, link) "
        "VALUES (%s,%s,%s,%s,%s,%s)",
        (user["id"], alert_type, symbol, message, reason, link),
    )
    token = user.get("onesignal_token")
    if token:
        _onesignal_push(token, message, reason, "https://www.apexq.io" + (link or ""))
    email = user.get("email")
    if email:
        cur.execute(
            "INSERT INTO email_queue (user_id, subject, body) VALUES (%s,%s,%s)",
            (user["id"], "Apex Q Alert: " + message, message + "\n\n" + reason +
             "\n\nThis is an educational notification, not trading advice."),
        )
    conn.commit()
    cur.close()
    return True


def check_alerts_for_symbol(user_id, symbol, report=None):
    """Run the enabled alerts that involve this symbol against fresh data. Called opportunistically
    from analyze so alerts evaluate as users browse, with no separate cron required. Silent and
    defensive: any failure is logged and skipped, never surfaced to the request."""
    conn = get_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, email, COALESCE(onesignal_token,'') FROM users WHERE id=%s", (user_id,))
        urow = cur.fetchone()
        if not urow:
            cur.close(); conn.close(); return
        user = {"id": urow[0], "email": urow[1], "onesignal_token": urow[2] or None}
        sym = (symbol or "").upper()
        cur.execute(
            "SELECT id, alert_type, symbol, politician_name, threshold_price, direction "
            "FROM user_alerts WHERE user_id=%s AND enabled=true AND "
            "(symbol IS NULL OR UPPER(symbol)=%s)",
            (user_id, sym),
        )
        rules = cur.fetchall()
        cur.close()
        if not rules:
            conn.close(); return
        r = report or compute_full_report(sym)
        if not r:
            conn.close(); return
        price = r.get("price")
        verdict = r.get("verdict")
        name = r.get("name") or sym
        for rule in rules:
            aid, atype, asym, pol, thresh, direction = rule
            try:
                if atype == "price_cross" and price is not None and thresh is not None:
                    t = float(thresh)
                    if direction == "above" and price >= t:
                        _deliver_alert(conn, user, atype, sym,
                            "Price Alert: %s crossed above $%s" % (sym, t),
                            "%s is trading at $%s, above your $%s mark. Filed for your awareness only." % (name, price, t),
                            "/?symbol=" + sym)
                    elif direction == "below" and price <= t:
                        _deliver_alert(conn, user, atype, sym,
                            "Price Alert: %s crossed below $%s" % (sym, t),
                            "%s is trading at $%s, below your $%s mark. Filed for your awareness only." % (name, price, t),
                            "/?symbol=" + sym)
                elif atype == "verdict_change" and verdict:
                    cur2 = conn.cursor()
                    cur2.execute("SELECT last_verdict FROM verdict_history WHERE user_id=%s AND symbol=%s", (user_id, sym))
                    prev = cur2.fetchone()
                    cur2.close()
                    if prev and prev[0] and prev[0] != verdict:
                        _deliver_alert(conn, user, atype, sym,
                            "Verdict Change: %s is now %s" % (sym, verdict),
                            "%s moved from %s to %s in the engine. What changed is worth a look." % (name, prev[0], verdict),
                            "/?symbol=" + sym)
                elif atype == "52week_high" and price is not None:
                    hi = r.get("week52_high") or r.get("fifty_two_week_high")
                    if hi and price >= float(hi) * 0.999:
                        _deliver_alert(conn, user, atype, sym,
                            "52 Week High: %s" % sym,
                            "%s touched a fresh 52 week high at $%s. A notable level, shared for context only." % (name, price),
                            "/?symbol=" + sym)
                elif atype == "52week_low" and price is not None:
                    lo = r.get("week52_low") or r.get("fifty_two_week_low")
                    if lo and price <= float(lo) * 1.001:
                        _deliver_alert(conn, user, atype, sym,
                            "52 Week Low: %s" % sym,
                            "%s touched a fresh 52 week low at $%s. A notable level, shared for context only." % (name, price),
                            "/?symbol=" + sym)
                elif atype in ("congress_buy", "congress_sell"):
                    cong = r.get("congressional")
                    trades = cong.get("trades") if isinstance(cong, dict) else (cong if isinstance(cong, list) else [])
                    want = "purchase" if atype == "congress_buy" else "sale"
                    for tr in (trades or [])[:10]:
                        tt = (tr.get("transaction") or tr.get("type") or "").lower()
                        who = tr.get("politician") or tr.get("name") or "A member of Congress"
                        if want in tt and (not pol or pol.lower() in who.lower()):
                            verb = "bought" if atype == "congress_buy" else "sold"
                            _deliver_alert(conn, user, atype, sym,
                                "Congress Alert: %s %s %s" % (who, verb, sym),
                                "A disclosure shows %s %s %s. Congressional trades are a policy signal, not a recommendation." % (who, verb, name),
                                "/?symbol=" + sym)
                            break
                elif atype in ("insider_buy", "insider_sell"):
                    ins = r.get("insider")
                    itrades = ins.get("trades") if isinstance(ins, dict) else (ins if isinstance(ins, list) else [])
                    want = "buy" if atype == "insider_buy" else "sell"
                    for tr in (itrades or [])[:10]:
                        tt = (tr.get("transaction") or tr.get("type") or "").lower()
                        if want in tt or (want == "buy" and "purchase" in tt):
                            verb = "buying" if atype == "insider_buy" else "selling"
                            _deliver_alert(conn, user, atype, sym,
                                "Insider Alert: %s at %s" % (verb.title(), sym),
                                "A company insider was %s %s. Insiders know their business, though their reasons vary." % (verb, name),
                                "/?symbol=" + sym)
                            break
                elif atype == "filing_new":
                    filings = r.get("sec_filings")
                    if filings and isinstance(filings, list) and filings:
                        _deliver_alert(conn, user, atype, sym,
                            "New Filing: %s" % sym,
                            "%s has a recent SEC filing on record. Primary source documents are where the real story lives." % name,
                            "/?symbol=" + sym)
                elif atype == "alpha_change":
                    alpha = r.get("alpha_score")
                    if alpha is not None:
                        _deliver_alert(conn, user, atype, sym,
                            "Alpha Shift: %s" % sym,
                            "%s now carries an Alpha Score of %s. Worth seeing what moved it." % (name, alpha),
                            "/?symbol=" + sym)
                elif atype == "earnings_tomorrow":
                    ed = r.get("earnings_date") or r.get("next_earnings")
                    if ed:
                        _deliver_alert(conn, user, atype, sym,
                            "Earnings Ahead: %s" % sym,
                            "%s has earnings coming up on %s. Volatility often rises around reports." % (name, ed),
                            "/?symbol=" + sym)
            except Exception as e:
                logger.error("alert rule %s: %s" % (aid, e))
        conn.close()
    except Exception as e:
        logger.error("check_alerts_for_symbol: %s" % e)
        try:
            conn.close()
        except Exception:
            pass


# ---------- Morning Briefing agent ----------
# A cron triggered agent that, once per trading morning, scans each opted in user's watchlist and
# brokerage holdings for overnight congressional trades, insider filings, and upcoming earnings,
# then writes a neutral plain English briefing via DeepSeek and delivers it by push and email.
# Idempotent per user per day via the morning_briefings unique key. Token protected, no login.

def _sector_bucket(sec):
    """Collapse messy sector strings into the eleven GICS style buckets used across the app."""
    if not sec:
        return "Other"
    s = sec.lower()
    if "tech" in s: return "Technology"
    if "health" in s or "pharma" in s: return "Healthcare"
    if "financ" in s or "bank" in s: return "Financials"
    if "energy" in s or "oil" in s: return "Energy"
    if "industr" in s or "defense" in s: return "Industrials"
    if "consumer" in s and ("staple" in s or "defensive" in s): return "Consumer Staples"
    if "consumer" in s: return "Consumer Discretionary"
    if "material" in s: return "Materials"
    if "utilit" in s: return "Utilities"
    if "real estate" in s or "reit" in s: return "Real Estate"
    if "commun" in s or "telecom" in s: return "Communication Services"
    return "Other"


def _briefing_for_user(conn, user, today_str):
    """Build and store one user's briefing. Returns the text, or None if nothing notable."""
    uid = user["id"]
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM watchlist WHERE user_id=%s", (uid,))
    syms = set(row[0].upper() for row in cur.fetchall() if row[0])
    cur.execute("SELECT DISTINCT symbol FROM holdings WHERE user_id=%s", (uid,))
    for row in cur.fetchall():
        if row[0]:
            syms.add(row[0].upper())
    cur.close()
    if not syms:
        return None
    events = []
    for sym in list(syms)[:25]:
        try:
            r = compute_full_report(sym)
        except Exception:
            r = None
        if not r:
            continue
        name = r.get("name") or sym
        cong = r.get("congressional")
        ctrades = cong.get("trades") if isinstance(cong, dict) else (cong if isinstance(cong, list) else [])
        if ctrades:
            hi = [t for t in ctrades if t.get("high_clout")]
            if hi:
                events.append("%s (%s): a high clout congressional trade was disclosed." % (sym, name))
            else:
                events.append("%s (%s): recent congressional trading activity on record." % (sym, name))
        ins = r.get("insider")
        itrades = ins.get("trades") if isinstance(ins, dict) else (ins if isinstance(ins, list) else [])
        if itrades:
            events.append("%s (%s): recent insider filing activity." % (sym, name))
        if r.get("earnings_flag") or r.get("earnings_soon"):
            events.append("%s (%s): earnings are coming up." % (sym, name))
    if not events:
        return None
    body = None
    if DEEPSEEK_KEY:
        try:
            guard = ("You are Apex Q Intelligence, a strictly objective, non advisory market data "
                     "processing machine. You are legally forbidden from providing financial advice, "
                     "buy or sell directives, or predictions. ")
            prompt = (guard + "You are a neutral data reporter. Summarize the following overnight events "
                      "for the user's stocks as short plain English bullets. Do not give advice. Do not use "
                      "dashes. Events: " + " | ".join(events))
            headers = {"Authorization": "Bearer " + DEEPSEEK_KEY, "Content-Type": "application/json"}
            payload = {"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}],
                       "temperature": 0.2, "max_tokens": 400}
            resp = requests.post("https://api.deepseek.com/chat/completions", headers=headers, json=payload, timeout=20)
            if resp.status_code == 200:
                body = resp.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            logger.error("briefing ai: %s" % e)
    if not body:
        body = "Overnight on your stocks:\n" + "\n".join("- " + e for e in events)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO morning_briefings (user_id, date, content) VALUES (%s,%s,%s) "
        "ON CONFLICT (user_id, date) DO UPDATE SET content=EXCLUDED.content", (uid, today_str, body))
    set_cached_ai("morning_briefing_%s_%s" % (uid, today_str), body)
    if user.get("onesignal_token"):
        _onesignal_push(user["onesignal_token"], "Your Apex Q Morning Briefing", body[:180], "https://www.apexq.io")
    if user.get("email"):
        cur.execute("INSERT INTO email_queue (user_id, subject, body) VALUES (%s,%s,%s)",
                    (uid, "Your Apex Q Morning Briefing",
                     body + "\n\nEducational research framework only. Not personalized financial advice."))
    conn.commit()
    cur.close()
    return body


@app.route("/signals/active-catalysts")
def signals_active_catalysts():
    cached = CACHE.get("active_catalysts")
    if cached and (time.time() - cached[1]) < 300:
        return jsonify(cached[0])
    conn = get_db()
    if conn is None:
        return jsonify({"catalysts": []})
    try:
        cur = conn.cursor()
        cur.execute("SELECT ticker, company_name, total_capital_deployed, executive_roles, "
                    "window_start_date, unique_insiders_count FROM insider_clusters "
                    "WHERE is_resolved=false AND unique_insiders_count >= 3 "
                    "ORDER BY unique_insiders_count DESC, created_at DESC LIMIT 40")
        out = []
        for row in cur.fetchall():
            cnt = row[5]
            out.append({
                "ticker": row[0], "company_name": row[1] or row[0],
                "total_capital_deployed": float(row[2] or 0),
                "executive_roles": row[3] or [],
                "window_start_date": row[4].isoformat() if row[4] else None,
                "unique_insiders_count": cnt,
                "severity": "CRITICAL" if cnt >= 5 else "HIGH",
            })
        cur.close(); conn.close()
        payload = {"catalysts": out}
        CACHE["active_catalysts"] = (payload, time.time())
        return jsonify(payload)
    except Exception as e:
        logger.error("active_catalysts: %s" % e)
        try:
            conn.close()
        except Exception:
            pass
        return jsonify({"catalysts": []})


@app.route("/cron/morning-briefing")
def cron_morning_briefing():
    secret = os.environ.get("CRON_SECRET", "").strip()
    if secret and request.args.get("token", "") != secret:
        return jsonify({"error": "unauthorized"}), 403
    conn = get_db()
    if conn is None:
        return jsonify({"error": "db"}), 500
    import datetime as _dt
    today_str = _dt.date.today().isoformat()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT u.id, u.email, COALESCE(u.onesignal_token,'') FROM users u "
        "WHERE COALESCE(u.agreed_tos,false)=true AND ("
        "EXISTS (SELECT 1 FROM watchlist w WHERE w.user_id=u.id) OR "
        "EXISTS (SELECT 1 FROM holdings h WHERE h.user_id=u.id))")
    users = [{"id": r[0], "email": r[1], "onesignal_token": r[2] or None} for r in cur.fetchall()]
    cur.close()
    done = 0
    for user in users:
        try:
            c2 = conn.cursor()
            c2.execute("SELECT 1 FROM morning_briefings WHERE user_id=%s AND date=%s", (user["id"], today_str))
            already = c2.fetchone()
            c2.close()
            if already:
                continue
            if _briefing_for_user(conn, user, today_str):
                done += 1
        except Exception as e:
            logger.error("briefing user %s: %s" % (user["id"], e))
    conn.close()
    return jsonify({"ok": True, "briefed": done, "eligible": len(users)})


@app.route("/briefing/latest")
def briefing_latest():
    u = current_user()
    if not u:
        return jsonify({"briefing": None})
    conn = get_db()
    if conn is None:
        return jsonify({"briefing": None})
    cur = conn.cursor()
    cur.execute("SELECT date, content, created_at FROM morning_briefings WHERE user_id=%s "
                "ORDER BY created_at DESC LIMIT 1", (u["id"],))
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row:
        return jsonify({"briefing": None})
    return jsonify({"briefing": {"date": row[0], "content": row[1],
                                 "created_at": row[2].isoformat() if row[2] else None}})


def _reference_sector_weights(gics):
    """Aggregate sector weights of recent congressional and insider purchases across the scan
    universe, a fast proxy for what the two groups are buying. Both normalized to percentages."""
    cong = dict((s, 0.0) for s in gics)
    ins = dict((s, 0.0) for s in gics)
    for sym in SCAN_UNIVERSE:
        try:
            r = compute_full_report(sym)
        except Exception:
            r = None
        if not r:
            continue
        b = _sector_bucket(r.get("sector"))
        if b not in cong:
            continue
        c = r.get("congressional")
        ct = c.get("trades") if isinstance(c, dict) else (c if isinstance(c, list) else [])
        cong[b] += len([t for t in (ct or []) if "purchase" in str(t.get("action", t.get("transaction", ""))).lower()])
        i = r.get("insider")
        it = i.get("trades") if isinstance(i, dict) else (i if isinstance(i, list) else [])
        ins[b] += len([t for t in (it or []) if "buy" in str(t.get("transaction", t.get("type", ""))).lower() or "purchase" in str(t.get("transaction", t.get("type", ""))).lower()])
    ct_total = sum(cong.values()) or 1
    it_total = sum(ins.values()) or 1
    cong_pct = dict((s, round(cong[s] / ct_total * 100, 1)) for s in gics)
    ins_pct = dict((s, round(ins[s] / it_total * 100, 1)) for s in gics)
    return cong_pct, ins_pct


@app.route("/portfolio/drift")
def portfolio_drift():
    u = current_user()
    if not u:
        return jsonify({"connected": False, "error": "not_logged_in"})
    agg, accounts, err = _snaptrade_agg(u)
    if err == "not_connected" or not agg:
        return jsonify({"connected": False})
    if err:
        return jsonify({"connected": True, "error": err}), 502
    GICS = ["Technology", "Healthcare", "Financials", "Energy", "Industrials",
            "Consumer Discretionary", "Consumer Staples", "Materials", "Utilities",
            "Real Estate", "Communication Services"]
    port = dict((s, 0.0) for s in GICS)
    total = 0.0
    for sym, h in agg.items():
        try:
            r = light_score(sym) or {}
            price = float(r.get("price") or h.get("broker_price") or 0)
            val = price * h["shares"]
            b = _sector_bucket(r.get("sector"))
            if b in port:
                port[b] += val
                total += val
        except Exception:
            continue
    port_pct = dict((s, round(port[s] / total * 100, 1) if total else 0.0) for s in GICS)
    ref = CACHE.get("drift_ref")
    if ref and (time.time() - ref[1]) < 3600:
        cong_pct, ins_pct = ref[0]
    else:
        cong_pct, ins_pct = _reference_sector_weights(GICS)
        CACHE["drift_ref"] = ((cong_pct, ins_pct), time.time())
    diffs = []
    for s in GICS:
        diffs.append((s, port_pct[s] - cong_pct.get(s, 0), port_pct[s] - ins_pct.get(s, 0)))
    diffs.sort(key=lambda x: abs(x[1]), reverse=True)
    lead = diffs[0]
    summary = ""
    if abs(lead[1]) >= 1:
        more = "more" if lead[1] > 0 else "less"
        summary = "Your portfolio is %.0f%% %s exposed to %s than the average Congressional buyer." % (abs(lead[1]), more, lead[0])
        ins_lead = max(diffs, key=lambda x: abs(x[2]))
        if abs(ins_lead[2]) >= 1:
            more2 = "more" if ins_lead[2] > 0 else "less"
            summary += " You are %.0f%% %s exposed to %s than corporate insiders." % (abs(ins_lead[2]), more2, ins_lead[0])
        summary += " This is purely observational, not advice."
    return jsonify({"connected": True, "sectors": GICS,
                    "portfolio": [port_pct[s] for s in GICS],
                    "congress": [cong_pct.get(s, 0) for s in GICS],
                    "insiders": [ins_pct.get(s, 0) for s in GICS],
                    "summary": summary})



@app.route("/alerts/custom")
def alerts_custom_list():
    u = current_user()
    if not u:
        return jsonify({"alerts": []})
    conn = get_db()
    if conn is None:
        return jsonify({"alerts": []})
    cur = conn.cursor()
    cur.execute(
        "SELECT id, alert_type, symbol, politician_name, threshold_price, direction, enabled, created_at "
        "FROM user_alerts WHERE user_id=%s ORDER BY created_at DESC", (u["id"],))
    out = []
    for row in cur.fetchall():
        out.append({
            "id": row[0], "alert_type": row[1], "label": ALERT_LABELS.get(row[1], row[1]),
            "symbol": row[2], "politician_name": row[3],
            "threshold_price": float(row[4]) if row[4] is not None else None,
            "direction": row[5], "enabled": row[6],
        })
    cur.close(); conn.close()
    return jsonify({"alerts": out})


@app.route("/alerts/create", methods=["POST"])
def alerts_create():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    b = request.get_json(silent=True) or {}
    atype = (b.get("alert_type") or "").strip()
    if atype not in ALERT_LABELS:
        return jsonify({"error": "Unknown alert type."}), 400
    symbol = (b.get("symbol") or "").strip().upper() or None
    politician = (b.get("politician_name") or "").strip() or None
    direction = (b.get("direction") or "").strip().lower() or None
    if direction and direction not in ("above", "below"):
        direction = None
    thresh = None
    if b.get("threshold_price") not in (None, ""):
        try:
            thresh = float(b.get("threshold_price"))
        except (TypeError, ValueError):
            return jsonify({"error": "Price must be a number."}), 400
    if atype == "price_cross" and (thresh is None or not symbol or not direction):
        return jsonify({"error": "Price alerts need a symbol, a price, and above or below."}), 400
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database unavailable."}), 500
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO user_alerts (user_id, alert_type, symbol, politician_name, threshold_price, direction) "
        "VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
        (u["id"], atype, symbol, politician, thresh, direction))
    aid = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True, "id": aid})


@app.route("/alerts/delete", methods=["POST"])
def alerts_delete():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    b = request.get_json(silent=True) or {}
    aid = b.get("id")
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database unavailable."}), 500
    cur = conn.cursor()
    cur.execute("DELETE FROM user_alerts WHERE id=%s AND user_id=%s", (aid, u["id"]))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@app.route("/alerts/toggle", methods=["POST"])
def alerts_toggle():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    b = request.get_json(silent=True) or {}
    aid = b.get("id")
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database unavailable."}), 500
    cur = conn.cursor()
    cur.execute("UPDATE user_alerts SET enabled = NOT enabled WHERE id=%s AND user_id=%s RETURNING enabled", (aid, u["id"]))
    row = cur.fetchone()
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True, "enabled": row[0] if row else None})


@app.route("/alerts/log")
def alerts_log():
    u = current_user()
    if not u:
        return jsonify({"log": []})
    conn = get_db()
    if conn is None:
        return jsonify({"log": []})
    cur = conn.cursor()
    cur.execute(
        "SELECT alert_type, symbol, message, reason, link, triggered_at, seen "
        "FROM alert_log WHERE user_id=%s ORDER BY triggered_at DESC LIMIT 30", (u["id"],))
    out = []
    for row in cur.fetchall():
        out.append({"alert_type": row[0], "symbol": row[1], "message": row[2],
                    "reason": row[3], "link": row[4],
                    "triggered_at": row[5].isoformat() if row[5] else None, "seen": row[6]})
    cur.execute("UPDATE alert_log SET seen=true WHERE user_id=%s AND seen=false", (u["id"],))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"log": out})


@app.route("/alerts/register-push", methods=["POST"])
def alerts_register_push():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    b = request.get_json(silent=True) or {}
    token = (b.get("token") or "").strip()
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database unavailable."}), 500
    cur = conn.cursor()
    cur.execute("UPDATE users SET onesignal_token=%s WHERE id=%s", (token or None, u["id"]))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@app.route("/alerts")
def alerts():
    # Reads the logged in user's saved stocks, scores each one, and surfaces only the names
    # that warrant a look right now. This is the in app feed. A push to the phone is the next layer.
    u = current_user()
    if not u:
        return jsonify({"status": "logged_out", "alerts": []})
    return jsonify(build_alerts(u["id"]))


@app.route("/alerts/subscribe", methods=["GET", "POST"])
def alerts_subscribe():
    # Saves a daily digest email subscription and the user's alert preference. No email is sent
    # yet and there is no verification step. A future cron job will read this table and send the
    # digest. alert_prefs is one of all, verdict_only, price_only.
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    uid = u["id"]

    if request.method == "GET":
        conn = get_db()
        if conn is None:
            return jsonify({"subscribed": False, "email": "", "alert_prefs": "all"})
        try:
            cur = conn.cursor()
            cur.execute("SELECT email, alert_prefs FROM alert_subscriptions WHERE user_id = %s", (uid,))
            row = cur.fetchone()
            cur.close()
        except Exception as e:
            logger.error("subscribe get error: %s" % e)
            return jsonify({"subscribed": False, "email": "", "alert_prefs": "all"})
        finally:
            conn.close()
        if row and row[0]:
            return jsonify({"subscribed": True, "email": row[0], "alert_prefs": row[1] or "all"})
        return jsonify({"subscribed": False, "email": "", "alert_prefs": "all"})

    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip()
    prefs = (data.get("alert_prefs") or "all").strip()
    if prefs not in ("all", "verdict_only", "price_only"):
        prefs = "all"
    if not email or "@" not in email or "." not in email:
        return jsonify({"error": "Please enter a valid email address."}), 400
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database not available."}), 500
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO alert_subscriptions (user_id, email, verified, alert_prefs) VALUES (%s,%s,0,%s) "
            "ON CONFLICT (user_id) DO UPDATE SET email=EXCLUDED.email, alert_prefs=EXCLUDED.alert_prefs",
            (uid, email, prefs),
        )
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error("subscribe post error: %s" % e)
        return jsonify({"error": "Could not save your subscription."}), 500
    finally:
        conn.close()
    return jsonify({"subscribed": True, "email": email, "alert_prefs": prefs})


@app.route("/dashboard")
def dashboard():
    # One aggregated payload for the home dashboard. Market data is returned for everyone. The
    # personal sections are added only when logged in. This leans on existing caches and never
    # makes a brand new external integration: movers, congress, and context are pure cache reads,
    # so if they are cold they come back empty and their own endpoints warm them. Indices and the
    # watchlist use light_score, which carries its own 15 minute cache, and portfolio reuses the
    # 60 second portfolio cache. So the 60 second poll from the page stays cheap after the first load.
    u = current_user()
    data = {"logged_in": bool(u)}

    INDEX_SET = [("^GSPC", "S&P 500"), ("^IXIC", "NASDAQ"), ("^DJI", "DOW JONES"), ("^VIX", "VIX")]
    indices = []
    for sym, label in INDEX_SET:
        r = light_score(sym)
        if r and isinstance(r.get("price"), (int, float)):
            indices.append({"symbol": sym, "label": label, "price": r.get("price"), "change_pct": r.get("change_pct")})
        else:
            indices.append({"symbol": sym, "label": label, "price": None, "change_pct": None})
    data["market_indices"] = indices

    # Live market context: read the cached value only, never call the model from here.
    data["market_context"] = get_cache("ctx_^GSPC")

    # Top movers: shared cached helper, top 3 each side, same source as Discover and /movers.
    md = get_movers_cached()
    data["movers"] = {
        "gainers": (md.get("gainers") or [])[:3],
        "losers": (md.get("losers") or [])[:3],
    }

    # Congress: read the cached insights only, top 3 most active.
    congress = []
    ci = CACHE.get("congress_insights")
    if ci and isinstance(ci[0], dict):
        congress = (ci[0].get("politicians") or [])[:3]
    data["congress"] = congress

    if not u:
        return jsonify(data)

    uid = u["id"]

    wl = []
    conn = get_db()
    if conn is not None:
        try:
            cur = conn.cursor()
            cur.execute("SELECT symbol, name FROM watchlist WHERE user_id = %s ORDER BY added_at DESC", (uid,))
            wrows = cur.fetchall()
            cur.close()
        except Exception as e:
            logger.error("dashboard watchlist error: %s" % e)
            wrows = []
        finally:
            conn.close()
        for sym, nm in wrows:
            r = light_score(sym)
            if r:
                wl.append({
                    "symbol": sym,
                    "name": r.get("name") or nm or sym,
                    "price": r.get("price"),
                    "change_pct": r.get("change_pct"),
                    "verdict": r.get("verdict"),
                })
            else:
                wl.append({"symbol": sym, "name": nm or sym, "price": None, "change_pct": None, "verdict": None})
    data["watchlist"] = wl

    port = compute_portfolio(uid)
    data["portfolio"] = port.get("totals") if isinstance(port, dict) else None

    ab = build_alerts(uid)
    data["alerts"] = (ab.get("alerts") or [])[:3]
    data["verdict_changes"] = ab.get("verdict_changes", 0)

    return jsonify(data)


def fmt_money_py(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "an unclear amount"
    if v >= 1e9:
        return "$%.1fB" % (v / 1e9)
    if v >= 1e6:
        return "$%.1fM" % (v / 1e6)
    if v >= 1e3:
        return "$%.0fK" % (v / 1e3)
    return "$%.0f" % v


def _safe_float(v, default):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def insider_brief(symbol, price):
    try:
        t = yf.Ticker(symbol)
        it = t.insider_transactions
        if it is None or it.empty:
            return {"selling": False, "clevel_sells": 0, "sell_value": 0}
        clevel_sells = 0
        sell_value = 0
        p = price if isinstance(price, (int, float)) and price > 0 else 0
        for _, rr in it.head(12).iterrows():
            row = rr.to_dict()
            pos = row.get("Position") or row.get("Title") or row.get("Relation") or ""
            desc = row.get("Transaction") or row.get("Text") or ""
            basis = str(desc) if str(desc).strip() else " ".join(str(x) for x in row.values())
            if any(c in str(pos).upper() for c in INSIDER_CLEVEL) and classify_insider_kind(basis) == "sell":
                clevel_sells += 1
                try:
                    sell_value += int(float(row.get("Shares") or 0)) * p
                except Exception:
                    pass
        return {"selling": clevel_sells >= 1, "clevel_sells": clevel_sells, "sell_value": sell_value}
    except Exception:
        return {"selling": False, "clevel_sells": 0, "sell_value": 0}


def build_ask_facts(d, ins, extra_news=None, extra_insider=None):
    # Shared, ETF aware fact sheet for the Ask answer. For a fund it states the cost and holdings
    # framing instead of a stock verdict, so the AI never tries to score an ETF like a stock.
    if d.get("verdict") == "ETF":
        hold = d.get("holdings") or []
        top = ", ".join([h.get("symbol", "") for h in hold[:5] if h.get("symbol")])
        facts = ("This is an exchange traded fund, a basket of many holdings, not a single stock. "
                 "Expense ratio: %s percent. Total assets under management: %s. Category: %s. Fund family: %s. "
                 "Dividend yield: %s percent. Price: %s. Change today: %s percent."
                 % (d.get("expense_ratio"), d.get("total_assets"), d.get("category"),
                    d.get("fund_family"), d.get("yield"), d.get("price"), d.get("change_pct")))
        if top:
            facts += " Largest holdings: " + top + "."
    else:
        facts = ("Current verdict: %s. Conviction: %s. Price: %s. Change today: %s percent. PE ratio: %s. Analyst upside to average target: %s percent."
                 % (d.get("verdict"), d.get("conviction"), d.get("price"), d.get("change_pct"), d.get("pe_ratio"), d.get("upside")))
        ins_src = extra_insider if extra_insider is not None else ins
        if ins_src is not None:
            facts += " Insider picture: about %s recent C level sales." % ins_src.get("clevel_sells")
    if extra_news:
        heads = "; ".join([n.get("headline", "") for n in extra_news if n.get("headline")])
        if heads:
            facts += " Recent headlines: " + heads + "."
    return facts


def ask_gemini(symbol, q, d, ins, extra_news=None, extra_insider=None, history=None):
    try:
        facts = build_ask_facts(d, ins, extra_news, extra_insider)
        # CHUNK: multi-turn chat. Gemini stays on a single text prompt, so the facts lead every turn,
        # then the conversation so far, then the new question. Repeating the facts keeps it grounded.
        if history:
            rules = (
                "You are the explanation layer for an educational stock app for everyday people and beginners. "
                "The user is asking about " + symbol + " (" + str(d.get("name", symbol)) + "). "
                "Here are the engine's current facts for this stock: " + facts + " "
                "Answer using only these facts plus basic, general investing ideas. Do not use outside knowledge about this "
                "specific company. Do not invent or assume any facts that are not above, such as news, earnings details, or analyst actions. "
                "If a question asks for a specific fact you do not have, say you do not have enough information to answer that. "
                "Answer in 2 to 4 short, plain sentences with no jargon. Do not use any dashes or hyphens, use plain words. "
                "Never tell the reader what they personally should buy or sell. Return plain text only, no markdown."
            )
            convo = ""
            for m in history:
                if not m.get("content"):
                    continue
                who = "User" if m.get("role") == "user" else "Assistant"
                convo += who + ": " + str(m.get("content")) + "\n"
            prompt = rules + " Here is the conversation so far:\n" + convo + "User: " + q + "\nAssistant:"
        else:
            prompt = (
                "You are the explanation layer for an educational stock app for everyday people and beginners. "
                "The user is looking at " + symbol + " (" + str(d.get("name", symbol)) + ") and asks: \"" + q + "\". "
                "Here are the engine's current facts for this stock: " + facts + " "
                "Answer in 2 to 4 short, plain sentences with no jargon, grounded only in these facts and basic investing ideas. "
                "Do not use any dashes or hyphens, use plain words. "
                "Never tell the reader what they personally should buy or sell. Return plain text only, no markdown."
            )
        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=" + GEMINI_KEY
        payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.3, "maxOutputTokens": 400}}
        r = requests.post(url, json=payload, timeout=12)
        if r.status_code == 200:
            data = r.json()
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
        logger.error("ask_gemini %s non-200 status %s: %s" % (symbol, r.status_code, str(r.text)[:200]))
    except Exception as e:
        logger.error("ask_gemini %s: %s" % (symbol, e))
        # CHUNK: log the raw response for debugging
        try:
            logger.error("ask_gemini raw response: %s" % str(r.text)[:200])
        except Exception:
            pass
    return None


# CHUNK: DeepSeek AI provider, same grounding rules as Gemini
def ask_deepseek(symbol, q, d, ins, extra_news=None, extra_insider=None, history=None):
    try:
        facts = build_ask_facts(d, ins, extra_news, extra_insider)
        # Immutable safety guardrail, prepended before everything else and never overridden.
        guardrail = (
            "You are Apex Q Intelligence, a strictly objective, non advisory market data processing "
            "machine. You are legally forbidden from providing financial advice, buy or sell "
            "directives, or asset price predictions. You must only process and reflect historical "
            "public data, SEC filings, and disclosed metrics in a neutral, plain English format. If "
            "asked for advice, respond: I cannot provide financial advice. Here is the data you requested. "
        )
        # CHUNK: multi-turn chat. With history we send a real system message carrying the live facts,
        # then the prior turns, then the new question. The facts are rebuilt and sent every turn so
        # the model stays grounded and cannot drift into invented facts, even on adversarial questions.
        if history:
            system_content = (
                guardrail +
                "You are the explanation layer for an educational stock app for everyday people and beginners. "
                "The user is asking about " + symbol + " (" + str(d.get("name", symbol)) + "). "
                "Here are the engine's current facts for this stock: " + facts + " "
                "Answer using only these facts plus basic, general investing ideas. Do not use outside knowledge about this "
                "specific company. Do not invent or assume any facts that are not above, such as news, earnings details, or analyst actions. "
                "If a question asks for a specific fact you do not have, say you do not have enough information to answer that. "
                "Answer in 2 to 4 short, plain sentences with no jargon. Do not use any dashes or hyphens, use plain words. "
                "Never tell the reader what they personally should buy or sell. Return plain text only, no markdown."
            )
            messages = [{"role": "system", "content": system_content}]
            for m in history:
                role = m.get("role")
                if role in ("user", "assistant") and m.get("content"):
                    messages.append({"role": role, "content": str(m.get("content"))})
            messages.append({"role": "user", "content": q})
        else:
            prompt = (
                guardrail +
                "You are the explanation layer for an educational stock app for everyday people and beginners. "
                "The user is looking at " + symbol + " (" + str(d.get("name", symbol)) + ") and asks: \"" + q + "\". "
                "Here are the engine's current facts for this stock: " + facts + " "
                "Answer in 2 to 4 short, plain sentences with no jargon, grounded only in these facts and basic investing ideas. "
                "Do not use any dashes or hyphens, use plain words. "
                "Never tell the reader what they personally should buy or sell. Return plain text only, no markdown."
            )
            messages = [{"role": "user", "content": prompt}]
        headers = {"Authorization": "Bearer " + DEEPSEEK_KEY, "Content-Type": "application/json"}
        payload = {"model": "deepseek-chat", "messages": messages, "temperature": 0.3, "max_tokens": 400}
        r = requests.post("https://api.deepseek.com/chat/completions", headers=headers, json=payload, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data["choices"][0]["message"]["content"].strip()
        logger.error("ask_deepseek %s non-200 status %s: %s" % (symbol, r.status_code, str(r.text)[:200]))
    except Exception as e:
        logger.error("ask_deepseek %s: %s" % (symbol, e))
        try:
            logger.error("ask_deepseek raw response: %s" % str(r.text)[:200])
        except Exception:
            pass
    return None


def ask_fallback(symbol, q, d, ins, extra_news=None, extra_insider=None):
    ql = q.lower()
    v = d.get("verdict", "WATCH")
    chg = d.get("change_pct")
    pe = d.get("pe_ratio")
    up = d.get("upside")
    # CHUNK: ground news questions in the report's actual headlines when we have them
    if extra_news and any(w in ql for w in ["news", "headline", "article", "report", "press", "announce", "update"]):
        heads = "; ".join([n.get("headline", "") for n in extra_news if n.get("headline")])
        if heads:
            return "Here are the most recent headlines for " + symbol + ". " + heads + ". Read the full articles in the News Feed section of the report."
    # CHUNK: ETFs are funds, not stocks, so answer on cost and holdings rather than a stock verdict.
    if v == "ETF":
        ans = symbol + " is an exchange traded fund, a single ticker that holds a basket of many investments. "
        er = d.get("expense_ratio")
        cat = d.get("category")
        if er not in (None, "N/A"):
            ans += "Its expense ratio, the yearly cost to own it, is about " + str(er) + " percent. "
        if cat not in (None, "N/A"):
            ans += "Its category is " + str(cat) + ". "
        ans += "Open the full report for its top holdings and sector mix. A fund is judged on what it costs and what it holds, not a stock style verdict."
        return ans
    # CHUNK: answer 'why did it move' questions with available signals
    if any(p in ql for p in ["why did it drop", "why is it down", "why did it fall", "why is it falling", "why did it rise", "why is it up", "why did it jump", "why is it rising", "why did it move", "what happened", "what caused", "what changed", "what's new", "whats new", "what is new", "any news", "news today"]):
        move_bits = []
        if isinstance(chg, (int, float)) and chg <= -0.01:
            move_bits.append("it is down %s%% today" % abs(chg))
        elif isinstance(chg, (int, float)) and chg >= 0.01:
            move_bits.append("it is up %s%% today" % chg)
        if v in ("PASS", "WATCH", "APPROVE"):
            move_bits.append("the engine currently reads it at %s" % v)
        move_ins = ins if ins is not None else insider_brief(symbol, d.get("price"))
        if move_ins and move_ins.get("selling"):
            move_bits.append("company executives have been selling recently, which can weigh on a stock")
        if isinstance(up, (int, float)) and up < 0:
            move_bits.append("it was already trading above the average analyst target, which can pull a price back")
        if move_bits:
            reasons = ", and ".join(move_bits)
            reasons = reasons[0].upper() + reasons[1:]
            return "Here is what the engine can see. " + reasons + ". That said, the real reason for a daily move is usually news, an earnings report, an analyst call, or a broader market swing, which the numbers alone do not capture. Check the News Feed section in the full report for the real story."
        return "Here is what the engine can see. The numbers on this one do not explain today's move, which usually means it is being driven by news, earnings, or a broader market swing rather than the signals. Check the News Feed section in the full report for the real story."
    parts = []
    if any(w in ql for w in ["why", "watch", "verdict", "call", "rating", "approve", "pass", "buy", "hold"]):
        if v == "WATCH":
            parts.append("%s is at WATCH, which means the signals disagree, so there is no clear edge today and the patient move is to wait for the picture to sharpen." % symbol)
        elif v == "APPROVE":
            parts.append("%s is at APPROVE, which means the positive signals currently outweigh the negative ones." % symbol)
        elif v == "PASS":
            parts.append("%s is at PASS, which means the negatives outweigh the positives right now." % symbol)
    if any(w in ql for w in ["change", "would", "flip", "improve", "turn", "move it"]):
        parts.append("To move toward APPROVE the engine wants the positives to outweigh the negatives. The fastest ways are an analyst upgrade to Buy, a C level executive buying shares, or a clear price move up on heavy volume. It slips toward PASS if the price breaks down alongside a negative analyst call or heavy executive selling.")
    ins_src = extra_insider if extra_insider is not None else ins
    if ins_src is not None:
        if ins_src.get("selling"):
            n = ins_src.get("clevel_sells")
            parts.append("On insiders: company people have been selling, about %s C level sale%s recently, roughly %s in value. Executives sell for many reasons, so selling alone is a softer signal than buying, but a cluster is a caution." % (n, "" if n == 1 else "s", fmt_money_py(ins_src.get("sell_value"))))
        else:
            parts.append("On insiders: no notable cluster of executive selling is showing up right now, which is neutral.")
    if any(w in ql for w in ["valuation", "expensive", "cheap", "pe", "p/e", "earnings", "overvalued", "undervalued", "value"]):
        if pe and str(pe) != "N/A":
            tail = " That is on the higher side, so a lot of growth is already priced in." if _safe_float(pe, 0) > 40 else (" That is on the lower, more value leaning side." if _safe_float(pe, 99) < 18 else " That sits in a middle range.")
            parts.append("On valuation: it trades at about %s times earnings." % pe + tail)
        else:
            parts.append("On valuation: a price to earnings number is not available for it right now, which is common for companies without steady profits.")
    if any(w in ql for w in ["target", "upside", "analyst", "potential", "go up"]):
        if isinstance(up, (int, float)):
            parts.append("On the analyst view: the average price target sits about %s%% %s today's price." % (abs(up), "above" if up >= 0 else "below"))
    if any(w in ql for w in ["today", "moving", "doing", "price today"]):
        if isinstance(chg, (int, float)):
            parts.append("Today it is %s%% %s." % (abs(chg), "up" if chg >= 0 else "down"))
    if not parts:
        parts.append("%s is at %s right now." % (symbol, v))
        if isinstance(chg, (int, float)):
            parts.append("It is %s%% %s today." % (abs(chg), "up" if chg >= 0 else "down"))
        if isinstance(up, (int, float)):
            parts.append("Analysts see about %s%% %s their average target." % (abs(up), "above" if up >= 0 else "below"))
        parts.append("Open the full report for the complete breakdown.")
    parts.append("This is educational, not advice.")
    return " ".join(parts)


# CHUNK: fuzzy ticker correction for common misspellings
FUZZY_TICKERS = {
    "NDVA": "NVDA", "NVDIA": "NVDA", "NVIDA": "NVDA",
    "APPL": "AAPL", "APLE": "AAPL",
    "TESLA": "TSLA",
    "GOOG": "GOOGL", "GOGL": "GOOGL",
    "MICROSOFT": "MSFT",
    "AMAZON": "AMZN",
    "FACEBOOK": "META",
    "NETFLIX": "NFLX",
    "JPMORGAN": "JPM",
    "BITCOIN": "BTC-USD",
}


def fuzzy_ticker(typo):
    # Returns a corrected ticker for a common misspelling, or None. Case-insensitive.
    if not typo:
        return None
    return FUZZY_TICKERS.get(str(typo).strip().upper())


NAME_TO_TICKER = {
    "bank of america": "BAC", "bofa": "BAC",
    "jpmorgan chase": "JPM", "jp morgan chase": "JPM", "jpmorgan": "JPM", "jp morgan": "JPM", "chase": "JPM",
    "wells fargo": "WFC", "citigroup": "C", "citi": "C", "goldman sachs": "GS", "goldman": "GS",
    "morgan stanley": "MS", "us bancorp": "USB", "us bank": "USB",
    "apple": "AAPL", "microsoft": "MSFT", "alphabet": "GOOGL", "google": "GOOGL", "amazon": "AMZN",
    "meta": "META", "facebook": "META", "nvidia": "NVDA", "tesla": "TSLA", "netflix": "NFLX",
    "broadcom": "AVGO", "oracle": "ORCL", "salesforce": "CRM", "adobe": "ADBE", "qualcomm": "QCOM",
    "intel": "INTC", "palantir": "PLTR",
    "exxon mobil": "XOM", "exxon": "XOM", "chevron": "CVX", "conocophillips": "COP", "occidental": "OXY",
    "walmart": "WMT", "costco": "COST", "target": "TGT", "home depot": "HD", "nike": "NKE",
    "mcdonalds": "MCD", "starbucks": "SBUX", "coca cola": "KO", "coke": "KO", "pepsico": "PEP", "pepsi": "PEP",
    "disney": "DIS", "johnson and johnson": "JNJ", "pfizer": "PFE", "merck": "MRK", "eli lilly": "LLY",
    "lilly": "LLY", "unitedhealth": "UNH", "boeing": "BA", "caterpillar": "CAT", "ford": "F",
    "general motors": "GM", "verizon": "VZ", "visa": "V", "mastercard": "MA",
}

SECTOR_TO_TICKER = {
    "energy": "XOM", "oil and gas": "XOM", "oil": "XOM",
    "technology": "AAPL", "tech": "AAPL",
    "banking": "JPM", "financials": "JPM", "financial": "JPM", "banks": "JPM", "bank": "JPM",
    "healthcare": "JNJ", "health care": "JNJ", "health": "JNJ",
    "retail": "WMT", "automotive": "TSLA", "auto": "TSLA", "cars": "TSLA",
    "semiconductors": "NVDA", "semiconductor": "NVDA", "chips": "NVDA", "chip": "NVDA",
    "defense": "BA", "artificial intelligence": "NVDA",
}

COMMON_TICKERS = set(NAME_TO_TICKER.values()) | set(SECTOR_TO_TICKER.values()) | set(SCAN_UNIVERSE)


def extract_entities(text):
    tl = " " + text.lower() + " "
    found = []
    seen = set()
    for tok in re.findall(r"\b[A-Z]{2,5}\b", text):
        if tok in COMMON_TICKERS and tok not in seen:
            found.append((tok, tok, False))
            seen.add(tok)
    for name in sorted(NAME_TO_TICKER, key=len, reverse=True):
        if any(name + suff in tl for suff in [" ", ",", ".", "?"]) and (" " + name) in tl:
            tkr = NAME_TO_TICKER[name]
            if tkr not in seen:
                found.append((tkr, name.title(), False))
                seen.add(tkr)
    for sec in sorted(SECTOR_TO_TICKER, key=len, reverse=True):
        if any(sec + suff in tl for suff in [" ", ",", ".", "?"]) and (" " + sec) in tl:
            tkr = SECTOR_TO_TICKER[sec]
            if tkr not in seen:
                found.append((tkr, sec.title() + " stocks, using " + tkr + " as a bellwether", True))
                seen.add(tkr)
    return found


PRIVATE_COMPANIES = {
    "spacex": "SpaceX", "starlink": "Starlink", "openai": "OpenAI", "anthropic": "Anthropic",
    "stripe": "Stripe", "databricks": "Databricks", "bytedance": "ByteDance", "tiktok": "TikTok",
    "x corp": "X", "discord": "Discord", "epic games": "Epic Games", "valve": "Valve",
}


def extract_private(text):
    tl = " " + text.lower() + " "
    out = []
    seen = set()
    for k in sorted(PRIVATE_COMPANIES, key=len, reverse=True):
        if any(k + suff in tl for suff in [" ", ",", ".", "?"]) and (" " + k) in tl:
            v = PRIVATE_COMPANIES[k]
            if v not in seen:
                out.append(v)
                seen.add(v)
    return out


def coach_answer(q, entities, private):
    scored = []
    for tkr, label, is_sec in entities[:4]:
        r = light_score(tkr)
        if r:
            scored.append((label, is_sec, r))
    parts = ["First, the honest part. I am an educational tool, not a financial advisor, so I will not tell you where to put your money. That is your call, and a real one. What I can do is show you how each one looks on the signals, in plain language, so you can decide for yourself."]
    for pname in private:
        parts.append(pname + " is privately held and not traded on the stock market, so there is no public stock for it to read and you cannot buy it like a normal share. If it ever goes public, that changes.")
    if not scored:
        if private:
            parts.append("That leaves nothing public here to compare. Name a publicly traded company or a ticker and I can break it down.")
        else:
            parts.append("I could not match that to stocks I can read. Try naming the companies or tickers directly, like Bank of America, JPMorgan, and Exxon.")
        return " ".join(parts)
    if private:
        parts.append("Here is the one I can actually read." if len(scored) == 1 else "Here are the ones I can actually read.")
    for label, is_sec, r in scored:
        v = r.get("verdict", "WATCH")
        chg = r.get("change_pct")
        up = r.get("upside")
        pe = r.get("pe_ratio")
        line = label + " is at " + v + " right now."
        bits = []
        if isinstance(chg, (int, float)):
            bits.append("%s%% %s today" % (abs(chg), "up" if chg >= 0 else "down"))
        if isinstance(up, (int, float)):
            bits.append("analysts see about %s%% %s their average target" % (abs(up), "above" if up >= 0 else "below"))
        try:
            bits.append("around %s times earnings" % round(float(pe), 1))
        except (TypeError, ValueError):
            pass
        if bits:
            line += " It is " + ", ".join(bits) + "."
        parts.append(line)
    parts.append("How to think about it, without anyone deciding for you. The amount of money, including the figure you mentioned, does not change what the signals say about each name. What matters more is your own time horizon, how much risk you can sit with, and whether you spread money out rather than put it all in one place. Concentrating everything in a single stock is how beginners get hurt.")
    parts.append("None of this is a recommendation. For a real decision with real money, your own homework and a licensed professional are the right next step.")
    return " ".join(parts)


def _ask_fact_line(tkr, label):
    """One fact line for the Ask prompts. Prefers the cached full report, the same data the person
    is looking at on the report screen, so Ask and the report can never disagree about a number.
    Falls back to the light snapshot when no full report has been run recently. Only fields that
    actually exist get included, so the model is never handed an invented figure."""
    cached = CACHE.get("full_" + tkr)
    if cached and (time.time() - cached[1]) < 900 and cached[0]:
        d = cached[0]
        bits = []
        if d.get("name"):
            bits.append("%s (%s)" % (d["name"], tkr))
        else:
            bits.append("%s (%s)" % (label, tkr))
        if d.get("sector"):
            bits.append("sector " + str(d["sector"]))
        if d.get("verdict"):
            bits.append("verdict " + str(d["verdict"]))
        if d.get("price") is not None:
            bits.append("price %s dollars" % d["price"])
        if d.get("change_pct") is not None:
            bits.append("%s percent today" % d["change_pct"])
        mc = d.get("market_cap")
        if mc:
            try:
                mcf = float(mc)
                if mcf >= 1e12:
                    bits.append("market cap %.2f trillion dollars" % (mcf / 1e12))
                elif mcf >= 1e9:
                    bits.append("market cap %.1f billion dollars" % (mcf / 1e9))
                elif mcf >= 1e6:
                    bits.append("market cap %.0f million dollars" % (mcf / 1e6))
            except (TypeError, ValueError):
                pass
        if d.get("pe_ratio") is not None:
            bits.append("PE %s" % d["pe_ratio"])
        ac = d.get("analyst_consensus") or {}
        if ac.get("consensus_rating"):
            bits.append("analyst consensus %s from %s analysts" % (ac["consensus_rating"], ac.get("number_of_analysts", "several")))
        if d.get("analyst_target"):
            bits.append("average analyst target %s dollars" % d["analyst_target"])
        if d.get("revenue_growth") is not None:
            bits.append("revenue growth %s percent" % d["revenue_growth"])
        if d.get("profit_margin") is not None:
            bits.append("profit margin %s percent" % d["profit_margin"])
        if d.get("debt_to_equity") is not None:
            bits.append("debt to equity %s" % d["debt_to_equity"])
        if d.get("roe") is not None:
            bits.append("return on equity %s percent" % d["roe"])
        if d.get("alpha_score") is not None:
            bits.append("Apex Q Alpha Score %s out of 100" % d["alpha_score"])
        moat = d.get("apex_moat") or {}
        if moat.get("rating"):
            bits.append("moat rating %s" % moat["rating"])
        if d.get("conviction"):
            bits.append("conviction %s" % d["conviction"])
        if d.get("insider_sell_value"):
            try:
                isv = float(d["insider_sell_value"])
                if isv >= 1e6:
                    bits.append("recent insider selling %.1f million dollars" % (isv / 1e6))
            except (TypeError, ValueError):
                pass
        return ", ".join(bits)
    r = light_score(tkr)
    if r:
        return "%s (%s): verdict %s, %s percent today, analyst upside %s percent, PE %s" % (
            label, tkr, r.get("verdict"), r.get("change_pct"), r.get("upside"), r.get("pe_ratio"))
    return None


def coach_gemini(q, entities):
    facts = []
    for tkr, label, is_sec in entities[:4]:
        line = _ask_fact_line(tkr, label)
        if line:
            facts.append(line)
    if not facts:
        return None
    prompt = (
        "You are the educational explanation layer of a stock app for everyday people and beginners. "
        "The user asked, possibly by voice: \"" + q + "\". "
        "Here are the engine's current live facts: " + "; ".join(facts) + ". "
        "STRICT RULES: You are not a financial advisor. Do not tell the user where to invest, do not recommend a specific stock to buy, and do not suggest how to split any amount of money. "
        "Instead, explain in simple plain language how each option looks based on the facts, what the differences mean, and how a beginner should think the decision through themselves, including risk, time horizon, and not concentrating money in one name. "
        "Make clear the dollar amount does not change what the signals say. "
        "Keep it to about 5 to 8 short sentences, no jargon. Do not use any dashes or hyphens, use plain words. Never tell the reader what they personally should buy or sell. Return plain text only, no markdown."
    )
    try:
        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=" + GEMINI_KEY
        payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.3, "maxOutputTokens": 500}}
        r = requests.post(url, json=payload, timeout=12)
        if r.status_code == 200:
            data = r.json()
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception as e:
        logger.error("coach_gemini: %s" % e)
    return None


# CHUNK: DeepSeek coach for multi-stock comparison questions
def ask_deepseek_coach(q, entities):
    facts = []
    for tkr, label, is_sec in entities[:4]:
        line = _ask_fact_line(tkr, label)
        if line:
            facts.append(line)
    if not facts:
        return None
    prompt = (
        "You are the educational explanation layer of a stock app for everyday people and beginners. "
        "The user asked, possibly by voice: \"" + q + "\". "
        "Here are the engine's current live facts: " + "; ".join(facts) + ". "
        "STRICT RULES: You are not a financial advisor. Do not tell the user where to invest, do not recommend a specific stock to buy, and do not suggest how to split any amount of money. "
        "Instead, explain in simple plain language how each option looks based on the facts, what the differences mean, and how a beginner should think the decision through themselves, including risk, time horizon, and not concentrating money in one name. "
        "Make clear the dollar amount does not change what the signals say. "
        "Keep it to about 5 to 8 short sentences, no jargon. Do not use any dashes or hyphens, use plain words. Never tell the reader what they personally should buy or sell. Return plain text only, no markdown."
    )
    try:
        headers = {"Authorization": "Bearer " + DEEPSEEK_KEY, "Content-Type": "application/json"}
        payload = {"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 500}
        r = requests.post("https://api.deepseek.com/chat/completions", headers=headers, json=payload, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data["choices"][0]["message"]["content"].strip()
        logger.error("ask_deepseek_coach non-200 status %s: %s" % (r.status_code, str(r.text)[:200]))
    except Exception as e:
        logger.error("ask_deepseek_coach: %s" % e)
        try:
            logger.error("ask_deepseek_coach raw response: %s" % str(r.text)[:200])
        except Exception:
            pass
    return None


@app.route("/ask")
def ask():
    symbol = (request.args.get("symbol") or "").strip().upper()
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"answer": "Ask a question, like why is this a watch, or name a few stocks and ask how they compare."})
    gate = usage_gate("ask")
    if gate is not None:
        return gate
    # AI cache: identical questions (same symbol, same text, no chat history) return instantly with
    # no provider cost. History-bearing turns skip the cache since they are conversation specific.
    _ask_hist = request.args.get("history", "[]")
    _ask_cacheable = _ask_hist in ("[]", "", None)
    if _ask_cacheable:
        import hashlib as _hh
        _ask_key = "ask_" + _hh.sha1((symbol + "|" + q.lower()).encode()).hexdigest()
        _hit = get_cached_ai(_ask_key, max_age_hours=3)
        if _hit and _hit.get("response"):
            return jsonify({"answer": _hit["response"], "cached": True})
    else:
        _ask_key = None
    # CHUNK: multi-turn chat. Optional prior turns, sent as a JSON list of {role, content}. The
    # current question is the q param, so history holds only the turns before it.
    history = []
    try:
        parsed = json.loads(request.args.get("history", "[]"))
        if isinstance(parsed, list):
            history = [m for m in parsed if isinstance(m, dict) and m.get("role") in ("user", "assistant") and m.get("content")]
    except Exception:
        history = []
    ql = q.lower()
    entities = extract_entities(q)
    private = extract_private(q)
    allocation = any(p in ql for p in [
        "where should i", "where do i", "should i invest", "invest", "put my money",
        "put $", "split", "allocate", "best to buy", "which should i buy",
        "which one should i", "what should i buy", "better buy", "worth buying",
    ])
    comparison = any(p in ql for p in [
        "difference", "compare", "comparison", "versus", " vs ", "vs.", "between",
        "stronger", "better than", "which is better",
    ])
    trigger = allocation or comparison
    total_named = len(entities) + len(private)
    if total_named >= 2 or (trigger and total_named >= 1):
        # CHUNK: DeepSeek primary, Gemini fallback, rules-based final safety net
        if entities and not private:
            if DEEPSEEK_KEY:
                a = ask_deepseek_coach(q, entities)
                if a:
                    return jsonify({"answer": a})
            if GEMINI_KEY:
                a = coach_gemini(q, entities)
                if a:
                    return jsonify({"answer": a})
        return jsonify({"answer": coach_answer(q, entities, private)})

    sym = symbol or (entities[0][0] if entities else "")
    # CHUNK: Ask/Compare name resolution — accept a company name, not just a ticker
    if sym and not looks_like_ticker(sym):
        sym = resolve_ticker(sym).upper()
    if not sym:
        if private:
            return jsonify({"answer": coach_answer(q, [], private)})
        return jsonify({"answer": "Tell me which stock you mean. Type a ticker in the box, or name the company in your question."})
    d = light_score(sym)
    # CHUNK: try fuzzy fix before giving up
    if not d:
        fixed = fuzzy_ticker(sym)
        if fixed and fixed != sym:
            d_fixed = light_score(fixed)
            if d_fixed:
                logger.info("fuzzy ticker correction: %s -> %s" % (sym, fixed))
                sym = fixed
                d = d_fixed
    # CHUNK: last resort, treat it as a company name and resolve to a ticker
    if not d:
        resolved = resolve_ticker(sym).upper()
        if resolved and resolved != sym:
            d_res = light_score(resolved)
            if d_res:
                logger.info("name resolution: %s -> %s" % (sym, resolved))
                sym = resolved
                d = d_res
    if not d:
        return jsonify({"answer": "Could not find a stock matching that name. Try the ticker symbol instead."})
    # CHUNK: defer to the authoritative full report verdict so Ask never contradicts the report
    full = compute_full_report(sym)
    if full and full.get("verdict"):
        d = dict(d)
        d["verdict"] = full.get("verdict")
        if full.get("conviction"):
            d["conviction"] = full.get("conviction")
        if full.get("verdict") == "ETF":
            for _k in ("expense_ratio", "total_assets", "category", "fund_family", "yield", "holdings"):
                if _k in full:
                    d[_k] = full[_k]
    # CHUNK: pull the specific data the question is about, so the answer is grounded in real facts.
    # The full report and its news are already in hand from the verdict step above, so reuse it.
    extra_news = None
    if any(w in ql for w in ["news", "headline", "article", "report", "press", "announce", "update"]):
        if isinstance(full, dict) and full.get("news"):
            extra_news = full["news"][:3]
    ins = None
    if any(w in ql for w in ["insider", "executive", "exec", "selling", "sold", "buying", "bought"]):
        ins = insider_brief(sym, d.get("price"))
    # CHUNK: DeepSeek primary, Gemini fallback, rules-based final safety net. History threads into
    # the two AI providers for multi-turn chat. The fallback stays single-turn, current question only.
    if DEEPSEEK_KEY:
        a = ask_deepseek(sym, q, d, ins, extra_news=extra_news, extra_insider=ins, history=history)
        if a:
            if _ask_key:
                set_cached_ai(_ask_key, a)
            return jsonify({"answer": a, "verdict": d.get("verdict"), "symbol": sym})
    if GEMINI_KEY:
        a = ask_gemini(sym, q, d, ins, extra_news=extra_news, extra_insider=ins, history=history)
        if a:
            if _ask_key:
                set_cached_ai(_ask_key, a)
            return jsonify({"answer": a, "verdict": d.get("verdict"), "symbol": sym})
    _fb = ask_fallback(sym, q, d, ins, extra_news=extra_news, extra_insider=ins)
    if _ask_key:
        set_cached_ai(_ask_key, _fb)
    return jsonify({"answer": _fb, "verdict": d.get("verdict"), "symbol": sym})


@app.route("/trending")
def trending():
    # The day's trending stocks, the names most actively traded right now. Pulled live from
    # FMP and refreshed every half hour so it stays current without burning the daily call budget.
    now = time.time()
    if _TREND["data"] is not None and now - _TREND["ts"] < 1800:
        return jsonify(_TREND["data"])

    def parse_pct(v):
        try:
            return round(float(str(v).replace("%", "").replace("(", "-").replace(")", "").strip()), 2)
        except Exception:
            return None

    items = []
    data = fmp_get("/stable/most-actives")
    if not isinstance(data, list) or not data:
        data = fmp_get("/api/v3/stock_market/actives")
    if not isinstance(data, list) or not data:
        data = fmp_get("/stable/biggest-gainers")
    if isinstance(data, list):
        for d in data[:12]:
            sym = d.get("symbol")
            if not sym or len(sym) > 6:
                continue
            items.append({
                "symbol": sym,
                "name": d.get("name") or sym,
                "change_pct": parse_pct(d.get("changesPercentage") or d.get("changePercentage")),
                "price": d.get("price"),
            })

    out = {"items": items, "data_timestamp": int(time.time())}
    if items:
        _TREND["data"] = out
        _TREND["ts"] = now
    return jsonify(out)


@app.route("/themes")
def themes():
    out = []
    for k in THEMES:
        out.append({"key": k, "name": THEMES[k]["name"]})
    return jsonify({"themes": out})


_DISABLED_THEMES = {"crypto", "macro", "japan", "india", "korea", "australia", "south_korea"}


@app.route("/discover")
def discover():
    # STABILIZATION: disabled global themes return empty until rebuilt.
    if (request.args.get("theme") or "").strip().lower() in _DISABLED_THEMES:
        return jsonify({"theme": None, "results": [], "disabled": True}), 404
    key = request.args.get("theme", "").strip()
    if key not in THEMES:
        return jsonify({"error": "Unknown theme"}), 404
    cached = get_cache("theme_" + key)
    if cached:
        return jsonify(cached)
    theme = THEMES[key]
    results = []
    for sym in theme["tickers"]:
        r = light_score(sym)
        if r:
            results.append(r)
        else:
            logger.warning("discover theme %s: no data for %s" % (key, sym))
    results.sort(key=lambda x: x["score"], reverse=True)
    out = {
        "key": key,
        "name": theme["name"],
        "explainer": theme["explainer"],
        "why": theme["why"],
        "unknown": theme["unknown"],
        "results": results,
        "data_timestamp": int(time.time()),
    }
    # Only cache when the basket actually scored, so a transient data miss does not stick for the
    # full cache window. An empty result will be retried on the next tap instead of being frozen.
    if results:
        set_cache("theme_" + key, out)
    return jsonify(out)


# CHUNK: shareable read-only snapshot at /s/<symbol>. Standalone HTML, no app shell, no auth.
@app.route("/s/<symbol>")
def snapshot(symbol):
    symbol = (symbol or "").strip().upper()
    d = light_score(symbol)
    e = html.escape
    if not d:
        return Response("<html><body style='font-family:sans-serif;padding:40px;text-align:center'><h2>Snapshot unavailable</h2><p>We could not read " + e(symbol) + " right now. <a href='/'>Open Apex Q</a></p></body></html>", mimetype="text/html")

    v = d.get("verdict", "WATCH")
    vcolor = {"APPROVE": "#0a8f3c", "PASS": "#c1121f", "WATCH": "#b8860b"}.get(v, "#b8860b")
    chg = d.get("change_pct")
    chg_color = "#0a8f3c" if isinstance(chg, (int, float)) and chg >= 0 else "#c1121f"
    chg_txt = (("+" if isinstance(chg, (int, float)) and chg >= 0 else "") + str(chg) + "%") if isinstance(chg, (int, float)) else "n/a"
    name = d.get("name", symbol)
    price = d.get("price", 0)
    pe = d.get("pe_ratio", "N/A")
    tgt = d.get("analyst_target", "N/A")
    mc = fmt_money_py(d.get("market_cap")) if isinstance(d.get("market_cap"), (int, float)) else "n/a"
    up = d.get("upside")

    # Plain English read, written here with simple logic, no AI call.
    if v == "APPROVE":
        para = "The signals on " + str(name) + " lean positive right now. The engine sees more pointing up than down."
    elif v == "PASS":
        para = "The engine is cautious on " + str(name) + " right now. More of the signals point down than up."
    else:
        para = "The signals on " + str(name) + " are mixed right now, so the patient read is to watch and wait for a clearer setup."
    if isinstance(up, (int, float)):
        para += " Analysts see about " + str(abs(up)) + " percent " + ("above" if up >= 0 else "below") + " today's price on average."

    page = """<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>%(sym)s snapshot, Apex Q</title>
<style>
body{margin:0;background:#eef1f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#0f1419;padding:24px;}
.card{max-width:520px;margin:0 auto;background:#fff;border:1px solid #dde2ea;border-radius:18px;padding:26px;box-shadow:0 8px 30px rgba(0,0,0,.06);}
.brand{font-size:13px;font-weight:800;letter-spacing:1px;color:#003eaa;text-transform:uppercase;}
.sym{font-size:34px;font-weight:800;margin:10px 0 2px;letter-spacing:-1px;}
.name{font-size:15px;color:#5b6573;margin-bottom:16px;}
.price{font-size:26px;font-weight:800;}
.chg{font-size:15px;font-weight:700;margin-left:8px;}
.verdict{display:inline-block;margin:16px 0;padding:8px 16px;border-radius:10px;color:#fff;font-weight:800;letter-spacing:1px;font-size:15px;}
.conv{font-size:13px;color:#5b6573;margin-bottom:6px;}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin:18px 0;}
.cell{background:#f6f8fb;border:1px solid #e6ebf2;border-radius:11px;padding:12px;}
.lbl{font-size:11px;color:#5b6573;text-transform:uppercase;letter-spacing:.4px;}
.val{font-size:17px;font-weight:700;margin-top:3px;}
.para{font-size:15px;line-height:1.6;background:#f6f8fb;border-radius:12px;padding:16px;margin:6px 0 4px;}
.foot{font-size:12px;color:#5b6573;line-height:1.6;margin-top:20px;border-top:1px solid #e6ebf2;padding-top:16px;}
.foot a{color:#003eaa;font-weight:700;text-decoration:none;}
</style></head><body>
<div class="card">
  <div class="brand">Apex Q</div>
  <div class="sym">%(sym)s</div>
  <div class="name">%(name)s</div>
  <div><span class="price">$%(price)s</span><span class="chg" style="color:%(chgc)s">%(chg)s</span></div>
  <div class="verdict" style="background:%(vc)s">%(verdict)s</div>
  <div class="conv">How strong the signal is: %(conv)s</div>
  <div class="grid">
    <div class="cell"><div class="lbl">Price vs Earnings</div><div class="val">%(pe)s</div></div>
    <div class="cell"><div class="lbl">What analysts think it is worth</div><div class="val">%(tgt)s</div></div>
    <div class="cell"><div class="lbl">Total company value</div><div class="val">%(mc)s</div></div>
    <div class="cell"><div class="lbl">Move today</div><div class="val" style="color:%(chgc)s">%(chg)s</div></div>
  </div>
  <div class="para">%(para)s</div>
  <div class="foot">Powered by Apex Q, an educational stock intelligence terminal. This is not financial advice. <a href="/">Open the full terminal</a></div>
</div>
</body></html>""" % {
        "sym": e(symbol),
        "name": e(str(name)),
        "price": e(str(price)),
        "chg": e(chg_txt),
        "chgc": chg_color,
        "vc": vcolor,
        "verdict": e(v),
        "conv": e(str(d.get("conviction", ""))),
        "pe": e(str(pe)) if pe not in (None, "N/A") else "n/a",
        "tgt": ("$" + e(str(tgt))) if isinstance(tgt, (int, float)) else "n/a",
        "mc": e(mc),
        "para": e(para),
    }
    return Response(page, mimetype="text/html")


# CHUNK: removed for security. The /debug/ask endpoint exposed partial API keys and is gone.


# ============ BACKEND UPGRADE: New Endpoints ============

@app.route("/api/market-snapshot")
def market_snapshot():
    """Combined real-time indices, top movers, trending names, and market context in one payload.
    Reuses existing caches (light_score, _MOVERS, _TREND, ctx_^GSPC) so it is always fast and
    never makes redundant external calls. Cached for 60 seconds for rapid polling."""
    ckey = "market_snapshot"
    cached = CACHE.get(ckey)
    if cached and (time.time() - cached[1]) < 60:
        return jsonify(cached[0])

    # Indices — reuse light_score which now carries Finnhub real-time fallback
    INDEX_SET = [("^GSPC", "S&P 500"), ("^IXIC", "NASDAQ"), ("^DJI", "DOW JONES"), ("^VIX", "VIX")]
    indices = []
    for sym, label in INDEX_SET:
        r = light_score(sym)
        if r and isinstance(r.get("price"), (int, float)):
            indices.append({
                "symbol": sym, "label": label,
                "price": r.get("price"), "change_pct": r.get("change_pct"),
                "price_source": r.get("price_source", "yfinance"),
            })
        else:
            indices.append({"symbol": sym, "label": label, "price": None, "change_pct": None, "price_source": "N/A"})

    # Top movers — shared cached helper, same source as the Home dashboard and /movers
    md = get_movers_cached()
    movers = {
        "gainers": (md.get("gainers") or [])[:5],
        "losers": (md.get("losers") or [])[:5],
    }

    # Trending — read the existing cache only
    trending = []
    if _TREND.get("data"):
        trending = (_TREND["data"].get("items") or [])[:5]

    # Market context — read cached Gemini analysis only
    market_context = get_cache("ctx_^GSPC")

    payload = {
        "indices": indices,
        "movers": movers,
        "trending": trending,
        "market_context": market_context,
        "data_timestamp": int(time.time()),
    }
    CACHE[ckey] = (payload, time.time())
    return jsonify(payload)


@app.route("/api/custom-signals")
def custom_signals():
    """Proprietary Apex Q Smart Money Composite Signal. Merges insider flow, congressional
    trading, analyst revision momentum, and price momentum into a single composite score
    with a plain-English read. Each component carries its own sub-score and direction so the
    user can see exactly what is driving the composite."""
    symbol = request.args.get("symbol", "").strip().upper()
    if not symbol:
        return jsonify({"error": "No symbol provided"}), 400

    ckey = "custom_signal_" + symbol
    cached = get_cache(ckey)
    if cached is not None:
        return jsonify(cached)

    report = compute_full_report(symbol)
    if not report:
        return jsonify({"error": "Could not analyze " + symbol}), 404

    components = []
    composite_score = 0

    # 1. Insider Flow signal (-3 to +3)
    insider_score = 0
    ins = report.get("insider", [])
    ins_buys = len([t for t in ins if t.get("is_clevel") and t.get("action") == "A" and t.get("kind") != "grant"])
    ins_sells = len([t for t in ins if t.get("is_clevel") and t.get("action") == "D"])
    if ins_buys >= 2:
        insider_score = 3
    elif ins_buys == 1:
        insider_score = 2
    elif ins_sells >= 4:
        insider_score = -3
    elif ins_sells >= 2:
        insider_score = -2
    elif ins_sells == 1:
        insider_score = -1
    composite_score += insider_score
    components.append({
        "name": "Insider Flow",
        "score": insider_score, "max": 3,
        "detail": "%d executive buy(s), %d executive sell(s)" % (ins_buys, ins_sells),
        "signal": "bullish" if insider_score > 0 else ("bearish" if insider_score < 0 else "neutral"),
    })

    # 2. Congressional Flow signal (-2 to +2)
    cong_score = 0
    cong = report.get("congressional", [])
    cong_buys = len([t for t in cong if "purchase" in str(t.get("action", "")).lower()])
    cong_sells = len([t for t in cong if "sale" in str(t.get("action", "")).lower()])
    cong_net = cong_buys - cong_sells
    if cong_net >= 2:
        cong_score = 2
    elif cong_net == 1:
        cong_score = 1
    elif cong_net <= -2:
        cong_score = -1
    composite_score += cong_score
    components.append({
        "name": "Congressional Flow",
        "score": cong_score, "max": 2,
        "detail": "%d buy(s), %d sell(s) by lawmakers" % (cong_buys, cong_sells),
        "signal": "bullish" if cong_score > 0 else ("bearish" if cong_score < 0 else "neutral"),
    })

    # 3. Analyst Momentum (-2 to +2)
    analyst_score = 0
    rec = report.get("recommendation", "hold").upper()
    if rec in ("BUY", "STRONG_BUY"):
        analyst_score = 2
    elif rec in ("SELL", "STRONG_SELL"):
        analyst_score = -2
    fmp_grades = (report.get("fmp") or {}).get("grades", [])
    recent_upgrades = len([g for g in fmp_grades if "up" in str(g.get("action", "")).lower()])
    recent_downgrades = len([g for g in fmp_grades if "down" in str(g.get("action", "")).lower()])
    if recent_upgrades > recent_downgrades and analyst_score < 1:
        analyst_score = 1
    elif recent_downgrades > recent_upgrades and analyst_score > -1:
        analyst_score = -1
    composite_score += analyst_score
    components.append({
        "name": "Analyst Momentum",
        "score": analyst_score, "max": 2,
        "detail": "Rating: %s, %d upgrade(s), %d downgrade(s)" % (rec.replace("_", " "), recent_upgrades, recent_downgrades),
        "signal": "bullish" if analyst_score > 0 else ("bearish" if analyst_score < 0 else "neutral"),
    })

    # 4. Price Momentum (-3 to +3)
    price_score = 0
    chg = report.get("change_pct", 0)
    if isinstance(chg, (int, float)):
        if chg > 3:
            price_score = 2
        elif chg > 0:
            price_score = 1
        elif chg < -8:
            price_score = -3
        elif chg < -3:
            price_score = -2
        elif chg < 0:
            price_score = -1
    composite_score += price_score
    components.append({
        "name": "Price Momentum",
        "score": price_score, "max": 3,
        "detail": "%s%% today" % chg,
        "signal": "bullish" if price_score > 0 else ("bearish" if price_score < 0 else "neutral"),
    })

    # Composite rating
    max_possible = 10  # 3+2+2+3
    if composite_score >= 5:
        rating = "Strong Bullish"
    elif composite_score >= 2:
        rating = "Bullish"
    elif composite_score >= -1:
        rating = "Neutral"
    elif composite_score >= -4:
        rating = "Bearish"
    else:
        rating = "Strong Bearish"

    # Plain English summary
    bull = [c for c in components if c["signal"] == "bullish"]
    bear = [c for c in components if c["signal"] == "bearish"]
    if composite_score >= 2:
        summary = "The Smart Money Composite reads %s on %s. " % (rating, symbol)
        if bull:
            summary += "Bullish signals from: " + ", ".join(c["name"] for c in bull) + ". "
        if bear:
            summary += "Partial caution from: " + ", ".join(c["name"] for c in bear) + ". "
    elif composite_score <= -2:
        summary = "The Smart Money Composite reads %s on %s. " % (rating, symbol)
        if bear:
            summary += "Bearish signals from: " + ", ".join(c["name"] for c in bear) + ". "
        if bull:
            summary += "Some support from: " + ", ".join(c["name"] for c in bull) + ". "
    else:
        summary = "The Smart Money Composite reads %s on %s. Signals are mixed across insider flow, congressional trading, analyst momentum, and price action." % (rating, symbol)

    payload = {
        "symbol": symbol,
        "composite_score": composite_score,
        "max_score": max_possible,
        "rating": rating,
        "summary": summary,
        "components": components,
        "data_timestamp": int(time.time()),
    }
    set_cache(ckey, payload)
    return jsonify(payload)


@app.route("/api/stream/prices")
def stream_prices():
    """Server-Sent Events stream for live prices. Continuously sends updated prices for
    requested symbols every 10 seconds. Uses Finnhub real-time quotes when available,
    yfinance as fallback. The frontend subscribes with:
    new EventSource('/api/stream/prices?symbols=AAPL,MSFT,NVDA')
    Each event is a JSON object with symbol, price, change_pct, source, and timestamp.
    The connection auto-closes after 5 minutes to prevent resource leaks."""
    symbols_param = request.args.get("symbols", "")
    syms = [s.strip().upper() for s in symbols_param.split(",") if s.strip()][:15]
    if not syms:
        syms = ["AAPL", "MSFT", "NVDA", "GOOGL", "TSLA"]

    def generate():
        start = time.time()
        max_duration = 300  # 5 minutes max per connection
        while time.time() - start < max_duration:
            for sym in syms:
                q = get_realtime_price(sym)
                if q:
                    data = json.dumps({
                        "symbol": sym,
                        "price": q["price"],
                        "change_pct": q["change_pct"],
                        "source": q["source"],
                        "timestamp": int(time.time()),
                    })
                    yield "data: %s\n\n" % data
            time.sleep(10)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                             "Connection": "keep-alive"})


# CHUNK: historical OHLCV data for the interactive candlestick chart. Caches heavily so Yahoo is
# not hit too often, with a shorter window for intraday intervals. Reads the cache tuple directly,
# the same pattern the Finnhub and portfolio caches use, so the per interval TTL works correctly.
def detect_patterns(candles):
    """Scan OHLC candles for the ten most common candlestick patterns. Each candle is a dict with
    open, high, low, close. Returns a list of detected patterns with the date of the most recent
    occurrence and a plain English meaning, plus a one line summary. Only the last stretch of
    candles is scanned, since old patterns are not actionable. All logic is standard candlestick
    definitions, tolerance based so real world bars register without being sloppy."""
    if not candles or len(candles) < 3:
        return {"patterns": [], "summary": "Not enough price history to scan for patterns."}

    def body(c):
        return abs(c["close"] - c["open"])

    def rng(c):
        return max(c["high"] - c["low"], 1e-9)

    def upper_wick(c):
        return c["high"] - max(c["open"], c["close"])

    def lower_wick(c):
        return min(c["open"], c["close"]) - c["low"]

    def is_green(c):
        return c["close"] >= c["open"]

    scan = candles[-20:]
    found = {}  # name -> {date, meaning} keeping the most recent occurrence

    meanings = {
        "Doji": "A Doji shows indecision. Buyers and sellers ended in a near tie, which often precedes a reversal or pause.",
        "Hammer": "A Hammer is a bullish reversal pattern that often forms at the bottom of a downtrend, with a long lower wick showing buyers rejected lower prices.",
        "Shooting Star": "A Shooting Star is a bearish reversal pattern that often forms at the top of an uptrend, with a long upper wick showing sellers rejected higher prices.",
        "Bullish Engulfing": "A Bullish Engulfing pattern is a strong bullish signal where a green candle fully swallows the prior red candle, showing buyers taking control.",
        "Bearish Engulfing": "A Bearish Engulfing pattern is a strong bearish signal where a red candle fully swallows the prior green candle, showing sellers taking control.",
        "Morning Star": "A Morning Star is a three candle bullish reversal, a small indecision candle bridging a red candle and a strong green one.",
        "Evening Star": "An Evening Star is a three candle bearish reversal, a small indecision candle bridging a green candle and a strong red one.",
        "Bullish Harami": "A Bullish Harami is a potential bullish reversal where a small green candle sits inside the prior large red candle, hinting the downtrend is stalling.",
        "Bearish Harami": "A Bearish Harami is a potential bearish reversal where a small red candle sits inside the prior large green candle, hinting the uptrend is stalling.",
        "Piercing Line": "A Piercing Line is a bullish reversal where a green candle opens below the prior red candle's low but closes back above its midpoint.",
    }

    def stamp(name, c):
        d = c.get("date") or ""
        found[name] = {"pattern": name, "date": d, "meaning": meanings[name]}

    for i in range(len(scan)):
        c = scan[i]
        b = body(c)
        r = rng(c)
        # Single candle patterns.
        if b <= 0.1 * r:
            stamp("Doji", c)
        if b <= 0.35 * r and lower_wick(c) >= 2 * b and upper_wick(c) <= 0.35 * b + 1e-9:
            stamp("Hammer", c)
        if b <= 0.35 * r and upper_wick(c) >= 2 * b and lower_wick(c) <= 0.35 * b + 1e-9:
            stamp("Shooting Star", c)
        # Two candle patterns.
        if i >= 1:
            p = scan[i - 1]
            pb = body(p)
            if is_green(c) and not is_green(p) and c["close"] >= p["open"] and c["open"] <= p["close"] and b > pb:
                stamp("Bullish Engulfing", c)
            if not is_green(c) and is_green(p) and c["open"] >= p["close"] and c["close"] <= p["open"] and b > pb:
                stamp("Bearish Engulfing", c)
            if is_green(c) and not is_green(p) and b < pb * 0.6 and max(c["open"], c["close"]) <= max(p["open"], p["close"]) and min(c["open"], c["close"]) >= min(p["open"], p["close"]):
                stamp("Bullish Harami", c)
            if not is_green(c) and is_green(p) and b < pb * 0.6 and max(c["open"], c["close"]) <= max(p["open"], p["close"]) and min(c["open"], c["close"]) >= min(p["open"], p["close"]):
                stamp("Bearish Harami", c)
            if is_green(c) and not is_green(p) and c["open"] < p["low"] and c["close"] > (p["open"] + p["close"]) / 2 and c["close"] < p["open"]:
                stamp("Piercing Line", c)
        # Three candle patterns.
        if i >= 2:
            a = scan[i - 2]
            m = scan[i - 1]
            if not is_green(a) and body(m) <= 0.4 * rng(m) and is_green(c) and c["close"] > (a["open"] + a["close"]) / 2 and body(a) > body(m):
                stamp("Morning Star", c)
            if is_green(a) and body(m) <= 0.4 * rng(m) and not is_green(c) and c["close"] < (a["open"] + a["close"]) / 2 and body(a) > body(m):
                stamp("Evening Star", c)

    patterns = sorted(found.values(), key=lambda x: x["date"], reverse=True)
    n = len(patterns)
    if n == 0:
        summary = "No significant patterns detected in the last %d candles." % len(scan)
    else:
        summary = "%d pattern%s detected in the last %d candles." % (n, "" if n == 1 else "s", len(scan))
    return {"patterns": patterns, "summary": summary}


@app.route("/history/<symbol>")
def history(symbol):
    symbol = symbol.strip().upper()
    if not symbol:
        return jsonify({"error": "No symbol"}), 400
    period = request.args.get("period", "1y")
    interval = request.args.get("interval", "1d")
    ttl = 60 if interval in ("1m", "5m", "15m") else 900
    cache_key = "hist_%s_%s_%s" % (symbol, period, interval)
    cached = CACHE.get(cache_key)
    if cached and (time.time() - cached[1]) < ttl:
        return jsonify(cached[0])
    try:
        data = fetch_with_fallback(symbol, period, interval)
        if not data:
            return jsonify({"error": "No data for %s" % symbol}), 404
        # Human readable date on each candle for the pattern scanner, then detect patterns on the
        # daily series only (intraday bars are too noisy for candlestick reversal signals).
        import datetime as _dt
        for cd in data:
            cd["date"] = _dt.datetime.utcfromtimestamp(cd["time"]).strftime("%b %d, %Y")
        pat = detect_patterns(data) if interval == "1d" else {"patterns": [], "summary": "Pattern scan runs on the daily chart."}
        payload = {"symbol": symbol, "data": data, "period": period, "interval": interval,
                   "patterns": pat["patterns"], "pattern_summary": pat["summary"]}
        set_cache(cache_key, payload)
        return jsonify(payload)
    except Exception as e:
        logger.error("History error for %s: %s" % (symbol, e))
        return jsonify({"error": str(e)}), 500


# ============ END New Endpoints ============


# ============ PREMIUM ROUTES ============
@app.route("/usage")
def usage():
    u = current_user()
    return jsonify({
        "tier": (u.get("tier") if u else "free"),
        "premium": is_premium(u),
        "scan": {"used": usage_count("scan", u), "limit": usage_limit("scan")},
        "ask": {"used": usage_count("ask", u), "limit": usage_limit("ask")},
    })


@app.route("/upgrade", methods=["POST"])
def upgrade():
    # Simulated checkout for testing. Sets the current user to premium with no real payment.
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in", "message": "Log in to upgrade."}), 401
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database not available."}), 500
    try:
        cur = conn.cursor()
        cur.execute("UPDATE users SET tier = 'premium' WHERE id = %s", (u["id"],))
        conn.commit()
        cur.close()
        session["tier"] = "premium"
        return jsonify({"ok": True, "tier": "premium"})
    except Exception as e:
        logger.error("upgrade error: %s" % e)
        return jsonify({"error": "Could not upgrade. Try again."}), 500
    finally:
        conn.close()


@app.route("/export")
@require_premium
def export_trades():
    # Premium only. CSV of the insider and congressional trades for one symbol.
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"error": "Add a symbol, like /export?symbol=AAPL"}), 400
    report = compute_full_report(symbol)
    if not report:
        return jsonify({"error": "Could not load %s right now." % symbol}), 404
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["category", "name", "title_or_party", "action", "shares", "value", "date"])
    for t in (report.get("insider") or []):
        w.writerow(["insider", t.get("name", ""), t.get("title", ""),
                    t.get("kind") or t.get("action", ""), t.get("shares", ""),
                    t.get("value", ""), t.get("date", "")])
    for t in (report.get("congressional") or []):
        w.writerow(["congress", t.get("politician", ""), t.get("party", ""),
                    t.get("action", ""), "", t.get("amount", ""), t.get("date", "")])
    out = buf.getvalue()
    buf.close()
    fname = "apexq_%s_trades.csv" % symbol
    return Response(out, mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=%s" % fname})
@app.route("/create-checkout-session", methods=["POST"])
def create_checkout():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in", "message": "Log in to upgrade."}), 401
    if _stripe is None or not STRIPE_PRICE_ID or not os.environ.get("STRIPE_SECRET_KEY"):
        return jsonify({"error": "payments_unavailable", "message": "Payments are not set up yet."}), 503
    try:
        # Named checkout, not session, so it never shadows the Flask session object.
        # Every new subscriber gets a 7 day free trial. The card is collected up front and the first
        # $12.99 charge lands on day 8 unless they cancel, matching the pause style setup chosen in
        # the Stripe dashboard. Change trial_period_days here if the trial length ever changes.
        checkout = _stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
            subscription_data={"trial_period_days": 7},
            success_url=request.host_url + "?upgraded=true",
            cancel_url=request.host_url,
            client_reference_id=str(u["id"]),
        )
        return jsonify({"url": checkout.url})
    except Exception as e:
        logger.error("create_checkout error: %s" % e)
        return jsonify({"error": "checkout_failed", "message": str(e)}), 500


@app.route("/stripe-webhook", methods=["POST"])
def stripe_webhook():
    if _stripe is None:
        return "stripe unavailable", 503
    payload = request.get_data(as_text=True)
    sig = request.headers.get("Stripe-Signature")
    try:
        event = _stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
    except Exception as e:
        logger.error("stripe webhook verify failed: %s" % e)
        return "invalid", 400
    if event.get("type") == "checkout.session.completed":
        sid = event["data"]["object"].get("client_reference_id")
        if sid:
            conn = get_db()
            if conn is not None:
                try:
                    cur = conn.cursor()
                    cur.execute("UPDATE users SET tier = 'premium' WHERE id = %s", (int(sid),))
                    conn.commit()
                    cur.close()
                except Exception as e:
                    logger.error("stripe webhook db error: %s" % e)
                finally:
                    conn.close()
    return "ok", 200


# ============ END PREMIUM ROUTES ============


# ============ PAPER TRADING ============
# Free for every logged in user. Each account starts with 1000000 in virtual cash. Buys and sells
# use the same realtime price source as the rest of the app. Educational simulation only.
PAPER_START_CASH = 1000000.0


def _paper_price(symbol):
    """Best available current price for a symbol as a float, or None. Tries the light realtime
    quote first, then the cached discover score (light_score), then a direct yfinance close, so a
    live price is almost always found and open positions never fall back to a flat zero P&L."""
    rp = get_realtime_price(symbol)
    if rp and rp.get("price"):
        try:
            return float(rp["price"])
        except (TypeError, ValueError):
            pass
    ls = light_score(symbol)
    if ls and ls.get("price"):
        try:
            return float(ls["price"])
        except (TypeError, ValueError):
            pass
    try:
        h = yf.Ticker(symbol).history(period="1d", timeout=10)
        if h is not None and len(h) and "Close" in h:
            v = float(h["Close"].iloc[-1])
            if v == v and v > 0:
                return v
    except Exception:
        pass
    return None


def _paper_chart(rows, start_equity):
    """Reconstruct daily total equity, cash plus marked to market holdings, since the first trade,
    alongside a buy and hold S&P line starting from the same equity. Returns [] if there are no
    trades or prices cannot be loaded. Reuses _price_pairs, which is cached one hour per ticker."""
    if not rows:
        return []
    buy_dates = [r[4].date() for r in rows if r[4] is not None]
    if not buy_dates:
        return []
    start_date = min(buy_dates)
    today = datetime.now().date()
    if start_date >= today:
        start_date = today - timedelta(days=5)
    fetch_end = today + timedelta(days=1)
    spy_pairs = _price_pairs("SPY", start_date, fetch_end)
    if not spy_pairs:
        return []
    spy_dates = [d for d, _ in spy_pairs]
    spy_map = dict(spy_pairs)
    spy_first = spy_pairs[0][1]
    symbols = sorted(set(r[1] for r in rows))
    hist = {}
    for s in symbols:
        hist[s] = dict(_price_pairs(s, start_date, fetch_end))
    trades = []
    for r in rows:
        tid, sym, sh, bp, bd, sold, sp, sd = r
        trades.append({
            "symbol": sym,
            "shares": float(sh),
            "buy_price": float(bp),
            "buy_date": bd.date() if bd else start_date,
            "sold": bool(sold),
            "sell_price": float(sp) if sp is not None else None,
            "sell_date": sd.date() if sd else None,
        })
    out = []
    last_price = {s: None for s in symbols}
    for d in spy_dates:
        for s in symbols:
            if d in hist[s]:
                last_price[s] = hist[s][d]
        cash = start_equity
        holdings = 0.0
        for t in trades:
            if t["buy_date"] <= d:
                cash -= t["shares"] * t["buy_price"]
            sold_by_d = t["sold"] and t["sell_date"] is not None and t["sell_date"] <= d
            if sold_by_d:
                cash += t["shares"] * (t["sell_price"] if t["sell_price"] is not None else t["buy_price"])
            held = (t["buy_date"] <= d) and not sold_by_d
            if held:
                lp = last_price.get(t["symbol"])
                if lp is not None:
                    holdings += t["shares"] * lp
        port = cash + holdings
        spx = start_equity * (spy_map[d] / spy_first)
        out.append({"date": d.isoformat(), "portfolio_value": round(port, 2), "sp500_value": round(spx, 2)})
    return out


# ---------- SnapTrade: read only brokerage connections ----------
# Lets a logged in user connect their real brokerage (Robinhood, Schwab, Fidelity, and so on)
# through SnapTrade's connection portal and see their true holdings inside Apex Q. Strictly read
# only: the connection is requested with read access, and no order or transfer endpoint exists
# anywhere in this file. Signing uses only the standard library, no new dependency. Two things are
# built in for the thirty day self test: a last_synced timestamp on every payload, and a
# discrepancy check that prices every position through Apex Q's own feed and flags any position
# where the two sources disagree by more than 1.5 percent.
import hmac as _hmac
import hashlib as _hashlib
import base64 as _base64
from urllib.parse import urlencode as _st_urlencode

SNAPTRADE_CLIENT_ID = os.environ.get("SNAPTRADE_CLIENT_ID", "").strip()
SNAPTRADE_CONSUMER_KEY = os.environ.get("SNAPTRADE_CONSUMER_KEY", "").strip()
SNAPTRADE_BASE = "https://api.snaptrade.com/api/v1"


def _snaptrade_request(method, path, query_extra=None, body=None):
    """Signed SnapTrade call. Their scheme: HMAC SHA256 of a JSON object holding the request body,
    the full path, and the exact query string, keyed with the consumer key, sent base64 encoded in
    a Signature header. The same query string signed is the one sent on the URL."""
    if not SNAPTRADE_CLIENT_ID or not SNAPTRADE_CONSUMER_KEY:
        return None, "SnapTrade is not configured. Set SNAPTRADE_CLIENT_ID and SNAPTRADE_CONSUMER_KEY."
    q = {"clientId": SNAPTRADE_CLIENT_ID, "timestamp": str(int(time.time()))}
    if query_extra:
        q.update(query_extra)
    query_string = _st_urlencode(q)
    sig_obj = {"content": body, "path": "/api/v1" + path, "query": query_string}
    sig_data = json.dumps(sig_obj, separators=(",", ":"), sort_keys=True)
    sig = _base64.b64encode(
        _hmac.new(SNAPTRADE_CONSUMER_KEY.encode(), sig_data.encode(), _hashlib.sha256).digest()
    ).decode()
    url = SNAPTRADE_BASE + path + "?" + query_string
    try:
        if method == "GET":
            r = requests.get(url, headers={"Signature": sig}, timeout=20)
        elif method == "DELETE":
            r = requests.delete(url, headers={"Signature": sig}, timeout=20)
        else:
            r = requests.post(url, headers={"Signature": sig, "Content-Type": "application/json"}, json=body, timeout=20)
        if r.status_code >= 400:
            logger.error("snaptrade %s %s -> %s %s" % (method, path, r.status_code, r.text[:300]))
            return None, "SnapTrade returned an error (%s)." % r.status_code
        return (r.json() if r.text else {}), None
    except Exception as e:
        logger.error("snaptrade request %s: %s" % (path, e))
        return None, "Could not reach SnapTrade."


def _snaptrade_creds(u):
    """(userId, userSecret, error) for this user, registering with SnapTrade on first use and
    storing the returned per user secret in the users table."""
    db = get_db()
    if db is None:
        return None, None, "Database unavailable."
    cur = db.cursor()
    cur.execute("SELECT snaptrade_secret FROM users WHERE id = %s", (u["id"],))
    row = cur.fetchone()
    st_user_id = "apexq-user-%s" % u["id"]
    if row and row[0]:
        cur.close()
        db.close()
        return st_user_id, row[0], None
    data, err = _snaptrade_request("POST", "/snapTrade/registerUser", body={"userId": st_user_id})
    if err or not data or not data.get("userSecret"):
        cur.close()
        db.close()
        return None, None, err or "Could not register with SnapTrade."
    secret = data["userSecret"]
    cur.execute("UPDATE users SET snaptrade_secret = %s WHERE id = %s", (secret, u["id"]))
    db.commit()
    cur.close()
    db.close()
    return st_user_id, secret, None


def _st_position_symbol(p):
    """Symbol string out of SnapTrade's nested position object, defensively."""
    try:
        s = p.get("symbol") or {}
        inner = s.get("symbol") or {}
        if isinstance(inner, dict):
            return (inner.get("symbol") or inner.get("raw_symbol") or "").upper()
        return str(inner).upper()
    except Exception:
        return ""


@app.route("/snaptrade/connect", methods=["POST"])
def snaptrade_connect():
    u = current_user()
    if not u:
        return jsonify({"error": "Log in first."}), 401
    st_id, st_secret, err = _snaptrade_creds(u)
    if err:
        return jsonify({"error": err}), 502
    data, err = _snaptrade_request(
        "POST", "/snapTrade/login",
        query_extra={"userId": st_id, "userSecret": st_secret},
        body={"connectionType": "read", "immediateRedirect": False},
    )
    if err or not data or not data.get("redirectURI"):
        return jsonify({"error": err or "Could not open the connection portal."}), 502
    return jsonify({"url": data["redirectURI"]})


@app.route("/snaptrade/holdings")
def snaptrade_holdings():
    u = current_user()
    if not u:
        return jsonify({"error": "Log in first."}), 401
    refresh = request.args.get("refresh") == "1"
    ck = "st_hold_%s" % u["id"]
    cached = CACHE.get(ck)
    if cached and not refresh and (time.time() - cached[1]) < 600:
        return jsonify(cached[0])
    db = get_db()
    if db is None:
        return jsonify({"error": "Database unavailable."}), 500
    cur = db.cursor()
    cur.execute("SELECT snaptrade_secret FROM users WHERE id = %s", (u["id"],))
    row = cur.fetchone()
    cur.close()
    db.close()
    if not row or not row[0]:
        return jsonify({"connected": False})
    st_id = "apexq-user-%s" % u["id"]
    secret = row[0]
    accounts, err = _snaptrade_request("GET", "/accounts", query_extra={"userId": st_id, "userSecret": secret})
    if err:
        return jsonify({"error": err}), 502
    if not accounts:
        return jsonify({"connected": False})
    out_accounts = []
    agg = {}
    broker_total = 0.0
    for a in accounts:
        acct_id = a.get("id")
        name = a.get("name") or a.get("institution_name") or "Account"
        number = str(a.get("number") or "")
        masked = ("..." + number[-4:]) if len(number) >= 4 else ""
        total_amt = None
        try:
            total_amt = ((a.get("balance") or {}).get("total") or {}).get("amount")
        except Exception:
            total_amt = None
        out_accounts.append({"name": name, "number": masked, "total": total_amt})
        if not acct_id:
            continue
        positions, perr = _snaptrade_request(
            "GET", "/accounts/%s/positions" % acct_id,
            query_extra={"userId": st_id, "userSecret": secret},
        )
        if perr or not isinstance(positions, list):
            continue
        for p in positions:
            sym = _st_position_symbol(p)
            if not sym:
                continue
            try:
                units = float(p.get("units") or 0)
                price = float(p.get("price") or 0)
            except (TypeError, ValueError):
                continue
            if units == 0:
                continue
            if sym not in agg:
                agg[sym] = {"symbol": sym, "shares": 0.0, "broker_price": price, "value": 0.0}
            agg[sym]["shares"] += units
            agg[sym]["broker_price"] = price
            agg[sym]["value"] += units * price
            broker_total += units * price
    # Discrepancy check: price every position through Apex Q's own feed and flag disagreement.
    flagged = []
    our_total = 0.0
    positions_out = []
    for sym in sorted(agg.keys()):
        h = agg[sym]
        ours = _paper_price(sym)
        drift_pct = None
        if ours is not None and h["broker_price"] > 0:
            drift_pct = round(abs(ours - h["broker_price"]) / h["broker_price"] * 100, 2)
            if drift_pct > 1.5:
                flagged.append(sym)
        our_total += h["shares"] * (ours if ours is not None else h["broker_price"])
        positions_out.append({
            "symbol": sym,
            "shares": round(h["shares"], 4),
            "broker_price": round(h["broker_price"], 2),
            "our_price": round(ours, 2) if ours is not None else None,
            "value": round(h["value"], 2),
            "drift_pct": drift_pct,
        })
    payload = {
        "connected": True,
        "accounts": out_accounts,
        "positions": positions_out,
        "broker_total": round(broker_total, 2),
        "our_total": round(our_total, 2),
        "flagged": flagged,
        "last_synced": int(time.time()),
    }
    CACHE[ck] = (payload, time.time())
    return jsonify(payload)


@app.route("/snaptrade/disconnect", methods=["POST"])
def snaptrade_disconnect():
    u = current_user()
    if not u:
        return jsonify({"error": "Log in first."}), 401
    st_id = "apexq-user-%s" % u["id"]
    db = get_db()
    if db is None:
        return jsonify({"error": "Database unavailable."}), 500
    cur = db.cursor()
    cur.execute("SELECT snaptrade_secret FROM users WHERE id = %s", (u["id"],))
    row = cur.fetchone()
    if row and row[0]:
        _snaptrade_request("DELETE", "/snapTrade/deleteUser", query_extra={"userId": st_id})
    cur.execute("UPDATE users SET snaptrade_secret = NULL WHERE id = %s", (u["id"],))
    db.commit()
    cur.close()
    db.close()
    CACHE.pop("st_hold_%s" % u["id"], None)
    return jsonify({"ok": True})


# ---------- Model Portfolio Builder and Stock Alternatives ----------
# Educational tools for people who do not want to research one stock at a time. The builder scans
# the same universe Discover uses, keeps only names the engine currently rates APPROVE (topping up
# with the strongest WATCH names when needed), tilts the ranking to a chosen risk style, and lays
# out an equal split of a dollar amount. Alternatives suggests higher conviction same sector names
# when someone is viewing a PASS or WATCH stock. Everything is factual signal language, never a
# directive: no output here ever tells anyone what they personally should do with money.
#
# DEVIATION FROM SPEC, DOCUMENTED: the risk tilts were specified on beta, revenue growth, and free
# cash flow yield, but the light snapshot this scan runs on does not carry those fields, and
# forcing 38 full reports per request would hammer the data providers. The tilts instead use real
# signals the snapshot does carry: valuation (PE), dividend yield, and mega cap size for the
# conservative style, and momentum, analyst upside, and higher PE tolerance for the aggressive
# style. When a full report for a symbol is already cached, its Alpha Score is used directly.

RISK_ETFS = {
    "conservative": [
        {"symbol": "SCHD", "name": "Schwab US Dividend Equity ETF", "note": "Steady dividend payers with a value tilt."},
        {"symbol": "VTV", "name": "Vanguard Value ETF", "note": "Large established companies at lower valuations."},
        {"symbol": "AGG", "name": "iShares Core US Aggregate Bond ETF", "note": "Broad bond exposure that cushions stock swings."},
    ],
    "moderate": [
        {"symbol": "SPY", "name": "SPDR S&P 500 ETF", "note": "The 500 largest US companies in one fund."},
        {"symbol": "QQQ", "name": "Invesco QQQ Trust", "note": "The Nasdaq 100, heavy on large technology names."},
        {"symbol": "VTI", "name": "Vanguard Total Stock Market ETF", "note": "The entire US stock market in a single fund."},
    ],
    "aggressive": [
        {"symbol": "QQQ", "name": "Invesco QQQ Trust", "note": "The Nasdaq 100, heavy on large technology names."},
        {"symbol": "ARKK", "name": "ARK Innovation ETF", "note": "High growth innovation companies with big swings."},
        {"symbol": "IWM", "name": "iShares Russell 2000 ETF", "note": "Small US companies, higher growth potential and higher risk."},
    ],
}


def _builder_alpha(r, sym):
    """Alpha Score for ranking. Uses the real Alpha Score when a full report for the symbol is
    already cached, otherwise a documented fallback composite scaled from the light snapshot's own
    engine score plus momentum and analyst upside. Clamped 5 to 95 so a fallback can never claim
    perfect confidence."""
    cached = CACHE.get("full_" + sym)
    if cached and (time.time() - cached[1]) < 900 and cached[0] and cached[0].get("alpha_score") is not None:
        try:
            return int(cached[0]["alpha_score"]), True
        except (TypeError, ValueError):
            pass
    base = 0.0
    try:
        base = float(r.get("score") or 0) * 12
    except (TypeError, ValueError):
        base = 0.0
    try:
        if float(r.get("change_pct") or 0) > 2:
            base += 8
    except (TypeError, ValueError):
        pass
    try:
        if float(r.get("upside") or 0) > 15:
            base += 6
    except (TypeError, ValueError):
        pass
    return int(max(5, min(95, base))), False


def _builder_reason(r, alpha):
    """One factual sentence about what the engine sees, built only from fields that exist."""
    bits = []
    try:
        up = float(r.get("upside") or 0)
        if up >= 15:
            bits.append("analysts see %s percent upside" % round(up, 1))
    except (TypeError, ValueError):
        pass
    try:
        chg = float(r.get("change_pct") or 0)
        if chg > 2:
            bits.append("strong momentum today at plus %s percent" % round(chg, 1))
    except (TypeError, ValueError):
        pass
    try:
        pe = float(r.get("pe_ratio") or 0)
        if 0 < pe < 20:
            bits.append("a reasonable valuation at %s times earnings" % round(pe, 1))
    except (TypeError, ValueError):
        pass
    try:
        dy = float(r.get("div_yield") or 0)
        if dy > 1.5:
            bits.append("a %s percent dividend" % round(dy, 2))
    except (TypeError, ValueError):
        pass
    conv = (r.get("conviction") or "").lower()
    if conv in ("high", "very high"):
        bits.append("%s engine conviction" % conv)
    if not bits:
        bits.append("a positive overall signal mix with an Alpha Score of %s" % alpha)
    return ("Currently rated %s with " % (r.get("verdict") or "APPROVE")) + ", ".join(bits[:2]) + "."


def _builder_candidates(sector):
    """Scored candidates from the scan universe, light snapshot per symbol, individually cached."""
    out = []
    for sym in SCAN_UNIVERSE:
        r = light_score(sym)
        if not r or not r.get("verdict"):
            continue
        if sector and sector.lower() != "all":
            if (r.get("sector") or "").lower() != sector.lower():
                continue
        alpha, real_alpha = _builder_alpha(r, sym)
        out.append((r, alpha, real_alpha))
    return out


def _authoritative_verdict(sym):
    """The verdict + alpha the stock's OWN full report would show -- the single source of truth.
    Prefers a fresh cached full report; computes one only on a miss. Returns (verdict, alpha), or
    (None, None) if it cannot be determined. Exists so the model portfolio can NEVER display a
    verdict that contradicts the stock's full intelligence report (the APPROVE-here / WATCH-there
    bug). compute_full_report already prefers the v2 engine's verdict, so this stays in lockstep
    with what the report shows."""
    try:
        cached = CACHE.get("full_" + sym)
        full = cached[0] if cached and (time.time() - cached[1]) < 900 and cached[0] else None
        if not full:
            full = compute_full_report(sym)
        if not full:
            return None, None
        return full.get("verdict"), full.get("alpha_score")
    except Exception as e:
        logger.warning("authoritative verdict %s: %s" % (sym, e))
        return None, None


@app.route("/portfolio/generate")
def portfolio_generate():
    risk = (request.args.get("risk") or "moderate").strip().lower()
    if risk not in ("conservative", "moderate", "aggressive"):
        risk = "moderate"
    sector = (request.args.get("sector") or "all").strip()
    amount_note = ""
    try:
        amount = float((request.args.get("amount") or "100000").replace(",", ""))
        if amount <= 0:
            amount = 100000.0
            amount_note = "That amount did not look right, so the model used 100,000 dollars."
    except (TypeError, ValueError):
        amount = 100000.0
        amount_note = "That amount did not look right, so the model used 100,000 dollars."
    ck = "portfolio_gen_%s_%s_%s" % (risk, int(amount), sector.lower())
    cached = CACHE.get(ck)
    if cached and (time.time() - cached[1]) < 900:
        return jsonify(cached[0])

    cands = _builder_candidates(sector)
    approve = [(r, a, ra) for (r, a, ra) in cands if r.get("verdict") == "APPROVE"]
    pool = list(approve)
    if len(pool) < 5:
        watch = [(r, a, ra) for (r, a, ra) in cands if r.get("verdict") == "WATCH"]
        watch.sort(key=lambda x: (float(x[0].get("score") or 0), x[1]), reverse=True)
        for w in watch:
            try:
                if float(w[0].get("score") or 0) >= 3:
                    pool.append(w)
            except (TypeError, ValueError):
                continue
            if len(pool) >= 5:
                break
    if len(pool) < 5:
        payload = {"error": "Not enough strong signals right now to build a balanced model. Try a broader sector or check back later."}
        return jsonify(payload), 200

    # Risk tilt: additive bonuses on real snapshot fields, then rank.
    ranked = []
    for (r, alpha, real_alpha) in pool:
        adj = alpha
        try:
            pe = float(r.get("pe_ratio") or 0)
        except (TypeError, ValueError):
            pe = 0
        try:
            dy = float(r.get("div_yield") or 0)
        except (TypeError, ValueError):
            dy = 0
        try:
            mc = float(r.get("market_cap") or 0)
        except (TypeError, ValueError):
            mc = 0
        try:
            chg = float(r.get("change_pct") or 0)
        except (TypeError, ValueError):
            chg = 0
        try:
            up = float(r.get("upside") or 0)
        except (TypeError, ValueError):
            up = 0
        if risk == "conservative":
            if 0 < pe < 20:
                adj += 5
            if dy > 1.5:
                adj += 5
            if mc > 1e11:
                adj += 5
        elif risk == "aggressive":
            if chg > 2:
                adj += 5
            if up > 20:
                adj += 5
            if pe > 30 or pe == 0:
                adj += 3
        ranked.append((adj, alpha, r))
    ranked.sort(key=lambda x: x[0], reverse=True)

    count = min(len(ranked), 5 if amount < 500 else 8, 10)
    # RECONCILE WITH THE FULL REPORT before showing anything. light_score is fine for RANKING the
    # universe fast, but the verdict and alpha SHOWN must match each stock's own full report -- else
    # the table can say APPROVE on a name the report holds at WATCH. Walk the ranked list top down,
    # pull the authoritative full-report verdict, and keep only true APPROVEs until we have enough.
    # Bounded so the heavy engine runs on at most a handful of names.
    # Reconcile each candidate against its OWN full report so the verdict shown can never
    # contradict the report, but STAY POPULATED. We attach the true full-report verdict, then
    # rank genuine APPROVEs first and fill the rest with the strongest WATCH names (labelled
    # WATCH by _builder_reason). Heavy computes are bounded; PASS / unconfirmable names are dropped.
    confirmed = []
    scan_cap = min(len(ranked), count + 12)
    approves = 0
    for (adj, alpha, r) in ranked[:scan_cap]:
        sym = r.get("symbol") or ""
        auth_v, auth_a = _authoritative_verdict(sym)
        if auth_v not in ("APPROVE", "WATCH"):
            continue  # PASS or couldn't confirm -> never show it in the model
        rr = dict(r)
        rr["verdict"] = auth_v
        use_alpha = auth_a if isinstance(auth_a, (int, float)) else alpha
        tier = 0 if auth_v == "APPROVE" else 1
        confirmed.append((tier, adj, use_alpha, rr))
        if tier == 0:
            approves += 1
        if approves >= count:
            break
    if not confirmed:
        return jsonify({"error": "Not enough confirmed signals right now to build a model. Try a broader sector or check back later."}), 200
    # APPROVE first, then WATCH; keep the light-score ranking within each tier.
    confirmed.sort(key=lambda c: (c[0], -c[1]))
    picks = [(adj, use_alpha, rr) for (_t, adj, use_alpha, rr) in confirmed[:count]]
    count = len(picks)
    # Conviction weighted, risk aware allocation, the way professional managers tilt a book:
    # weight factor = (alpha / 100) * (1 / max(beta, 0.5)). Higher conviction and lower volatility
    # earn a larger slice. Beta comes from the cached full report when one exists; when it is
    # missing, that component is neutral 1.0, so the tilt still works on conviction alone. Any
    # single position is capped at 20 percent with the excess redistributed proportionally, and if
    # no usable factors exist at all, the split falls back to plain equal weight.
    factors = []
    for (adj, alpha, r) in picks:
        alpha_part = (alpha / 100.0) if alpha else 1.0
        beta_part = 1.0
        symq = r.get("symbol") or ""
        cached_full = CACHE.get("full_" + symq)
        if cached_full and (time.time() - cached_full[1]) < 900 and cached_full[0]:
            try:
                b = float(cached_full[0].get("beta"))
                beta_part = 1.0 / max(b, 0.5)
            except (TypeError, ValueError):
                beta_part = 1.0
        factors.append(alpha_part * beta_part)
    total_f = sum(factors)
    if total_f > 0:
        weights = [f / total_f for f in factors]
    else:
        weights = [1.0 / count] * count
    # Cap at 20 percent, redistribute the excess proportionally among the uncapped, repeat until
    # stable. With five names the cap makes the split exactly equal, which is correct behavior.
    cap = 0.20
    for _ in range(10):
        over = [i for i, w in enumerate(weights) if w > cap + 1e-9]
        if not over:
            break
        excess = sum(weights[i] - cap for i in over)
        for i in over:
            weights[i] = cap
        under = [i for i, w in enumerate(weights) if w < cap - 1e-9]
        under_total = sum(weights[i] for i in under)
        if not under or under_total <= 0:
            break
        for i in under:
            weights[i] += excess * (weights[i] / under_total)
    stocks = []
    for idx, (adj, alpha, r) in enumerate(picks):
        w = weights[idx]
        stocks.append({
            "symbol": r.get("symbol"),
            "name": r.get("name") or r.get("symbol"),
            "price": r.get("price"),
            "verdict": r.get("verdict"),
            "alpha_score": alpha,
            "sector": r.get("sector") or "",
            "pe_ratio": r.get("pe_ratio") if r.get("pe_ratio") is not None else "N/A",
            "dividend_yield": r.get("div_yield") if r.get("div_yield") is not None else "N/A",
            "allocation_dollars": round(amount * w, 2),
            "allocation_pct": round(w * 100, 1),
            "reason": _builder_reason(r, alpha),
        })
    payload = {
        "risk": risk,
        "amount": amount,
        "amount_note": amount_note,
        "sector": sector,
        "count": count,
        "stocks": stocks,
        "etf_suggestions": RISK_ETFS.get(risk, RISK_ETFS["moderate"]),
        "generated_at": int(time.time()),
        "disclaimer": "This is a computer generated educational model based on Apex Q's scoring engine. It is not personalized financial advice. All investments carry risk.",
    }
    CACHE[ck] = (payload, time.time())
    return jsonify(payload)


def _alt_positive_signals(r, alpha, sym):
    """Factual positive signals for one APPROVE alternative, only from fields that exist. The
    cached full report enriches with moat and growth when available; the light snapshot supplies
    the rest. Never invents a figure."""
    sig = {}
    cached = CACHE.get("full_" + sym)
    full = cached[0] if cached and (time.time() - cached[1]) < 900 and cached[0] else None
    if full:
        moat = (full.get("apex_moat") or {}).get("rating")
        if moat:
            sig["moat"] = str(moat) + " moat"
        try:
            rg = float(full.get("revenue_growth"))
            if rg > 10:
                sig["growth"] = "Revenue growth %s percent" % round(rg, 1)
        except (TypeError, ValueError):
            pass
    try:
        up = float(r.get("upside") or 0)
        if up >= 10:
            sig["upside"] = "Analysts see %s percent upside" % round(up, 1)
    except (TypeError, ValueError):
        pass
    try:
        chg = float(r.get("change_pct") or 0)
        if chg > 2:
            sig["momentum"] = "Up %s percent today" % round(chg, 1)
    except (TypeError, ValueError):
        pass
    try:
        pe = float(r.get("pe_ratio") or 0)
        if 0 < pe < 20:
            sig["valuation"] = "Reasonable valuation at %s times earnings" % round(pe, 1)
    except (TypeError, ValueError):
        pass
    try:
        dy = float(r.get("div_yield") or 0)
        if dy > 1.5:
            sig["dividend"] = "Pays a %s percent dividend" % round(dy, 2)
    except (TypeError, ValueError):
        pass
    conv = (r.get("conviction") or "").lower()
    if conv in ("high", "very high"):
        sig["conviction"] = "Engine conviction is " + conv
    if not sig:
        sig["signal"] = "Positive overall signal mix, Alpha Score %s" % alpha
    return sig


def _alt_current_negatives(symbol, cur):
    """What is dragging on the stock being viewed, for the left side of the comparison. Pulls the
    real warning flags and insider selling from the cached full report when present, otherwise
    builds honest basics from the light snapshot."""
    neg = []
    cached = CACHE.get("full_" + symbol)
    full = cached[0] if cached and (time.time() - cached[1]) < 900 and cached[0] else None
    if full:
        for f in (full.get("flags") or [])[:3]:
            if isinstance(f, dict) and f.get("text"):
                neg.append(f["text"])
        try:
            isv = float(full.get("insider_sell_value") or 0)
            if isv >= 1e6:
                neg.append("Recent insider selling of %.1f million dollars" % (isv / 1e6))
        except (TypeError, ValueError):
            pass
        try:
            rg = float(full.get("revenue_growth"))
            if rg < 0:
                neg.append("Revenue is shrinking, %s percent growth" % round(rg, 1))
        except (TypeError, ValueError):
            pass
    try:
        chg = float(cur.get("change_pct") or 0)
        if chg < -2:
            neg.append("Down %s percent today" % round(abs(chg), 1))
    except (TypeError, ValueError):
        pass
    try:
        pe = float(cur.get("pe_ratio") or 0)
        if pe > 35:
            neg.append("A rich valuation at %s times earnings" % round(pe, 1))
    except (TypeError, ValueError):
        pass
    if not neg:
        neg.append("Mixed signals with no clear positive edge right now")
    # De-duplicate while keeping order, cap at four so the card stays readable.
    seen = set()
    out = []
    for n in neg:
        if n not in seen:
            seen.add(n)
            out.append(n)
        if len(out) >= 4:
            break
    return out


@app.route("/portfolio/alternatives")
def portfolio_alternatives():
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"alternatives": []})
    ck = "port_alt_" + symbol
    cached = CACHE.get(ck)
    if cached and (time.time() - cached[1]) < 900:
        return jsonify(cached[0])
    cur = light_score(symbol)
    if not cur or not cur.get("verdict"):
        payload = {"alternatives": []}
        CACHE[ck] = (payload, time.time())
        return jsonify(payload)
    if cur.get("verdict") == "APPROVE":
        payload = {"alternatives": []}
        CACHE[ck] = (payload, time.time())
        return jsonify(payload)
    sector = cur.get("sector") or ""
    cands = _builder_candidates("all")
    # APPROVE only, never WATCH or PASS. Alpha above 50, or an engine score of 4 plus when the
    # alpha is the fallback composite rather than a real cached Alpha Score.
    def strong(r, a, real_alpha):
        if r.get("verdict") != "APPROVE" or r.get("symbol") == symbol:
            return False
        if a > 50:
            return True
        if not real_alpha:
            try:
                return float(r.get("score") or 0) >= 4
            except (TypeError, ValueError):
                return False
        return False
    same = [(r, a) for (r, a, ra) in cands if strong(r, a, ra) and sector and (r.get("sector") or "") == sector]
    # Variety: the user's own brokerage and tracked holdings join the candidate pool, so
    # alternatives are not the same handful of scan universe names every time. The same sector
    # list takes its top 12 by alpha and shuffles, so repeat visits see fresh strong names.
    # DEVIATION, DOCUMENTED: the spec suggested scanning all S&P 500 constituents, but a cold
    # scan of 500 symbols is minutes of provider calls per request; holdings plus the universe
    # keeps it fast and personal.
    try:
        uu = current_user()
        if uu:
            dbh = get_db()
            if dbh is not None:
                curh = dbh.cursor()
                curh.execute("SELECT DISTINCT symbol FROM holdings WHERE user_id = %s", (uu["id"],))
                extra_syms = [rw[0] for rw in curh.fetchall() if rw and rw[0]]
                curh.close()
                dbh.close()
                known = set(r.get("symbol") for (r, a, ra) in cands)
                for es in extra_syms:
                    if es in known or es == symbol:
                        continue
                    er = light_score(es)
                    if er and er.get("verdict"):
                        ea, ereal = _builder_alpha(er, es)
                        cands.append((er, ea, ereal))
                        same_ok = strong(er, ea, ereal) and sector and (er.get("sector") or "") == sector
                        if same_ok:
                            same.append((er, ea))
    except Exception as e:
        logger.error("alternatives holdings pool: %s" % e)
    same.sort(key=lambda x: x[1], reverse=True)
    top_pool = same[:12]
    random.shuffle(top_pool)
    picks = top_pool[:5]
    cross_sector = False
    if len(picks) < 2:
        allap = [(r, a) for (r, a, ra) in cands if strong(r, a, ra)]
        allap.sort(key=lambda x: x[1], reverse=True)
        picks = allap[:5]
        cross_sector = True
    alts = []
    for (r, a) in picks:
        alts.append({
            "symbol": r.get("symbol"),
            "name": r.get("name") or r.get("symbol"),
            "price": r.get("price"),
            "verdict": r.get("verdict"),
            "alpha_score": a,
            "sector": r.get("sector") or "",
            "positive_signals": _alt_positive_signals(r, a, r.get("symbol") or ""),
        })
    payload = {
        "alternatives": alts,
        "sector": "" if cross_sector else sector,
        "cross_sector": cross_sector,
        "for_symbol": symbol,
        "current": {
            "symbol": symbol,
            "name": cur.get("name") or symbol,
            "verdict": cur.get("verdict"),
            "negatives": _alt_current_negatives(symbol, cur),
        },
    }
    CACHE[ck] = (payload, time.time())
    return jsonify(payload)


# ---------- SnapTrade portfolio analysis and auto populate ----------
# Turns a linked brokerage into a fully analyzed Apex Q portfolio with zero manual typing. The
# aggregator captures each position's weighted average cost from the broker. Analyze runs every
# holding through the engine's light snapshot, enriched with the full report's moat, insider, and
# congressional reads whenever one is already cached, and labels known funds as ETF. Sync upserts
# every holding into the same holdings table the manual Portfolio tracker reads, so the built in
# summary card, allocation warnings, and table light up automatically.
#
# DEVIATION FROM SPEC, DOCUMENTED: forcing compute_full_report for every holding on every sync
# would fire dozens of provider calls and take half a minute or more per request, so enrichment
# uses the cached full report when present and the light snapshot otherwise. Tapping any symbol
# runs the full report, which caches it, so the analysis gets richer as the person uses the app.
# Premium gating honors FREE_LIMITS_ENABLED: while limits are paused for building, everyone sees
# the analysis; at launch it locks to premium automatically with the rest of the paywall.

_ETF_SET = {"SPY", "QQQ", "VTI", "VOO", "IVV", "DIA", "IWM", "SCHD", "VTV", "AGG", "ARKK", "VYM",
            "VUG", "VEA", "VWO", "BND", "GLD", "SLV", "XLK", "XLF", "XLE", "XLV", "XLY", "XLP",
            "XLI", "XLU", "XLB", "XLRE", "XLC", "SPCX", "JEPI", "JEPQ", "SCHG", "SCHB", "QQQM"}


def _snaptrade_agg(u):
    """Aggregated positions for a user's linked accounts: shares, broker price, value, and the
    weighted average purchase cost per symbol. Returns (agg, accounts, error)."""
    db = get_db()
    if db is None:
        return None, None, "Database unavailable."
    cur = db.cursor()
    cur.execute("SELECT snaptrade_secret FROM users WHERE id = %s", (u["id"],))
    row = cur.fetchone()
    cur.close()
    db.close()
    if not row or not row[0]:
        return None, None, "not_connected"
    st_id = "apexq-user-%s" % u["id"]
    secret = row[0]
    accounts, err = _snaptrade_request("GET", "/accounts", query_extra={"userId": st_id, "userSecret": secret})
    if err:
        return None, None, err
    if not accounts:
        # The secret still exists, so the person IS linked. An empty account list here means the
        # brokerage authorization went stale or SnapTrade returned nothing this moment, NOT that
        # they disconnected. Report it distinctly so the app never wipes a real connection from view.
        return None, None, "needs_refresh"
    agg = {}
    for a in accounts:
        acct_id = a.get("id")
        if not acct_id:
            continue
        positions, perr = _snaptrade_request(
            "GET", "/accounts/%s/positions" % acct_id,
            query_extra={"userId": st_id, "userSecret": secret},
        )
        if perr or not isinstance(positions, list):
            continue
        for p in positions:
            sym = _st_position_symbol(p)
            if not sym:
                continue
            try:
                units = float(p.get("units") or 0)
                price = float(p.get("price") or 0)
            except (TypeError, ValueError):
                continue
            if units == 0:
                continue
            try:
                apc = float(p.get("average_purchase_price") or 0)
            except (TypeError, ValueError):
                apc = 0.0
            if apc <= 0:
                apc = price
            if sym not in agg:
                agg[sym] = {"symbol": sym, "shares": 0.0, "broker_price": price, "cost_total": 0.0}
            agg[sym]["shares"] += units
            agg[sym]["broker_price"] = price
            agg[sym]["cost_total"] += units * apc
    return agg, accounts, None


def _snaptrade_analysis(agg):
    """Every holding through the engine: verdict, Alpha Score, moat, insider and congressional
    reads, profit and loss, and a one sentence educational summary per position."""
    holdings = []
    counts = {"APPROVE": 0, "WATCH": 0, "PASS": 0, "ETF": 0}
    total_value = 0.0
    total_cost = 0.0
    total_day = 0.0
    for sym in sorted(agg.keys()):
        h = agg[sym]
        shares = h["shares"]
        avg_cost = (h["cost_total"] / shares) if shares else 0.0
        r = light_score(sym) or {}
        price = None
        try:
            price = float(r.get("price")) if r.get("price") is not None else None
        except (TypeError, ValueError):
            price = None
        if price is None:
            price = h["broker_price"]
        value = shares * price
        cost = shares * avg_cost
        gl = value - cost
        gl_pct = (gl / cost * 100) if cost > 0 else 0.0
        try:
            chg = float(r.get("change_pct") or 0)
            total_day += value * (chg / (100 + chg)) if chg > -100 else 0
        except (TypeError, ValueError):
            pass
        is_etf = sym in _ETF_SET
        verdict = "ETF" if is_etf else (r.get("verdict") or "N/A")
        alpha, _real = _builder_alpha(r, sym) if r else (None, False)
        moat = ""
        insider_note = ""
        congress_note = ""
        expense_ratio = None
        cached = CACHE.get("full_" + sym)
        full = cached[0] if cached and (time.time() - cached[1]) < 900 and cached[0] else None
        if full:
            moat = (full.get("apex_moat") or {}).get("rating") or ""
            try:
                isv = float(full.get("insider_sell_value") or 0)
                if isv >= 1e6:
                    insider_note = "Selling %.1fM" % (isv / 1e6)
            except (TypeError, ValueError):
                pass
            cong = full.get("congressional")
            trades = cong.get("trades") if isinstance(cong, dict) else (cong if isinstance(cong, list) else None)
            if isinstance(trades, list) and trades:
                congress_note = "%s recent trades" % len(trades)
            if is_etf:
                er = full.get("expense_ratio")
                if er is not None:
                    expense_ratio = er
        if is_etf:
            summary = "A fund holding. Funds spread risk across many names, so verdicts apply to single stocks, not to this."
        elif r and r.get("verdict"):
            summary = _builder_reason(r, alpha or 0)
        else:
            summary = "The engine could not score this one right now."
        if verdict in counts:
            counts[verdict] += 1
        total_value += value
        total_cost += cost
        holdings.append({
            "symbol": sym,
            "name": r.get("name") or sym,
            "shares": round(shares, 4),
            "cost_basis": round(avg_cost, 2),
            "current_price": round(price, 2),
            "current_value": round(value, 2),
            "gain_loss": round(gl, 2),
            "gain_loss_pct": round(gl_pct, 2),
            "verdict": verdict,
            "alpha_score": alpha,
            "moat": moat,
            "insider_note": insider_note,
            "congress_note": congress_note,
            "expense_ratio": expense_ratio,
            "sector": r.get("sector") or "",
            "summary": summary,
        })
    return {
        "holdings": holdings,
        "summary": {
            "total_holdings": len(holdings),
            "approve": counts["APPROVE"],
            "watch": counts["WATCH"],
            "pass": counts["PASS"],
            "etf": counts["ETF"],
            "total_value": round(total_value, 2),
            "total_gain_loss": round(total_value - total_cost, 2),
            "total_day_change": round(total_day, 2),
        },
        "last_synced": int(time.time()),
    }


def _snaptrade_gate():
    """Premium gate that honors the master pause. Returns (user, error_response)."""
    u = current_user()
    if not u:
        return None, (jsonify({"error": "Log in first."}), 401)
    if FREE_LIMITS_ENABLED and not is_premium(u):
        return None, (jsonify({"error": "premium_required"}), 402)
    return u, None


@app.route("/snaptrade/analyze")
def snaptrade_analyze():
    u, gate = _snaptrade_gate()
    if gate:
        return gate
    ck = "st_analyze_%s" % u["id"]
    cached = CACHE.get(ck)
    if cached and (time.time() - cached[1]) < 300 and request.args.get("refresh") != "1":
        return jsonify(cached[0])
    agg, accounts, err = _snaptrade_agg(u)
    if err == "not_connected":
        return jsonify({"connected": False})
    if err == "needs_refresh":
        return jsonify({"connected": True, "needs_refresh": True})
    if err:
        return jsonify({"error": err}), 502
    payload = _snaptrade_analysis(agg)
    payload["connected"] = True
    CACHE[ck] = (payload, time.time())
    return jsonify(payload)


@app.route("/snaptrade/sync", methods=["POST"])
def snaptrade_sync():
    u, gate = _snaptrade_gate()
    if gate:
        return gate
    agg, accounts, err = _snaptrade_agg(u)
    if err == "not_connected":
        return jsonify({"connected": False})
    if err == "needs_refresh":
        # Do NOT touch stored holdings here. An empty pull must never overwrite the real portfolio
        # the person already synced. Tell them to reconnect the brokerage instead.
        return jsonify({"connected": True, "needs_refresh": True})
    if err:
        return jsonify({"error": err}), 502
    if not agg:
        return jsonify({"connected": True, "needs_refresh": True})
    # Auto populate the built in Portfolio tracker: upsert every brokerage holding, so the summary
    # card, allocation math, and warnings reflect the real account with no manual typing.
    db = get_db()
    if db is None:
        return jsonify({"error": "Database unavailable."}), 500
    cur = db.cursor()
    for sym, h in agg.items():
        shares = round(h["shares"], 4)
        avg_cost = round((h["cost_total"] / h["shares"]) if h["shares"] else 0.0, 2)
        cur.execute("SELECT id FROM holdings WHERE user_id = %s AND symbol = %s", (u["id"], sym))
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE holdings SET shares = %s, avg_cost = %s WHERE id = %s", (shares, avg_cost, row[0]))
        else:
            cur.execute("INSERT INTO holdings (user_id, symbol, shares, avg_cost) VALUES (%s, %s, %s, %s)",
                        (u["id"], sym, shares, avg_cost))
    db.commit()
    cur.close()
    db.close()
    CACHE.pop("portfolio_" + str(u["id"]), None)
    CACHE.pop("st_analyze_%s" % u["id"], None)
    payload = _snaptrade_analysis(agg)
    payload["connected"] = True
    payload["synced_to_portfolio"] = len(agg)
    CACHE["st_analyze_%s" % u["id"]] = (payload, time.time())
    return jsonify(payload)


# ---------- Neural text to speech ----------
# The browser's built in speech engine sounds robotic on many devices, so read aloud audio is
# generated server side with Gemini's TTS model through the same GEMINI_KEY the app already uses.
# Gemini returns raw 24kHz mono PCM, which gets wrapped in a WAV header with the standard library
# so every browser can play it. A small in memory cache keeps repeat reads of the same text free,
# and the frontend falls back to the device voice if this route ever fails, so read aloud can
# never break outright.

_TTS_CACHE = {}


def _pcm_to_wav(pcm, rate=24000):
    import struct
    header = struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF", 36 + len(pcm), b"WAVE", b"fmt ", 16,
        1, 1, rate, rate * 2, 2, 16, b"data", len(pcm),
    )
    return header + pcm


@app.route("/tts", methods=["POST"])
def tts_route():
    u = current_user()
    if not u:
        return jsonify({"error": "Log in first."}), 401
    body = request.get_json(silent=True) or {}
    text = (body.get("text") or "").strip()
    if not text:
        return jsonify({"error": "Nothing to read."}), 400
    text = text[:4000]
    key = os.environ.get("GEMINI_KEY", "").strip()
    if not key:
        return jsonify({"error": "tts_unavailable"}), 503
    import hashlib as _h
    ck = _h.sha1(text.encode()).hexdigest()
    hit = _TTS_CACHE.get(ck)
    if hit and (time.time() - hit[1]) < 900:
        return Response(hit[0], mimetype="audio/wav")
    try:
        # Try current TTS models in order. Preview model names get renamed and retired by Google,
        # which silently breaks TTS and drops the app back to the robotic device voice. Trying the
        # stable name first, then known preview names, keeps the neural voice working across those
        # changes. Voice is Achernar, a warmer female voice than the default.
        voice = os.environ.get("TTS_VOICE", "Achernar").strip() or "Achernar"
        # Model list, newest first. The 2.5 preview names this used to call have been RETIRED by
        # Google -- which is precisely the failure the comment above predicted, and precisely why
        # the site fell back to the device's robotic voice. gemini-2.0-flash-exp was never a TTS
        # model at all and has been dropped from the chain.
        # Override with TTS_MODEL in Railway when Google renames things again.
        models = [m.strip() for m in os.environ.get(
            "TTS_MODEL",
            "gemini-3.1-flash-tts-preview,gemini-2.5-flash-preview-tts,gemini-2.5-pro-preview-tts"
        ).split(",") if m.strip()]

        # Gemini TTS is INSTRUCTION-DRIVEN: it is a language model that speaks, not a pipe that
        # reads. Handed raw text with no directive it can return nothing at all. Telling it how to
        # speak is also how we control the delivery -- this is what makes it not sound like a robot.
        style = os.environ.get(
            "TTS_STYLE",
            "Read the following aloud in a warm, natural, conversational voice, at an unhurried pace, "
            "like a knowledgeable friend explaining it across a table. Do not sound like a news anchor "
            "or a robot. Do not add any words of your own.",
        )
        prompt = style + "\n\n" + text

        r = None
        used = None
        for m in models:
            try:
                r = requests.post(
                    "https://generativelanguage.googleapis.com/v1beta/models/" + m + ":generateContent?key=" + key,
                    json={
                        "contents": [{"parts": [{"text": prompt}]}],
                        "generationConfig": {
                            "responseModalities": ["AUDIO"],
                            "speechConfig": {"voiceConfig": {"prebuiltVoiceConfig": {"voiceName": voice}}},
                        },
                    },
                    timeout=60,
                )
                if r.status_code == 200:
                    used = m
                    break
                logger.error("tts model %s -> %s: %s" % (m, r.status_code, r.text[:160]))
            except Exception as me:
                logger.error("tts model %s error: %s" % (m, me))
        if not used or r is None or r.status_code != 200:
            return jsonify({"error": "tts_unavailable"}), 503
        logger.info("tts served via %s voice %s" % (used, voice))
        data = r.json()
        b64 = data["candidates"][0]["content"]["parts"][0]["inlineData"]["data"]
        import base64 as _b64
        wav = _pcm_to_wav(_b64.b64decode(b64))
        # Keep the audio cache small; each clip can be a few megabytes.
        if len(_TTS_CACHE) > 20:
            _TTS_CACHE.clear()
        _TTS_CACHE[ck] = (wav, time.time())
        return Response(wav, mimetype="audio/wav")
    except Exception as e:
        logger.error("tts: %s" % e)
        return jsonify({"error": "tts_unavailable"}), 503


@app.route("/api/user/premium-status")
def api_premium_status():
    """Single fact for the frontend paywall: is this account premium. Called on page load and after
    login to decide whether the report blur applies. No other data leaves this route."""
    return jsonify({"is_premium": is_premium(current_user())})


@app.route("/quote")
def quote():
    """Light live quote for one symbol: company name, price, and day change. Used by the practice
    view to confirm what a ticker is before buying it. Name comes from the yfinance symbol search
    and is cached a day; the price rides the same realtime path as everything else."""
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol or len(symbol) > 10:
        return jsonify({"error": "Enter a ticker symbol."}), 400
    name = ""
    ckey = "qname_" + symbol
    cached_name = CACHE.get(ckey)
    if cached_name and (time.time() - cached_name[1]) < 86400:
        name = cached_name[0]
    else:
        try:
            s = yf.Search(symbol, max_results=3)
            for x in s.quotes:
                if (x.get("symbol") or "").upper() == symbol:
                    name = x.get("longname") or x.get("shortname") or ""
                    break
            CACHE[ckey] = (name, time.time())
        except Exception:
            pass
    rp = get_realtime_price(symbol)
    if not rp or not rp.get("price"):
        px = _paper_price(symbol)
        if px is None:
            return jsonify({"error": "No live price found for " + symbol + "."}), 404
        return jsonify({"symbol": symbol, "name": name, "price": round(px, 2), "change_pct": None})
    return jsonify({
        "symbol": symbol,
        "name": name,
        "price": rp.get("price"),
        "change_pct": rp.get("change_pct") if isinstance(rp.get("change_pct"), (int, float)) else None,
    })


@app.route("/paper/portfolio")
def paper_portfolio():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    uid = u["id"]
    ckey = "paper_%s" % uid
    cached = CACHE.get(ckey)
    if cached and (time.time() - cached[1]) < 60:
        return jsonify(cached[0])
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Paper trading is not available right now."}), 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(paper_cash, %s) FROM users WHERE id = %s", (PAPER_START_CASH, uid))
        crow = cur.fetchone()
        cash = float(crow[0]) if crow else PAPER_START_CASH
        cur.execute("SELECT id, symbol, shares, buy_price, buy_date, sold, sell_price, sell_date "
                    "FROM paper_trades WHERE user_id = %s ORDER BY buy_date ASC", (uid,))
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        logger.error("paper_portfolio db error: %s" % e)
        conn.close()
        return jsonify({"error": "Could not load your paper portfolio."}), 500
    conn.close()

    open_positions = []
    closed_trades = []
    held_symbols = set()
    for r in rows:
        tid, sym, sh, bp, bd, sold, sp, sd = r
        sh = float(sh)
        bp = float(bp)
        if sold:
            spf = float(sp) if sp is not None else bp
            closed_trades.append({
                "trade_id": tid, "symbol": sym, "shares": round(sh, 4),
                "buy_price": round(bp, 2), "sell_price": round(spf, 2),
                "pnl": round((spf - bp) * sh, 2),
                "return_pct": round((spf / bp - 1) * 100, 2) if bp else 0,
                "buy_date": bd.isoformat() if bd else None,
                "sell_date": sd.isoformat() if sd else None,
            })
        else:
            held_symbols.add(sym)
            open_positions.append({"trade_id": tid, "symbol": sym, "shares": sh, "buy_price": bp, "buy_date": bd})

    prices = {}
    for sym in held_symbols:
        p = _paper_price(sym)
        if p is not None:
            prices[sym] = p

    holdings_value = 0.0
    holdings_out = []
    for pos in open_positions:
        cp = prices.get(pos["symbol"], pos["buy_price"])
        val = pos["shares"] * cp
        holdings_value += val
        holdings_out.append({
            "trade_id": pos["trade_id"], "symbol": pos["symbol"], "shares": round(pos["shares"], 4),
            "buy_price": round(pos["buy_price"], 2), "current_price": round(cp, 2),
            "value": round(val, 2), "pnl": round((cp - pos["buy_price"]) * pos["shares"], 2),
            "return_pct": round((cp / pos["buy_price"] - 1) * 100, 2) if pos["buy_price"] else 0,
            "buy_date": pos["buy_date"].isoformat() if pos["buy_date"] else None,
        })

    total_value = cash + holdings_value
    total_pnl = total_value - PAPER_START_CASH
    total_return = (total_value / PAPER_START_CASH - 1) * 100 if PAPER_START_CASH else 0
    closed_wins = len([t for t in closed_trades if t["pnl"] > 0])
    win_rate = (closed_wins / len(closed_trades) * 100) if closed_trades else 0.0

    chart_data = _paper_chart(rows, PAPER_START_CASH)
    sp500_return = 0.0
    if chart_data and chart_data[-1].get("sp500_value"):
        sp500_return = (chart_data[-1]["sp500_value"] / PAPER_START_CASH - 1) * 100
    # Guarantee at least two points so a line can always draw. When there are no trades yet, show
    # a flat baseline from the account creation date to today: the portfolio still sits at starting
    # cash, and the S&P line is a buy and hold of SPY with that same cash from the creation date.
    if not chart_data or len(chart_data) < 2:
        try:
            cur2 = None
            conn2 = get_db()
            created = None
            if conn2 is not None:
                c2 = conn2.cursor()
                c2.execute("SELECT created_at FROM users WHERE id = %s", (uid,))
                cr = c2.fetchone()
                c2.close()
                conn2.close()
                if cr and cr[0]:
                    created = cr[0].date()
            today = datetime.now().date()
            if not created or created >= today:
                created = today - timedelta(days=30)
            spy_pairs = _price_pairs("SPY", created, today + timedelta(days=1))
            start_str = created.isoformat()
            end_str = today.isoformat()
            if spy_pairs and len(spy_pairs) >= 2:
                spy_first = spy_pairs[0][1]
                spy_last = spy_pairs[-1][1]
                sp_start = PAPER_START_CASH
                sp_end = round(PAPER_START_CASH * (spy_last / spy_first), 2) if spy_first else PAPER_START_CASH
                start_str = spy_pairs[0][0].isoformat()
                end_str = spy_pairs[-1][0].isoformat()
            else:
                sp_start = PAPER_START_CASH
                sp_end = PAPER_START_CASH
            chart_data = [
                {"date": start_str, "portfolio_value": PAPER_START_CASH, "sp500_value": sp_start},
                {"date": end_str, "portfolio_value": round(total_value, 2), "sp500_value": sp_end},
            ]
            if sp_start:
                sp500_return = (sp_end / sp_start - 1) * 100
        except Exception as e:
            logger.error("paper baseline chart: %s" % e)

    result = {
        "cash": round(cash, 2),
        "holdings_value": round(holdings_value, 2),
        "total_value": round(total_value, 2),
        "starting_cash": PAPER_START_CASH,
        "total_pnl": round(total_pnl, 2),
        "total_return": round(total_return, 2),
        "sp500_return": round(sp500_return, 2),
        "win_rate": round(win_rate, 1),
        "open_count": len(holdings_out),
        "closed_count": len(closed_trades),
        "positions": holdings_out,
        "closed": list(reversed(closed_trades))[:10],
        "chart_data": chart_data,
    }
    set_cache(ckey, result)
    return jsonify(result)


@app.route("/paper/buy", methods=["POST"])
def paper_buy():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "").strip().upper()
    try:
        shares = float(data.get("shares") or 0)
    except (TypeError, ValueError):
        shares = 0
    if not symbol:
        return jsonify({"error": "Enter a symbol."}), 400
    if shares <= 0:
        return jsonify({"error": "Enter a share count greater than zero."}), 400
    price = _paper_price(symbol)
    if price is None or price <= 0:
        return jsonify({"error": "Could not get a price for %s." % symbol}), 400
    cost = shares * price
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Paper trading is not available right now."}), 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(paper_cash, %s) FROM users WHERE id = %s", (PAPER_START_CASH, u["id"]))
        row = cur.fetchone()
        cash = float(row[0]) if row else 0.0
        if cost > cash:
            cur.close()
            return jsonify({"error": "Not enough virtual cash. Cost $%.2f, you have $%.2f." % (cost, cash)}), 400
        cur.execute("UPDATE users SET paper_cash = COALESCE(paper_cash, %s) - %s WHERE id = %s",
                    (PAPER_START_CASH, cost, u["id"]))
        cur.execute("INSERT INTO paper_trades (user_id, symbol, shares, buy_price) VALUES (%s, %s, %s, %s)",
                    (u["id"], symbol, shares, price))
        conn.commit()
        cur.close()
        CACHE.pop("paper_%s" % u["id"], None)
        return jsonify({"ok": True, "symbol": symbol, "shares": shares, "price": round(price, 2), "cost": round(cost, 2)})
    except Exception as e:
        logger.error("paper_buy error: %s" % e)
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify({"error": "Could not complete the buy. Try again."}), 500
    finally:
        conn.close()


@app.route("/paper/sell", methods=["POST"])
def paper_sell():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    data = request.get_json(silent=True) or {}
    try:
        trade_id = int(data.get("trade_id") or 0)
    except (TypeError, ValueError):
        trade_id = 0
    if not trade_id:
        return jsonify({"error": "Missing trade id."}), 400
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Paper trading is not available right now."}), 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT symbol, shares, sold FROM paper_trades WHERE id = %s AND user_id = %s", (trade_id, u["id"]))
        row = cur.fetchone()
        if not row:
            cur.close()
            return jsonify({"error": "Trade not found."}), 404
        if row[2]:
            cur.close()
            return jsonify({"error": "That position is already closed."}), 400
        symbol = row[0]
        shares = float(row[1])
        price = _paper_price(symbol)
        if price is None or price <= 0:
            cur.close()
            return jsonify({"error": "Could not get a price to sell %s." % symbol}), 400
        proceeds = shares * price
        cur.execute("UPDATE paper_trades SET sold = true, sell_price = %s, sell_date = NOW() WHERE id = %s", (price, trade_id))
        cur.execute("UPDATE users SET paper_cash = COALESCE(paper_cash, %s) + %s WHERE id = %s",
                    (PAPER_START_CASH, proceeds, u["id"]))
        conn.commit()
        cur.close()
        CACHE.pop("paper_%s" % u["id"], None)
        return jsonify({"ok": True, "symbol": symbol, "shares": shares, "price": round(price, 2), "proceeds": round(proceeds, 2)})
    except Exception as e:
        logger.error("paper_sell error: %s" % e)
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify({"error": "Could not complete the sell. Try again."}), 500
    finally:
        conn.close()
# ============ END PAPER TRADING ============



# ---------- Cache warmer ----------
# Pre-fills the in-memory cache for the scan universe so common tickers never show a spinner.
# Runs once at startup and every 30 minutes after, on a background thread. Any failure is logged
# and skipped so a provider outage or missing database can never take the app down.
def warm_cache():
    warmed = 0
    for sym in SCAN_UNIVERSE:
        try:
            light_score(sym)
            compute_full_report(sym)
            warmed += 1
        except Exception as e:
            logger.error("warm_cache %s: %s" % (sym, e))
    logger.info("warm_cache: %d of %d tickers warmed" % (warmed, len(SCAN_UNIVERSE)))


@app.route("/admin/warm-cache")
def admin_warm_cache():
    threading.Thread(target=warm_cache, daemon=True).start()
    return jsonify({"warming": True, "universe": len(SCAN_UNIVERSE)})


def _schedule_warm():
    try:
        threading.Thread(target=warm_cache, daemon=True).start()
    except Exception as e:
        logger.error("schedule_warm: %s" % e)
    finally:
        t = threading.Timer(1800, _schedule_warm)
        t.daemon = True
        t.start()


# Kick the first warm shortly after boot so it never blocks startup or health checks.
try:
    _boot_timer = threading.Timer(8, _schedule_warm)
    _boot_timer.daemon = True
    _boot_timer.start()
except Exception as _e:
    logger.error("warm boot: %s" % _e)




# =========================================================================== #
# SEC EDGAR DATA PIPELINE (inlined)
# Fetches insider transactions (Forms 3/4/5) and financials (10-K/10-Q) directly
# from SEC EDGAR and stores them in PostgreSQL. Owns its own tables. Uses a
# private logger (sec_logger) so it never disturbs the app's global logger.
# =========================================================================== #
from psycopg2.extras import Json as _SecJson
from datetime import date as _sec_date
from xml.etree import ElementTree as _SecET

sec_logger = logging.getLogger("sec_edgar_pipeline")

SEC_USER_AGENT = "ApexQ/1.0 support@apexq.io"
MAX_REQUESTS_PER_SECOND = 8
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_DAILY_INDEX_BASE = "https://www.sec.gov/Archives/edgar/daily-index"
SEC_COMPANY_FACTS_BASE = "https://data.sec.gov/api/xbrl/companyfacts"
SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives"
SEC_INSIDER_FORMS = {"3", "4", "5", "3/A", "4/A", "5/A"}
SEC_FINANCIAL_FORMS = {"10-K", "10-Q", "10-K/A", "10-Q/A"}
# 13D = activist intent. 13G = PASSIVE (Vanguard/BlackRock file these on nearly every large
# cap). Both are collected; only 13D is ever scored. See _is_activist_form.
_SEC_ACTIVIST_FORMS = {"SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"}
_SEC_COMMENT_FORMS = {"UPLOAD", "CORRESP"}
SEC_TRANSACTION_CODE_MAP = {
    "P": "buy", "S": "sell", "A": "grant", "M": "option_exercise", "X": "option_exercise",
    "F": "tax_withholding", "G": "other", "D": "other", "C": "other", "V": "other", "J": "other",
}


class _SecTokenBucket:
    """At most `rate` requests per second, blocking when dry. Keeps us under SEC's ceiling."""
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


_sec_bucket = _SecTokenBucket(MAX_REQUESTS_PER_SECOND)


def _sec_get(url, expect_json=False, timeout=30, retries=3):
    """Polite SEC GET: rate limited, proper User-Agent, retry with backoff. Never raises."""
    headers = {"User-Agent": SEC_USER_AGENT, "Accept-Encoding": "gzip, deflate"}
    for attempt in range(retries):
        _sec_bucket.take()
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r
            if r.status_code == 404:
                sec_logger.warning("404 not found: %s", url); return None
            if r.status_code == 429:
                wait = 2 ** attempt
                sec_logger.warning("429 from SEC, backing off %ss: %s", wait, url)
                time.sleep(wait); continue
            sec_logger.warning("HTTP %s on %s (attempt %s)", r.status_code, url, attempt + 1)
        except requests.RequestException as e:
            sec_logger.warning("request error on %s: %s (attempt %s)", url, e, attempt + 1)
        time.sleep(1 + attempt)
    sec_logger.error("giving up on %s after %s attempts", url, retries)
    return None


def _sec_connect():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set; cannot connect to PostgreSQL.")
    if dsn.startswith("postgres://"):
        dsn = dsn.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(dsn)


def sec_create_tables(conn):
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS sec_filings_raw (
        id SERIAL PRIMARY KEY, cik TEXT, ticker TEXT, form_type TEXT, filing_date DATE,
        accession_number TEXT UNIQUE, raw_json JSONB, parse_status TEXT DEFAULT 'parsed',
        fetched_at TIMESTAMP DEFAULT NOW())""")
    cur.execute("""CREATE TABLE IF NOT EXISTS insider_transactions_sec (
        id SERIAL PRIMARY KEY, filing_id INTEGER REFERENCES sec_filings_raw(id) ON DELETE CASCADE,
        insider_name TEXT, insider_title TEXT, transaction_date DATE, transaction_code TEXT,
        shares NUMERIC, price NUMERIC, ownership_type TEXT, derived_ticker TEXT,
        created_at TIMESTAMP DEFAULT NOW())""")
    cur.execute("""CREATE TABLE IF NOT EXISTS company_financials_sec (
        id SERIAL PRIMARY KEY, filing_id INTEGER REFERENCES sec_filings_raw(id) ON DELETE CASCADE,
        ticker TEXT, period_end_date DATE, revenue NUMERIC, net_income NUMERIC, eps NUMERIC,
        total_assets NUMERIC, total_liabilities NUMERIC, operating_cash_flow NUMERIC,
        capex NUMERIC, source_text TEXT, created_at TIMESTAMP DEFAULT NOW())""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ins_sec_ticker ON insider_transactions_sec(derived_ticker)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fin_sec_ticker ON company_financials_sec(ticker)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_accession ON sec_filings_raw(accession_number)")
    conn.commit(); cur.close()
    sec_logger.info("sec tables ready")


def _sec_filing_exists(conn, accession_number):
    cur = conn.cursor()
    cur.execute("SELECT id FROM sec_filings_raw WHERE accession_number = %s", (accession_number,))
    row = cur.fetchone(); cur.close()
    return row[0] if row else None


def _sec_last_filing_date(conn):
    cur = conn.cursor()
    cur.execute("SELECT MAX(filing_date) FROM sec_filings_raw")
    row = cur.fetchone(); cur.close()
    return row[0] if row and row[0] else None


_sec_ticker_cache = {"map": None, "loaded_at": 0}


def sec_load_cik_ticker_map():
    if _sec_ticker_cache["map"] and (time.time() - _sec_ticker_cache["loaded_at"]) < 3600:
        return _sec_ticker_cache["map"]
    r = _sec_get(SEC_TICKERS_URL)
    if r is None:
        sec_logger.error("could not load company_tickers.json")
        return _sec_ticker_cache["map"] or {}
    try:
        data = r.json()
    except ValueError:
        sec_logger.error("company_tickers.json not valid JSON")
        return _sec_ticker_cache["map"] or {}
    mapping = {}
    for _, row in data.items():
        cik = str(row.get("cik_str", "")).zfill(10)
        ticker = row.get("ticker")
        if cik and ticker:
            mapping[cik] = ticker.upper()
    _sec_ticker_cache["map"] = mapping
    _sec_ticker_cache["loaded_at"] = time.time()
    sec_logger.info("loaded %s CIK->ticker mappings", len(mapping))
    return mapping


def sec_cik_to_ticker(cik):
    return sec_load_cik_ticker_map().get(str(cik).zfill(10))


def _sec_quarter_for(d):
    return (d.month - 1) // 3 + 1


def sec_fetch_daily_master(target_date):
    y = target_date.year; q = _sec_quarter_for(target_date); stamp = target_date.strftime("%Y%m%d")
    url = "%s/%s/QTR%s/master.%s.idx" % (SEC_DAILY_INDEX_BASE, y, q, stamp)
    r = _sec_get(url)
    if r is None:
        return []
    filings = []
    for line in r.text.splitlines():
        parts = line.split("|")
        if len(parts) != 5:
            continue
        cik, company, form_type, date_filed, filename = parts
        if not cik.isdigit():
            continue
        form_type = form_type.strip()
        if (form_type not in SEC_INSIDER_FORMS and form_type not in SEC_FINANCIAL_FORMS
                and form_type not in _SEC_ACTIVIST_FORMS and form_type not in _SEC_COMMENT_FORMS):
            continue
        m = re.search(r"(\d{10}-\d{2}-\d{6})", filename)
        accession = m.group(1) if m else None
        filings.append({"cik": cik, "company": company.strip(), "form_type": form_type,
                        "filing_date": target_date, "filename": filename.strip(),
                        "accession_number": accession})
    sec_logger.info("%s: %s relevant filings", stamp, len(filings))
    return filings


def _sec_strip_ns(tag):
    return tag.split("}")[-1] if "}" in tag else tag


def _sec_find_text(node, path):
    for child in node.iter():
        if _sec_strip_ns(child.tag) == path:
            for sub in child:
                if _sec_strip_ns(sub.tag) == "value":
                    return (sub.text or "").strip()
            return (child.text or "").strip()
    return None


def _sec_to_num(val):
    if val is None or val == "":
        return None
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return None


def sec_parse_form4_xml(xml_text):
    try:
        root = _SecET.fromstring(xml_text)
    except _SecET.ParseError as e:
        sec_logger.warning("form4 xml parse error: %s", e)
        return None
    owner_name = None; owner_title = None
    for node in root.iter():
        tag = _sec_strip_ns(node.tag)
        if tag == "rptOwnerName" and node.text:
            owner_name = node.text.strip()
        if tag == "officerTitle" and node.text:
            owner_title = node.text.strip()
    if not owner_title:
        for node in root.iter():
            tag = _sec_strip_ns(node.tag)
            if tag == "isDirector" and (node.text or "").strip() in ("1", "true"):
                owner_title = "Director"
            if tag == "isOfficer" and (node.text or "").strip() in ("1", "true") and not owner_title:
                owner_title = "Officer"
    transactions = []

    def _collect(table_tag, is_derivative):
        for node in root.iter():
            if _sec_strip_ns(node.tag) != table_tag:
                continue
            for txn in node:
                if _sec_strip_ns(txn.tag) not in ("nonDerivativeTransaction", "derivativeTransaction"):
                    continue
                transactions.append({
                    "transaction_code": _sec_find_text(txn, "transactionCode"),
                    "transaction_date": _sec_find_text(txn, "transactionDate"),
                    "shares": _sec_to_num(_sec_find_text(txn, "transactionShares")),
                    "price": _sec_to_num(_sec_find_text(txn, "transactionPricePerShare")),
                    "ownership_type": _sec_find_text(txn, "directOrIndirectOwnership"),
                    "is_derivative": is_derivative,
                })
    _collect("nonDerivativeTable", False)
    _collect("derivativeTable", True)
    return {"owner_name": owner_name, "owner_title": owner_title, "transactions": transactions}


def _sec_extract_xml(txt_body):
    m = re.search(r"(<ownershipDocument>.*?</ownershipDocument>)", txt_body, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1)
    for block in re.findall(r"<XML>(.*?)</XML>", txt_body, re.DOTALL | re.IGNORECASE):
        if "ownershipDocument" in block:
            return block.strip()
    return None


def sec_classify_transaction(code):
    if not code:
        return "other"
    return SEC_TRANSACTION_CODE_MAP.get(code.strip().upper(), "other")


def _sec_submission_txt_url(cik, accession_number):
    acc_nodash = accession_number.replace("-", "")
    return "%s/edgar/data/%s/%s/%s.txt" % (SEC_ARCHIVES_BASE, int(cik), acc_nodash, accession_number)


def _sec_safe_date(s):
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def sec_process_insider_filing(conn, filing):
    accession = filing.get("accession_number")
    if not accession:
        sec_logger.warning("insider filing missing accession"); return
    if _sec_filing_exists(conn, accession):
        return
    cik = filing["cik"]; ticker = sec_cik_to_ticker(cik)
    r = _sec_get(_sec_submission_txt_url(cik, accession))
    cur = conn.cursor()
    if r is None:
        cur.execute("INSERT INTO sec_filings_raw (cik, ticker, form_type, filing_date, accession_number, raw_json, parse_status) "
                    "VALUES (%s,%s,%s,%s,%s,%s,'unparsed') ON CONFLICT (accession_number) DO NOTHING",
                    (cik, ticker, filing["form_type"], filing["filing_date"], accession, _SecJson({"error": "fetch_failed"})))
        conn.commit(); cur.close(); return
    xml_text = _sec_extract_xml(r.text)
    parsed = sec_parse_form4_xml(xml_text) if xml_text else None
    status = "parsed" if parsed and parsed["transactions"] else "unparsed"
    cur.execute("INSERT INTO sec_filings_raw (cik, ticker, form_type, filing_date, accession_number, raw_json, parse_status) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (accession_number) DO NOTHING RETURNING id",
                (cik, ticker, filing["form_type"], filing["filing_date"], accession,
                 _SecJson({"accession": accession, "form": filing["form_type"], "has_xml": bool(xml_text)}), status))
    row = cur.fetchone()
    if not row:
        conn.commit(); cur.close(); return
    filing_id = row[0]
    if parsed and parsed["transactions"]:
        for t in parsed["transactions"]:
            category = sec_classify_transaction(t["transaction_code"])
            cur.execute("INSERT INTO insider_transactions_sec (filing_id, insider_name, insider_title, "
                        "transaction_date, transaction_code, shares, price, ownership_type, derived_ticker) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (filing_id, parsed["owner_name"], parsed["owner_title"], _sec_safe_date(t["transaction_date"]),
                         (t["transaction_code"] or "") + ":" + category, t["shares"], t["price"],
                         t["ownership_type"], ticker))
        sec_logger.info("stored %s insider txns for %s (%s)", len(parsed["transactions"]), ticker or cik, accession)
    else:
        sec_logger.info("insider filing %s stored unparsed", accession)
    conn.commit(); cur.close()


SEC_XBRL_CONCEPTS = {
    "revenue": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"],
    "net_income": ["NetIncomeLoss", "ProfitLoss"],
    "eps": ["EarningsPerShareDiluted", "EarningsPerShareBasic"],
    "total_assets": ["Assets"], "total_liabilities": ["Liabilities"],
    "operating_cash_flow": ["NetCashProvidedByUsedInOperatingActivities"],
    "capex": ["PaymentsToAcquirePropertyPlantAndEquipment"],
}


def _sec_latest_xbrl_value(facts, concepts, period_end=None):
    for concept in concepts:
        node = facts.get("us-gaap", {}).get(concept)
        if not node:
            continue
        units = node.get("units", {})
        series = units.get("USD") or units.get("USD/shares") or next(iter(units.values()), [])
        if not series:
            continue
        dated = [x for x in series if x.get("end")]
        dated.sort(key=lambda x: x["end"], reverse=True)
        if period_end:
            for x in dated:
                if x["end"] == period_end:
                    return x.get("val"), x["end"]
        if dated:
            return dated[0].get("val"), dated[0]["end"]
    return None, None


def sec_process_financial_filing(conn, filing):
    accession = filing.get("accession_number")
    if not accession or _sec_filing_exists(conn, accession):
        return
    cik = filing["cik"]; ticker = sec_cik_to_ticker(cik)
    cur = conn.cursor()
    r = _sec_get("%s/CIK%s.json" % (SEC_COMPANY_FACTS_BASE, str(cik).zfill(10)), expect_json=True)
    if r is None:
        cur.execute("INSERT INTO sec_filings_raw (cik, ticker, form_type, filing_date, accession_number, raw_json, parse_status) "
                    "VALUES (%s,%s,%s,%s,%s,%s,'unparsed') ON CONFLICT (accession_number) DO NOTHING",
                    (cik, ticker, filing["form_type"], filing["filing_date"], accession, _SecJson({"error": "no_xbrl"})))
        conn.commit(); cur.close(); return
    try:
        facts_root = r.json().get("facts", {})
    except ValueError:
        facts_root = {}
    metrics = {}; period_end = None
    for col, concepts in SEC_XBRL_CONCEPTS.items():
        val, pend = _sec_latest_xbrl_value(facts_root, concepts)
        metrics[col] = val
        if pend and not period_end:
            period_end = pend
    got_any = any(v is not None for v in metrics.values())
    status = "parsed" if got_any else "unparsed"
    cur.execute("INSERT INTO sec_filings_raw (cik, ticker, form_type, filing_date, accession_number, raw_json, parse_status) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (accession_number) DO NOTHING RETURNING id",
                (cik, ticker, filing["form_type"], filing["filing_date"], accession,
                 _SecJson({"accession": accession, "form": filing["form_type"], "xbrl": got_any}), status))
    row = cur.fetchone()
    if not row:
        conn.commit(); cur.close(); return
    filing_id = row[0]
    if got_any:
        cur.execute("INSERT INTO company_financials_sec (filing_id, ticker, period_end_date, revenue, net_income, "
                    "eps, total_assets, total_liabilities, operating_cash_flow, capex, source_text) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (filing_id, ticker, _sec_safe_date(period_end), metrics["revenue"], metrics["net_income"],
                     metrics["eps"], metrics["total_assets"], metrics["total_liabilities"],
                     metrics["operating_cash_flow"], metrics["capex"], "xbrl:companyfacts"))
        sec_logger.info("stored financials for %s (%s)", ticker or cik, accession)
    else:
        sec_logger.info("financial filing %s stored unparsed", accession)
    conn.commit(); cur.close()


def fetch_sec_edgar_data(days_back=1, max_filings=None):
    """Main entry point. Discover and process new SEC filings. Safe from a cron route."""
    started = time.time()
    conn = _sec_connect()
    try:
        sec_create_tables(conn)
        sec_load_cik_ticker_map()
        last = _sec_last_filing_date(conn)
        today = _sec_date.today()
        start_day = today - timedelta(days=days_back)
        if last:
            start_day = min(start_day, last + timedelta(days=1))
        summary = {"insider": 0, "financial": 0, "skipped_existing": 0, "days": 0, "errors": 0}
        processed = 0; day = start_day
        while day <= today:
            summary["days"] += 1
            try:
                filings = sec_fetch_daily_master(day)
            except Exception as e:
                sec_logger.error("daily index %s: %s", day, e); summary["errors"] += 1
                day += timedelta(days=1); continue
            for f in filings:
                if max_filings and processed >= max_filings:
                    day = today; break
                accession = f.get("accession_number")
                if accession and _sec_filing_exists(conn, accession):
                    summary["skipped_existing"] += 1; continue
                try:
                    if f["form_type"] in _SEC_ACTIVIST_FORMS:
                        sec_process_activist_filing(conn, f); summary["activist"] = summary.get("activist", 0) + 1
                    elif f["form_type"] in _SEC_COMMENT_FORMS:
                        sec_process_comment_letter(conn, f); summary["comment_letters"] = summary.get("comment_letters", 0) + 1
                    elif f["form_type"] in SEC_INSIDER_FORMS:
                        sec_process_insider_filing(conn, f); summary["insider"] += 1
                    elif f["form_type"] in SEC_FINANCIAL_FORMS:
                        sec_process_financial_filing(conn, f); summary["financial"] += 1
                    processed += 1
                except Exception as e:
                    sec_logger.error("error processing %s: %s", accession, e)
                    summary["errors"] += 1; conn.rollback()
            day += timedelta(days=1)
        summary["elapsed_sec"] = round(time.time() - started, 1)
        sec_logger.info("sec run complete: %s", summary)
        return summary
    finally:
        conn.close()
# =========================================================================== #
# END SEC EDGAR DATA PIPELINE
# =========================================================================== #


@app.route("/cron/sec-edgar")
def cron_sec_edgar():
    # Token gate: reject anything whose token does not match CRON_SECRET.
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    # Prefer the inlined pipeline function if present; otherwise import the standalone module
    # locally (inside the route) so a missing module never breaks app startup. Either way we call
    # the same entry point and return its summary directly so a manual browser run shows results.
    try:
        runner = fetch_sec_edgar_data
    except NameError:
        runner = None
    if runner is None:
        try:
            from sec_edgar_pipeline import fetch_sec_edgar_data as runner
        except Exception as e:
            logger.error("sec-edgar import failed: %s" % e)
            return jsonify({"error": "sec pipeline unavailable"}), 503
    try:
        result = runner(days_back=1)
        return jsonify(result)
    except Exception as e:
        logger.error("sec-edgar run error: %s" % e)
        return jsonify({"error": str(e)}), 500



# =========================================================================== #
# ANALYST SNAPSHOT + REVISION MOMENTUM ENGINE
#
# Why this exists: estimate-revision momentum is the most predictive forward
# factor in the literature, and it is the thing Bloomberg actually sells. No
# vendor will sell it cheaply, because it is not a field you fetch — it is a
# field you accumulate. So we accumulate it ourselves.
#
# Every time anyone analyzes a stock, we write that day's analyst state to
# analyst_snapshots. A daily cron sweeps the universe so coverage does not
# depend on user traffic. One row per symbol per day, forever. After 30 days we
# can compute 30-day revision momentum. After a year we own a dataset that
# cannot be bought. The universe grows on its own: every symbol anyone looks up
# is added to it and swept from then on.
#
# Integrity rule: until a symbol has real history, the revision score reports
# "building" and returns None. It never invents a number to look complete.
# =========================================================================== #

SNAP_SEED_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AVGO", "BRK-B", "LLY",
    "JPM", "V", "XOM", "UNH", "MA", "COST", "HD", "PG", "JNJ", "WMT",
    "NFLX", "CRM", "BAC", "AMD", "ORCL", "KO", "PEP", "MRK", "ABBV", "CVX",
    "ADBE", "TMO", "MCD", "CSCO", "ACN", "LIN", "INTC", "PFE", "DIS", "QCOM",
    "SPY", "QQQ", "SOFI", "PLTR", "COIN", "HOOD", "SMCI", "MU", "ARM", "UBER",
]


def _snap_f(v):
    """Coerce to float or None. Never raises."""
    try:
        if v is None:
            return None
        f = float(v)
        if f != f or f in (float("inf"), float("-inf")):
            return None
        return f
    except Exception:
        return None


def _snap_i(v):
    f = _snap_f(v)
    return int(f) if f is not None else None


def snap_create_tables():
    """Idempotent. Safe to call on every boot."""
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS analyst_snapshots ("
            "id SERIAL PRIMARY KEY,"
            "symbol TEXT NOT NULL,"
            "snap_date DATE NOT NULL,"
            "price DOUBLE PRECISION,"
            "market_cap DOUBLE PRECISION,"
            "target_mean DOUBLE PRECISION,"
            "target_high DOUBLE PRECISION,"
            "target_low DOUBLE PRECISION,"
            "analyst_count INTEGER,"
            "reco_key TEXT,"
            "reco_mean DOUBLE PRECISION,"
            "trailing_pe DOUBLE PRECISION,"
            "forward_pe DOUBLE PRECISION,"
            "eps_forward DOUBLE PRECISION,"
            "eps_trailing DOUBLE PRECISION,"
            "earnings_growth DOUBLE PRECISION,"
            "revenue_growth DOUBLE PRECISION,"
            "sector TEXT,"
            "created_at TIMESTAMP DEFAULT NOW(),"
            "CONSTRAINT uq_snap UNIQUE (symbol, snap_date))"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_snap_sym_date ON analyst_snapshots(symbol, snap_date DESC)")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS snapshot_universe ("
            "symbol TEXT PRIMARY KEY,"
            "added_at TIMESTAMP DEFAULT NOW(),"
            "last_snap DATE,"
            "hits INTEGER DEFAULT 1,"
            "active BOOLEAN DEFAULT true)"
        )
        for s in SNAP_SEED_UNIVERSE:
            cur.execute(
                "INSERT INTO snapshot_universe (symbol) VALUES (%s) ON CONFLICT (symbol) DO NOTHING",
                (s,),
            )
        conn.commit()
        cur.close()
        logger.info("snap: tables ready, universe seeded")
    except Exception as e:
        logger.error("snap_create_tables: %s" % e)
    finally:
        conn.close()


def snap_universe_add(symbol):
    """Every symbol anyone analyzes joins the daily sweep from then on."""
    if not symbol:
        return
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO snapshot_universe (symbol) VALUES (%s) "
            "ON CONFLICT (symbol) DO UPDATE SET hits = snapshot_universe.hits + 1, active = true",
            (symbol.upper(),),
        )
        conn.commit()
        cur.close()
    except Exception as e:
        logger.warning("snap_universe_add %s: %s" % (symbol, e))
    finally:
        conn.close()


def record_analyst_snapshot(symbol, info):
    """Write today's analyst state for one symbol. One row per symbol per day.

    Re-running on the same day overwrites rather than duplicating, so this is safe to call from
    the analyze path AND the cron without producing double rows.
    """
    if not symbol or not isinstance(info, dict) or not info:
        return False
    conn = get_db()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO analyst_snapshots "
            "(symbol, snap_date, price, market_cap, target_mean, target_high, target_low,"
            " analyst_count, reco_key, reco_mean, trailing_pe, forward_pe, eps_forward,"
            " eps_trailing, earnings_growth, revenue_growth, sector) "
            "VALUES (%s, CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (symbol, snap_date) DO UPDATE SET "
            "price=EXCLUDED.price, market_cap=EXCLUDED.market_cap, target_mean=EXCLUDED.target_mean,"
            "target_high=EXCLUDED.target_high, target_low=EXCLUDED.target_low,"
            "analyst_count=EXCLUDED.analyst_count, reco_key=EXCLUDED.reco_key,"
            "reco_mean=EXCLUDED.reco_mean, trailing_pe=EXCLUDED.trailing_pe,"
            "forward_pe=EXCLUDED.forward_pe, eps_forward=EXCLUDED.eps_forward,"
            "eps_trailing=EXCLUDED.eps_trailing, earnings_growth=EXCLUDED.earnings_growth,"
            "revenue_growth=EXCLUDED.revenue_growth, sector=EXCLUDED.sector",
            (
                symbol.upper(),
                _snap_f(info.get("currentPrice") or info.get("regularMarketPrice")),
                _snap_f(info.get("marketCap")),
                _snap_f(info.get("targetMeanPrice")),
                _snap_f(info.get("targetHighPrice")),
                _snap_f(info.get("targetLowPrice")),
                _snap_i(info.get("numberOfAnalystOpinions")),
                (info.get("recommendationKey") or None),
                _snap_f(info.get("recommendationMean")),
                _snap_f(info.get("trailingPE")),
                _snap_f(info.get("forwardPE")),
                _snap_f(info.get("forwardEps")),
                _snap_f(info.get("trailingEps")),
                _snap_f(info.get("earningsGrowth")),
                _snap_f(info.get("revenueGrowth")),
                (info.get("sector") or None),
            ),
        )
        cur.execute(
            "INSERT INTO snapshot_universe (symbol, last_snap) VALUES (%s, CURRENT_DATE) "
            "ON CONFLICT (symbol) DO UPDATE SET last_snap = CURRENT_DATE, active = true",
            (symbol.upper(),),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        logger.warning("record_analyst_snapshot %s: %s" % (symbol, e))
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        conn.close()


# Ratings ranked worst to best so we can measure whether the street moved up or down.
_RECO_RANK = {
    "strong_sell": 1, "sell": 2, "underperform": 2, "hold": 3, "neutral": 3,
    "buy": 4, "outperform": 4, "strong_buy": 5,
}


def compute_revision_momentum(symbol, window_days=30):
    """Compare today's analyst state against `window_days` ago.

    Returns a dict. `score` is 0-100, or None when there is not enough history yet — in which
    case `status` is "building" and `days_of_history` tells the UI how far along we are. It never
    fabricates a number to look finished.
    """
    out = {
        "status": "building", "score": None, "days_of_history": 0, "window_days": window_days,
        "target_change_pct": None, "analyst_count_change": None,
        "rating_direction": None, "notes": [],
    }
    if not symbol:
        return out
    conn = get_db()
    if not conn:
        out["status"] = "unavailable"
        return out
    try:
        cur = conn.cursor()
        sym = symbol.upper()
        cur.execute("SELECT COUNT(*), MIN(snap_date) FROM analyst_snapshots WHERE symbol=%s", (sym,))
        row = cur.fetchone() or (0, None)
        n_rows = row[0] or 0
        if row[1]:
            out["days_of_history"] = (_sec_date.today() - row[1]).days
        # Latest snapshot.
        cur.execute(
            "SELECT snap_date, target_mean, analyst_count, reco_key, price FROM analyst_snapshots "
            "WHERE symbol=%s ORDER BY snap_date DESC LIMIT 1", (sym,))
        cur_row = cur.fetchone()
        # Closest snapshot at or before the window start.
        cur.execute(
            "SELECT snap_date, target_mean, analyst_count, reco_key, price FROM analyst_snapshots "
            "WHERE symbol=%s AND snap_date <= CURRENT_DATE - %s ORDER BY snap_date DESC LIMIT 1",
            (sym, window_days))
        old_row = cur.fetchone()
        cur.close()

        if not cur_row or not old_row:
            out["notes"].append(
                "Revision history is still building for %s. %d day(s) recorded; %d needed."
                % (sym, out["days_of_history"], window_days))
            return out

        _, t_now, a_now, r_now, _p_now = cur_row
        _, t_old, a_old, r_old, _p_old = old_row

        pts = 0.0
        weight = 0.0

        # 1. Target price revision. The core signal: is the street marking this up or down?
        if t_now is not None and t_old not in (None, 0):
            chg = (float(t_now) - float(t_old)) / abs(float(t_old)) * 100.0
            out["target_change_pct"] = round(chg, 2)
            if chg >= 5:
                p = 100.0
            elif chg >= 2:
                p = 80.0
            elif chg > 0:
                p = 65.0
            elif chg == 0:
                p = 50.0
            elif chg > -2:
                p = 35.0
            elif chg > -5:
                p = 20.0
            else:
                p = 0.0
            pts += p * 0.55
            weight += 0.55
            out["notes"].append(
                "Consensus target %s %.1f%% over %d days."
                % ("raised" if chg > 0 else ("cut" if chg < 0 else "unchanged"), abs(chg), window_days))

        # 2. Rating drift. Downgrades are treated as more informative than upgrades, which is the
        #    asymmetry the research supports — sell-side ratings are structurally biased long, so a
        #    downgrade carries more information than an upgrade of the same size.
        rn = _RECO_RANK.get((r_now or "").lower())
        ro = _RECO_RANK.get((r_old or "").lower())
        if rn and ro:
            d = rn - ro
            if d > 0:
                p = 75.0
                out["rating_direction"] = "upgrade"
                out["notes"].append("Street rating improved to %s." % r_now)
            elif d < 0:
                p = 5.0
                out["rating_direction"] = "downgrade"
                out["notes"].append("Street rating cut to %s. Downgrades carry more signal than upgrades." % r_now)
            else:
                p = 50.0
                out["rating_direction"] = "unchanged"
            pts += p * 0.25
            weight += 0.25

        # 3. Coverage breadth. More analysts picking it up is a real, if quiet, forward signal.
        if a_now is not None and a_old is not None:
            d = int(a_now) - int(a_old)
            out["analyst_count_change"] = d
            if d >= 2:
                p = 85.0
            elif d == 1:
                p = 68.0
            elif d == 0:
                p = 50.0
            elif d == -1:
                p = 35.0
            else:
                p = 20.0
            pts += p * 0.20
            weight += 0.20
            if d != 0:
                out["notes"].append(
                    "%d analyst(s) %s coverage." % (abs(d), "added" if d > 0 else "dropped"))

        if weight <= 0:
            out["notes"].append("Snapshots exist but carry no analyst fields for %s." % sym)
            return out

        out["score"] = int(round(pts / weight))
        out["status"] = "ok"
        out["rows"] = n_rows
        return out
    except Exception as e:
        logger.error("compute_revision_momentum %s: %s" % (symbol, e))
        out["status"] = "error"
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


def snap_sweep(max_symbols=100):
    """Snapshot the stalest symbols in the universe. Designed to be called on a schedule.

    Processes oldest-first, so repeated runs cycle through the whole universe. Because the row is
    keyed (symbol, snap_date), a symbol already captured today is refreshed rather than duplicated,
    and running the cron five times a day costs nothing but keeps coverage complete.
    """
    started = time.time()
    summary = {"attempted": 0, "stored": 0, "failed": 0, "universe": 0}
    conn = get_db()
    if not conn:
        summary["error"] = "no database"
        return summary
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM snapshot_universe WHERE active")
        summary["universe"] = (cur.fetchone() or [0])[0]
        cur.execute(
            "SELECT symbol FROM snapshot_universe WHERE active "
            "AND (last_snap IS NULL OR last_snap < CURRENT_DATE) "
            "ORDER BY last_snap ASC NULLS FIRST, hits DESC LIMIT %s",
            (max_symbols,),
        )
        symbols = [r[0] for r in (cur.fetchall() or [])]
        cur.close()
    except Exception as e:
        logger.error("snap_sweep query: %s" % e)
        summary["error"] = str(e)
        return summary
    finally:
        conn.close()

    for sym in symbols:
        summary["attempted"] += 1
        try:
            info = yf.Ticker(sym).info or {}
            if record_analyst_snapshot(sym, info):
                summary["stored"] += 1
            else:
                summary["failed"] += 1
        except Exception as e:
            summary["failed"] += 1
            logger.warning("snap_sweep %s: %s" % (sym, e))
        time.sleep(0.35)  # be a good citizen; yfinance rate-limits hard otherwise

    summary["elapsed_sec"] = round(time.time() - started, 1)
    logger.info("snap_sweep complete: %s" % summary)
    return summary


@app.route("/cron/analyst-snapshot")
def cron_analyst_snapshot():
    """Daily (or hourly) capture. Token-gated like the other crons.

    Run it hourly. Each pass takes the stalest 100 symbols, so the universe stays fully covered as
    it grows, and no single request runs long enough to time out.
    """
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    try:
        limit = int(request.args.get("limit", 100))
    except Exception:
        limit = 100
    limit = max(1, min(limit, 400))
    try:
        snap_create_tables()
        return jsonify(snap_sweep(max_symbols=limit))
    except Exception as e:
        logger.error("cron_analyst_snapshot: %s" % e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/revision/<symbol>")
def api_revision(symbol):
    """Revision momentum for one symbol. Honest 'building' state until history exists."""
    try:
        window = int(request.args.get("window", 30))
    except Exception:
        window = 30
    return jsonify(compute_revision_momentum(symbol, window_days=max(7, min(window, 180))))


@app.route("/api/snapshot-status")
def api_snapshot_status():
    """How big is the dataset? Useful to watch the moat compound."""
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT symbol), MIN(snap_date), MAX(snap_date) FROM analyst_snapshots")
        rows, syms, first, last = cur.fetchone() or (0, 0, None, None)
        cur.execute("SELECT COUNT(*) FROM snapshot_universe WHERE active")
        uni = (cur.fetchone() or [0])[0]
        cur.close()
        return jsonify({
            "total_snapshots": rows, "symbols_covered": syms, "universe_size": uni,
            "first_snapshot": str(first) if first else None,
            "latest_snapshot": str(last) if last else None,
            "days_of_history": (last - first).days if (first and last) else 0,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# =========================================================================== #
# END ANALYST SNAPSHOT + REVISION MOMENTUM ENGINE
# =========================================================================== #



# =========================================================================== #
# POINT-IN-TIME FEATURE STORE + OUTCOME LABELER
#
# The rule this whole module exists to enforce: a row is written on the day the
# engine made its call, BEFORE the outcome exists, and its feature columns are
# never rewritten afterwards. Outcome labels are filled in later by a separate
# job that may only look at prices dated AFTER the snapshot.
#
# That is what makes a backtest honest. Reconstructed backtests -- pull today's
# data, pretend you had it in 2023 -- are how people fool themselves: today's
# fundamentals are restated, today's index membership survivor-biased, today's
# estimates already know the answer. Recording forward in time makes lookahead
# bias structurally impossible rather than merely discouraged.
#
# So: features are INSERT-only per (symbol, day). Labels are UPDATE-only, and
# only on rows old enough to have matured.
# =========================================================================== #

# Horizons we label. Keep in sync with the columns in feature_snapshots.
_FS_HORIZONS = (5, 20, 60)
_FS_BENCH = "SPY"


def _fs_count(seq, key, want):
    """Count transactions of a given direction in insider/congress lists. Never raises."""
    n = 0
    try:
        for row in (seq or []):
            v = str((row or {}).get(key, "")).lower()
            if want in v:
                n += 1
    except Exception:
        pass
    return n


def fs_create_tables():
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS feature_snapshots ("
            "id SERIAL PRIMARY KEY,"
            "symbol TEXT NOT NULL,"
            "snap_date DATE NOT NULL,"
            "snap_ts TIMESTAMP DEFAULT NOW(),"
            # -- what the engine decided, pulled out as columns so we can query them fast
            "price DOUBLE PRECISION,"
            "alpha_score INTEGER,"
            "verdict TEXT,"
            "confidence TEXT,"
            "data_quality TEXT,"
            "price_source TEXT,"
            "sector TEXT,"
            # -- everything else the engine knew, schema-free so adding a factor needs no migration
            "features JSONB,"
            # -- OUTCOME LABELS. NULL at write time. Filled only once the future has happened.
            "fwd_ret_5d DOUBLE PRECISION,"
            "fwd_ret_20d DOUBLE PRECISION,"
            "fwd_ret_60d DOUBLE PRECISION,"
            "fwd_max_dd_20d DOUBLE PRECISION,"
            "bench_ret_20d DOUBLE PRECISION,"
            "excess_ret_20d DOUBLE PRECISION,"
            "labeled_at TIMESTAMP,"
            "CONSTRAINT uq_fs UNIQUE (symbol, snap_date))"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fs_sym_date ON feature_snapshots(symbol, snap_date DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fs_unlabeled ON feature_snapshots(snap_date) WHERE labeled_at IS NULL")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fs_score ON feature_snapshots(alpha_score)")
        conn.commit()
        cur.close()
        logger.info("feature store: tables ready")
    except Exception as e:
        logger.error("fs_create_tables: %s" % e)
    finally:
        conn.close()


def record_feature_snapshot(symbol, result, info=None):
    """Write what the engine knew about `symbol` today. INSERT-only on the feature side.

    If a row already exists for today, the features are refreshed (a later run on the same day is
    simply a better-informed end-of-day view) but any labels already attached are left alone. What
    is never allowed is a label leaking backwards into the features.
    """
    if not symbol or not isinstance(result, dict) or not result:
        return False
    conn = get_db()
    if not conn:
        return False
    try:
        info = info or {}
        ins = result.get("insider") or []
        cong = result.get("congressional") or []

        feats = {
            # raw fundamentals as known today
            "change_pct": result.get("change_pct"),
            "pe_ratio": result.get("pe_ratio"),
            "peg_ratio": result.get("peg_ratio"),
            "price_to_book": result.get("price_to_book"),
            "price_to_sales": result.get("price_to_sales"),
            "ev_to_ebitda": result.get("ev_to_ebitda"),
            "roe": result.get("roe"),
            "fcf_yield": result.get("fcf_yield"),
            "profit_margin": result.get("profit_margin"),
            "revenue_growth": result.get("revenue_growth"),
            "debt_to_equity": result.get("debt_to_equity"),
            "beta": result.get("beta"),
            "market_cap": result.get("market_cap"),
            "volume": result.get("volume"),
            "analyst_target": result.get("analyst_target"),
            "analyst_consensus": result.get("analyst_consensus"),
            # engine output, layer by layer -- this is "what did it know and when"
            "alpha_breakdown": result.get("alpha_breakdown"),
            "alpha_v2": result.get("alpha_v2"),
            "legacy_verdict": result.get("legacy_verdict"),
            "verdict_signal_reason": result.get("verdict_signal_reason"),
            "conviction": result.get("conviction"),
            "score": result.get("score"),
            "apex_moat": result.get("apex_moat"),
            "active_cluster": result.get("active_cluster"),
            "flags": result.get("flags"),
            # event flags / counts, derived so the row stays small
            "insider_buys": _fs_count(ins, "type", "buy") or _fs_count(ins, "transaction", "buy"),
            "insider_sells": _fs_count(ins, "type", "sell") or _fs_count(ins, "transaction", "sell"),
            "insider_big_block": result.get("insider_big_block"),
            "insider_sell_value": result.get("insider_sell_value"),
            "cong_buys": _fs_count(cong, "type", "purchase") or _fs_count(cong, "transaction", "buy"),
            "cong_sells": _fs_count(cong, "type", "sale") or _fs_count(cong, "transaction", "sell"),
            "news_count": len(result.get("news") or []),
            # Logged so /api/backtest can eventually answer whether convergence predicts anything.
            # This is the whole reason it is not being acted on today.
            "converged": bool((result.get("convergence") or {}).get("converged")),
            "convergence_bonus": (result.get("convergence") or {}).get("bonus"),
            "convergence_signals": len((result.get("convergence") or {}).get("signals") or []),
            "forward_points": (result.get("forward_signals") or {}).get("points"),
            "risk_deduction": (result.get("risk_overlay") or {}).get("deduction"),
            "has_activist": bool(result.get("activist")),
            "has_severe_letter": any(c.get("severity") == "severe" for c in (result.get("comment_letters") or [])),
            "trial_setback": any(t.get("direction") == "setback" for t in (result.get("clinical_trials") or [])),
            # source freshness -- so we can later ask "was this call made on stale data?"
            "data_timestamp": result.get("data_timestamp"),
            "market_cap_reconciled": result.get("market_cap_reconciled"),
            "fmp": bool(result.get("fmp")),
        }

        conf = result.get("confidence")
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO feature_snapshots "
            "(symbol, snap_date, price, alpha_score, verdict, confidence, data_quality,"
            " price_source, sector, features) "
            "VALUES (%s, CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (symbol, snap_date) DO UPDATE SET "
            "snap_ts=NOW(), price=EXCLUDED.price, alpha_score=EXCLUDED.alpha_score,"
            "verdict=EXCLUDED.verdict, confidence=EXCLUDED.confidence,"
            "data_quality=EXCLUDED.data_quality, price_source=EXCLUDED.price_source,"
            "sector=EXCLUDED.sector, features=EXCLUDED.features",
            (
                symbol.upper(),
                _snap_f(result.get("price") or info.get("currentPrice") or info.get("regularMarketPrice")),
                _snap_i(result.get("alpha_score")),
                (result.get("verdict") or None),
                (str(conf) if conf is not None else None),
                (str(result.get("data_quality")) if result.get("data_quality") is not None else None),
                (result.get("price_source") or None),
                (info.get("sector") or None),
                _SecJson(feats),
            ),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        logger.warning("record_feature_snapshot %s: %s" % (symbol, e))
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        conn.close()


def _fs_closes(symbol, days=130):
    """Daily closes as [(date, close), ...] ascending. Uses the existing fallback chain."""
    out = []
    try:
        rows = fetch_with_fallback(symbol, period="6mo", interval="1d")
        for r in (rows or []):
            t = r.get("time")
            c = _snap_f(r.get("close"))
            if t and c:
                out.append((datetime.utcfromtimestamp(int(t)).date(), c))
    except Exception as e:
        logger.warning("_fs_closes %s: %s" % (symbol, e))
    out.sort(key=lambda x: x[0])
    return out


def label_outcomes(max_rows=300):
    """Attach realized outcomes to matured snapshots.

    THE POINT-IN-TIME GUARANTEE LIVES HERE: every price used is strictly AFTER the snapshot date.
    A row is only labeled once enough calendar time has passed for the horizon to be real. Feature
    columns are never touched.
    """
    started = time.time()
    summary = {"labeled": 0, "skipped": 0, "symbols": 0, "errors": 0}
    conn = get_db()
    if not conn:
        summary["error"] = "no database"
        return summary

    # Rows old enough that the shortest horizon has actually elapsed (calendar slack for weekends).
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT symbol FROM feature_snapshots "
            "WHERE labeled_at IS NULL AND snap_date <= CURRENT_DATE - %s "
            "ORDER BY symbol LIMIT %s",
            (_FS_HORIZONS[0] + 3, max(1, max_rows // 5)),
        )
        symbols = [r[0] for r in (cur.fetchall() or [])]
        cur.close()
    except Exception as e:
        conn.close()
        logger.error("label_outcomes query: %s" % e)
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not symbols:
        summary["elapsed_sec"] = round(time.time() - started, 1)
        return summary

    bench = _fs_closes(_FS_BENCH)
    bench_map = dict(bench)
    bench_dates = [d for d, _ in bench]

    def _forward(series, dates, start_date, n):
        """Close n TRADING days after start_date, using only bars strictly after it."""
        fut = [i for i, d in enumerate(dates) if d > start_date]
        if len(fut) < n:
            return None
        return series[dates[fut[n - 1]]]

    for sym in symbols:
        summary["symbols"] += 1
        closes = _fs_closes(sym)
        if len(closes) < 10:
            summary["errors"] += 1
            continue
        cmap = dict(closes)
        cdates = [d for d, _ in closes]

        conn = get_db()
        if not conn:
            break
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, snap_date, price FROM feature_snapshots "
                "WHERE symbol=%s AND labeled_at IS NULL AND snap_date <= CURRENT_DATE - %s",
                (sym, _FS_HORIZONS[0] + 3),
            )
            rows = cur.fetchall() or []

            for rid, sdate, spx in rows:
                base = _snap_f(spx) or cmap.get(sdate)
                if not base:
                    # Fall back to the last close on or before the snapshot date -- still never after it.
                    prior = [d for d in cdates if d <= sdate]
                    if not prior:
                        summary["skipped"] += 1
                        continue
                    base = cmap[prior[-1]]

                rets = {}
                for h in _FS_HORIZONS:
                    px = _forward(cmap, cdates, sdate, h)
                    rets[h] = ((px - base) / base * 100.0) if (px and base) else None

                # Max drawdown across the 20-day forward window, again only bars after the snapshot.
                dd = None
                fut_idx = [i for i, d in enumerate(cdates) if d > sdate][:20]
                if fut_idx and base:
                    lo = min(cmap[cdates[i]] for i in fut_idx)
                    dd = (lo - base) / base * 100.0

                # Benchmark over the same window, so we can measure skill rather than beta.
                b_ret = None
                if bench_dates:
                    b_prior = [d for d in bench_dates if d <= sdate]
                    b_fwd = _forward(bench_map, bench_dates, sdate, 20)
                    if b_prior and b_fwd:
                        b_base = bench_map[b_prior[-1]]
                        if b_base:
                            b_ret = (b_fwd - b_base) / b_base * 100.0

                excess = None
                if rets.get(20) is not None and b_ret is not None:
                    excess = rets[20] - b_ret

                # Only stamp labeled_at once the longest horizon has resolved; otherwise leave the
                # row open so a later pass can complete it. Partial labels are written meanwhile.
                done = rets.get(60) is not None
                cur.execute(
                    "UPDATE feature_snapshots SET fwd_ret_5d=%s, fwd_ret_20d=%s, fwd_ret_60d=%s,"
                    "fwd_max_dd_20d=%s, bench_ret_20d=%s, excess_ret_20d=%s,"
                    "labeled_at=CASE WHEN %s THEN NOW() ELSE NULL END WHERE id=%s",
                    (rets.get(5), rets.get(20), rets.get(60), dd, b_ret, excess, done, rid),
                )
                summary["labeled"] += 1

            conn.commit()
            cur.close()
        except Exception as e:
            summary["errors"] += 1
            logger.warning("label_outcomes %s: %s" % (sym, e))
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            conn.close()
        time.sleep(0.2)

    summary["elapsed_sec"] = round(time.time() - started, 1)
    logger.info("label_outcomes: %s" % summary)
    return summary


@app.route("/cron/label-outcomes")
def cron_label_outcomes():
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    try:
        fs_create_tables()
        return jsonify(label_outcomes())
    except Exception as e:
        logger.error("cron_label_outcomes: %s" % e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/backtest")
def api_backtest():
    """Does the Alpha Score actually predict anything?

    Buckets every labeled snapshot by score and reports the mean forward return and mean EXCESS
    return per bucket. Excess is the honest column: beating the market is skill, rising with it is
    beta. If the buckets do not separate, the score does not work -- and you will be able to see
    that here rather than believe otherwise for a year.
    """
    horizon = request.args.get("horizon", "20d")
    col = {"5d": "fwd_ret_5d", "20d": "fwd_ret_20d", "60d": "fwd_ret_60d"}.get(horizon, "fwd_ret_20d")
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT width_bucket(alpha_score, 0, 100, 5) AS b, COUNT(*), "
            "ROUND(AVG(" + col + ")::numeric, 2), ROUND(AVG(excess_ret_20d)::numeric, 2), "
            "MIN(alpha_score), MAX(alpha_score) "
            "FROM feature_snapshots "
            "WHERE alpha_score IS NOT NULL AND " + col + " IS NOT NULL "
            "GROUP BY b ORDER BY b"
        )
        buckets = []
        for b, n, avg_ret, avg_exc, lo, hi in (cur.fetchall() or []):
            buckets.append({
                "bucket": int(b) if b is not None else None,
                "score_range": "%s-%s" % (lo, hi),
                "n": int(n),
                "mean_return_pct": float(avg_ret) if avg_ret is not None else None,
                "mean_excess_vs_spy_pct": float(avg_exc) if avg_exc is not None else None,
            })
        cur.execute("SELECT COUNT(*) FROM feature_snapshots WHERE " + col + " IS NOT NULL")
        n_labeled = (cur.fetchone() or [0])[0]
        cur.execute("SELECT COUNT(*) FROM feature_snapshots")
        n_total = (cur.fetchone() or [0])[0]
        cur.close()

        note = None
        if n_labeled < 200:
            note = ("Only %d labeled observations. This is far too few to conclude anything. "
                    "Treat these numbers as a plumbing check, not a result." % n_labeled)
        return jsonify({
            "horizon": horizon, "buckets": buckets,
            "labeled_observations": n_labeled, "total_snapshots": n_total,
            "warning": note,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/feature-store-status")
def api_feature_store_status():
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*), COUNT(DISTINCT symbol), MIN(snap_date), MAX(snap_date),"
            "COUNT(fwd_ret_20d), COUNT(labeled_at) FROM feature_snapshots")
        rows, syms, first, last, lab20, done = cur.fetchone() or (0, 0, None, None, 0, 0)
        cur.close()
        return jsonify({
            "total_snapshots": rows, "symbols_covered": syms,
            "first_snapshot": str(first) if first else None,
            "latest_snapshot": str(last) if last else None,
            "days_of_history": (last - first).days if (first and last) else 0,
            "labeled_20d": lab20, "fully_labeled": done,
            "awaiting_maturity": rows - lab20,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# =========================================================================== #
# END POINT-IN-TIME FEATURE STORE
# =========================================================================== #



# =========================================================================== #
# ECONOMIC CALENDAR
#
# Two sources, no new paid API:
#   1. FRED's release-dates endpoint gives the REAL scheduled dates for CPI,
#      PPI, the Employment Situation, GDP, Retail Sales and jobless claims.
#      This matters: guessing "second week of the month" would sometimes
#      downgrade a stock's confidence on the wrong day and miss the right one,
#      which is worse than having no calendar at all.
#   2. FOMC dates are hardcoded from the Fed's published 2026 calendar, because
#      FOMC is not a FRED "release". Confirmed against federalreserve.gov.
#
# FRED also supplies the latest actual values (fed funds, CPI YoY, unemployment,
# GDP), cached 6 hours.
#
# If FRED is unreachable or no key is set, the calendar degrades to rule-derived
# dates that are explicitly marked estimated=True, and estimated events never
# trigger the confidence downgrade. An approximate date is fine to show a user;
# it is not fine to silently move a score with.
# =========================================================================== #

FRED_API_KEY = os.environ.get("FRED_API_KEY", "") or os.environ.get("FRED_KEY", "")
FRED_BASE = "https://api.stlouisfed.org/fred"

# Confirmed FOMC rate-decision days (second day of each two-day meeting), 2026-2027.
# Source: federalreserve.gov FOMC calendar. 2027 dates are the Fed's tentative schedule.
# NOTE TO FUTURE MAINTAINER: refresh this list each year; the Fed publishes it in advance.
FOMC_DECISION_DAYS = [
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
    "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09",
    "2027-01-27", "2027-03-17", "2027-04-28", "2027-06-16",
    "2027-07-28", "2027-09-22", "2027-11-03", "2027-12-15",
]

# Which sectors historically move most on each event. Used to tell a user when the macro
# event is not just market-wide noise but points directly at the stock they are looking at.
ECON_EVENT_SPECS = {
    "FOMC": {
        "name": "FOMC Interest Rate Decision",
        "importance": "High",
        "time_et": "2:00 PM ET",
        "url": "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
        "source_name": "Federal Reserve",
        "keywords": ["fed", "fomc", "interest rate", "rate decision", "powell", "warsh", "federal reserve", "rate cut", "rate hike"],
        "why": "The Fed sets the price of money. A surprise on rates repositions every asset class at once, and the press conference half an hour later often moves markets more than the decision itself.",
        "sectors": ["Financial Services", "Financials", "Real Estate", "Utilities", "Technology"],
    },
    "CPI": {
        "name": "CPI Inflation Report",
        "importance": "High",
        "time_et": "8:30 AM ET",
        "url": "https://www.bls.gov/cpi/",
        "source_name": "Bureau of Labor Statistics",
        "keywords": ["cpi", "inflation", "consumer price", "core inflation", "price index"],
        "why": "The most-watched inflation print. It lands an hour before the open, so the first repricing happens in thin premarket liquidity, and it drives what the market expects the Fed to do next.",
        "sectors": ["Consumer Cyclical", "Consumer Discretionary", "Consumer Defensive", "Consumer Staples", "Real Estate", "Utilities"],
    },
    "NFP": {
        "name": "Non-Farm Payrolls / Employment Situation",
        "importance": "High",
        "time_et": "8:30 AM ET",
        "url": "https://www.bls.gov/news.release/empsit.toc.htm",
        "source_name": "Bureau of Labor Statistics",
        "keywords": ["payroll", "jobs report", "nonfarm", "non-farm", "unemployment", "employment", "labor market", "hiring"],
        "why": "The monthly jobs report. A hot number can push rate-cut expectations out; a cold one can pull them forward. It moves the whole market, not one sector.",
        "sectors": [],  # broad market
    },
    "GDP": {
        "name": "GDP Report",
        "importance": "High",
        "time_et": "8:30 AM ET",
        "url": "https://www.bea.gov/data/gdp/gross-domestic-product",
        "source_name": "Bureau of Economic Analysis",
        "keywords": ["gdp", "gross domestic product", "economic growth", "recession", "economy grew", "economy shrank"],
        "why": "The broadest read on whether the economy is growing or contracting. Cyclical and industrial names carry the most sensitivity to it.",
        "sectors": ["Industrials", "Basic Materials", "Energy", "Consumer Cyclical"],
    },
    "PPI": {
        "name": "PPI Producer Prices",
        "importance": "Medium",
        "time_et": "8:30 AM ET",
        "url": "https://www.bls.gov/ppi/",
        "source_name": "Bureau of Labor Statistics",
        "keywords": ["ppi", "producer price", "wholesale price", "input costs", "factory prices"],
        "why": "Input-cost inflation before it reaches the shelf. It often previews the direction of CPI and squeezes margins for companies that cannot pass costs on.",
        "sectors": ["Industrials", "Basic Materials", "Consumer Defensive", "Consumer Staples"],
    },
    "RETAIL": {
        "name": "Retail Sales",
        "importance": "Medium",
        "time_et": "8:30 AM ET",
        "url": "https://www.census.gov/retail/index.html",
        "source_name": "U.S. Census Bureau",
        "keywords": ["retail sales", "consumer spending", "shoppers", "holiday spending"],
        "why": "Whether households are still spending. It is the cleanest monthly read on consumer strength.",
        "sectors": ["Consumer Cyclical", "Consumer Discretionary", "Consumer Defensive", "Consumer Staples"],
    },
    "CLAIMS": {
        "name": "Initial Jobless Claims",
        "importance": "Medium",
        "time_et": "8:30 AM ET",
        "url": "https://www.dol.gov/ui/data.pdf",
        "source_name": "Department of Labor",
        "keywords": ["jobless claims", "unemployment claims", "initial claims", "layoffs"],
        "why": "A weekly pulse on layoffs. One print rarely matters; a trend does, and it is the earliest signal the labour market is cracking.",
        "sectors": [],
    },
}

# FRED release names -> our event keys. Matched loosely so a small wording change upstream does
# not silently drop an event from the calendar.
_FRED_RELEASE_MATCH = [
    ("consumer price index", "CPI"),
    ("employment situation", "NFP"),
    ("producer price index", "PPI"),
    ("gross domestic product", "GDP"),
    ("advance monthly sales for retail", "RETAIL"),
    ("retail trade", "RETAIL"),
    ("unemployment insurance weekly claims", "CLAIMS"),
]


def _fred_get(path, params=None, cache_hours=6):
    """Call FRED. Cached. Returns parsed JSON or None. Never raises."""
    if not FRED_API_KEY:
        return None
    p = dict(params or {})
    p["api_key"] = FRED_API_KEY
    p["file_type"] = "json"
    ckey = "fred_" + path + "_" + "&".join("%s=%s" % (k, v) for k, v in sorted(p.items()) if k != "api_key")
    cached = CACHE.get(ckey)
    if cached and (time.time() - cached[1]) < cache_hours * 3600:
        return cached[0]
    try:
        r = requests.get(FRED_BASE + path, params=p, timeout=12)
        if r.status_code != 200:
            logger.warning("FRED %s -> %s" % (path, r.status_code))
            return None
        data = r.json()
        CACHE[ckey] = (data, time.time())
        return data
    except Exception as e:
        logger.warning("FRED %s failed: %s" % (path, e))
        return None


def fred_latest(series_id, n=2):
    """Most recent observations for a FRED series, newest first. [] on failure."""
    d = _fred_get("/series/observations", {
        "series_id": series_id, "sort_order": "desc", "limit": n,
    })
    out = []
    for o in ((d or {}).get("observations") or []):
        v = o.get("value")
        if v in (None, ".", ""):
            continue
        try:
            out.append({"date": o.get("date"), "value": float(v)})
        except Exception:
            continue
    return out


def fred_macro_snapshot():
    """Latest actual values for the headline macro series. Cached 6h inside _fred_get."""
    snap = {}
    try:
        ff = fred_latest("DFEDTARU", 1)  # fed funds target, upper bound
        if ff:
            snap["fed_funds_upper"] = {"value": ff[0]["value"], "as_of": ff[0]["date"], "unit": "%"}

        # CPI year-over-year has to be computed; FRED publishes the index level, not the YoY rate.
        cpi = fred_latest("CPIAUCSL", 13)
        if len(cpi) >= 13 and cpi[12]["value"]:
            yoy = (cpi[0]["value"] - cpi[12]["value"]) / cpi[12]["value"] * 100.0
            snap["cpi_yoy"] = {"value": round(yoy, 1), "as_of": cpi[0]["date"], "unit": "%"}

        un = fred_latest("UNRATE", 1)
        if un:
            snap["unemployment_rate"] = {"value": un[0]["value"], "as_of": un[0]["date"], "unit": "%"}

        gdp = fred_latest("A191RL1Q225SBEA", 1)  # real GDP, % change from preceding quarter, annualized
        if gdp:
            snap["gdp_qoq_annualized"] = {"value": gdp[0]["value"], "as_of": gdp[0]["date"], "unit": "%"}
    except Exception as e:
        logger.warning("fred_macro_snapshot: %s" % e)
    return snap


def _econ_fred_release_dates(start, end):
    """Real scheduled release dates from FRED. Returns [(date, event_key)]. [] if FRED is down."""
    d = _fred_get("/releases/dates", {
        "realtime_start": start.isoformat(),
        "realtime_end": end.isoformat(),
        "include_release_dates_with_no_data": "true",
        "limit": 400,
        "sort_order": "asc",
    })
    out = []
    for rd in ((d or {}).get("release_dates") or []):
        nm = str(rd.get("release_name", "")).lower()
        ds = rd.get("date")
        if not ds:
            continue
        for needle, key in _FRED_RELEASE_MATCH:
            if needle in nm:
                try:
                    out.append((_sec_date.fromisoformat(ds), key))
                except Exception:
                    pass
                break
    return out


def _econ_estimated_dates(start, end):
    """Rule-derived fallback used only when FRED is unavailable. Explicitly marked estimated:
    NFP is the first Friday, claims are every Thursday, and the monthly BLS/Census prints land
    roughly mid-month. These are close, not exact -- which is why they never move a score."""
    out = []
    day = start
    while day <= end:
        if day.weekday() == 4 and day.day <= 7:
            out.append((day, "NFP"))
        if day.weekday() == 3:
            out.append((day, "CLAIMS"))
        if day.day == 12:
            out.append((day, "CPI"))
        if day.day == 15:
            out.append((day, "PPI"))
        if day.day == 16:
            out.append((day, "RETAIL"))
        day += timedelta(days=1)
    return out


def build_economic_calendar(days_ahead=7, days_back=3):
    """Recent and upcoming macro events. Always returns a dict; never raises."""
    today = _sec_date.today()
    start = today - timedelta(days=days_back)
    end = today + timedelta(days=days_ahead)

    events = []
    estimated = False

    pairs = _econ_fred_release_dates(start, end)
    if not pairs:
        estimated = True
        pairs = _econ_estimated_dates(start, end)

    # FOMC is not a FRED release, so it is layered in from the Fed's published calendar.
    for ds in FOMC_DECISION_DAYS:
        try:
            d = _sec_date.fromisoformat(ds)
        except Exception:
            continue
        if start <= d <= end:
            pairs.append((d, "FOMC"))

    macro = fred_macro_snapshot()
    prev_for = {
        "FOMC": macro.get("fed_funds_upper"),
        "CPI": macro.get("cpi_yoy"),
        "NFP": macro.get("unemployment_rate"),
        "GDP": macro.get("gdp_qoq_annualized"),
    }

    seen = set()
    for d, key in sorted(pairs, key=lambda x: x[0]):
        spec = ECON_EVENT_SPECS.get(key)
        if not spec:
            continue
        sig = (d.isoformat(), key)
        if sig in seen:
            continue
        seen.add(sig)

        # FOMC dates are always confirmed; FRED-sourced dates are confirmed; the rule-derived
        # fallback is not. Only confirmed events are allowed to move a stock's confidence.
        is_est = estimated and key != "FOMC"
        prev = prev_for.get(key)
        days_out = (d - today).days

        events.append({
            "key": key,
            "name": spec["name"],
            "date": d.isoformat(),
            "time": spec["time_et"],
            "importance": spec["importance"],
            "why": spec["why"],
            "sectors": spec["sectors"],
            "days_out": days_out,
            "is_past": days_out < 0,
            "estimated_date": bool(is_est),
            "previous": prev,
            "forecast": None,  # no free forecast source; left honest rather than invented
            "url": spec.get("url"),
            "source_name": spec.get("source_name"),
        })

    upcoming = [e for e in events if not e["is_past"]]
    return {
        "generated": datetime.utcnow().isoformat() + "Z",
        "window": {"from": start.isoformat(), "to": end.isoformat()},
        "macro": macro,
        "events": events,
        "upcoming": upcoming,
        "next_high_impact": next((e for e in upcoming if e["importance"] == "High"), None),
        "dates_estimated": estimated,
        "source": "FRED release calendar + published FOMC schedule" if not estimated
                  else "estimated (FRED unavailable — dates approximate, not used for scoring)",
    }


def econ_event_within(hours=48):
    """The next High-importance, CONFIRMED-date event inside `hours`, or None.

    Estimated dates are deliberately excluded: a guessed date must never move a score.
    """
    try:
        cal = build_economic_calendar(days_ahead=max(1, int(hours / 24) + 1), days_back=0)
        limit_days = hours / 24.0
        for e in cal.get("upcoming") or []:
            if e["importance"] != "High":
                continue
            if e.get("estimated_date"):
                continue
            if e["days_out"] <= limit_days:
                return e
    except Exception as e:
        logger.warning("econ_event_within: %s" % e)
    return None


def econ_downgrade(confidence):
    """High -> Medium -> Low. Low stays Low."""
    return {"High": "Medium", "Medium": "Low", "Low": "Low"}.get(confidence, confidence)


def apply_econ_overlay(confidence, flags, sector):
    """Apply the macro overlay to a report's confidence and flags.

    Returns (confidence, flags, event_or_None). Safe: on any failure the inputs come back untouched.
    """
    try:
        ev = econ_event_within(48)
        if not ev:
            return confidence, flags, None

        when = "today" if ev["days_out"] == 0 else ("tomorrow" if ev["days_out"] == 1 else "in %d days" % ev["days_out"])
        flags = list(flags or [])
        flags.append({
            "level": "warn",
            "text": ("A major economic event (%s) is scheduled %s at %s. This can move the entire "
                     "market regardless of this stock's individual signals."
                     % (ev["name"], when, ev["time"])),
        })

        sec = (sector or "").strip()
        if sec and ev["sectors"] and any(sec.lower() == s.lower() for s in ev["sectors"]):
            flags.append({
                "level": "warn",
                "text": "%s stocks are typically sensitive to this kind of event." % sec,
            })
            ev = dict(ev)
            ev["sector_match"] = True

        return econ_downgrade(confidence), flags, ev
    except Exception as e:
        logger.warning("apply_econ_overlay: %s" % e)
        return confidence, flags, None


def build_macro_news(event_key, limit=4):
    """Coverage of a macro event, reusing the Finnhub general-news feed we already pay for.

    Filtered by the event's keywords so a CPI print surfaces inflation coverage, not whatever
    happened to be on the wire. Returns the same item shape as build_news(), so these flow through
    the existing news renderer and modal untouched -- they are clickable for free.
    """
    spec = ECON_EVENT_SPECS.get(event_key)
    if not spec or not FINNHUB_KEY:
        return []
    ckey = "macronews_" + event_key
    cached = CACHE.get(ckey)
    if cached and (time.time() - cached[1]) < 1800:
        return cached[0]

    kws = [k.lower() for k in spec.get("keywords", [])]
    out = []
    try:
        r = requests.get("https://finnhub.io/api/v1/news?category=general&token=%s" % FINNHUB_KEY, timeout=8)
        if r.status_code == 200:
            arts = [n for n in r.json() if n.get("headline")]
            arts.sort(key=lambda a: a.get("datetime", 0), reverse=True)
            for n in arts:
                blob = (str(n.get("headline", "")) + " " + str(n.get("summary", ""))).lower()
                if not any(k in blob for k in kws):
                    continue
                out.append({
                    "headline": clean_text(n["headline"]),
                    "source": clean_text(n.get("source", "Market News")) + " \u00b7 Macro",
                    "summary": _clean_summary(n.get("summary", ""), n.get("url", "")),
                    "summary_long": _full_summary(n.get("summary", ""), n.get("url", "")),
                    "url": n.get("url", ""),
                    "ts": _news_ts(n.get("datetime", 0)),
                    "is_macro": True,
                    "macro_event": spec["name"],
                })
                if len(out) >= limit:
                    break
    except Exception as e:
        logger.warning("build_macro_news %s: %s" % (event_key, e))

    CACHE[ckey] = (out, time.time())
    return out


@app.route("/api/macro-news")
def api_macro_news():
    key = (request.args.get("event") or "").upper()
    if key not in ECON_EVENT_SPECS:
        return jsonify({"error": "unknown event", "articles": []}), 400
    return jsonify({
        "event": ECON_EVENT_SPECS[key]["name"],
        "official_url": ECON_EVENT_SPECS[key].get("url"),
        "official_source": ECON_EVENT_SPECS[key].get("source_name"),
        "articles": build_macro_news(key),
    })


# =========================================================================== #
# LEARN LAYER
#
# The Learn tab teaches through the stocks a user is already looking at, so it
# needs to know what those were. Logged-in users get a durable row per symbol;
# logged-out users get the same thing in the signed session cookie, so the tab
# works before anyone makes an account.
#
# The breakdown shown in Learn is not recomputed -- it is read back out of the
# feature store, which means the lesson a user reads is literally the engine's
# own reasoning from the day they looked. If the score was wrong, the lesson
# says what the engine actually thought, not a tidied-up version.
# =========================================================================== #

def record_recent_search(symbol):
    """Remember that this user just looked at this symbol. Never raises."""
    if not symbol:
        return
    sym = symbol.upper()
    try:
        recent = [x for x in (session.get("recent_searches") or []) if x != sym]
        recent.insert(0, sym)
        session["recent_searches"] = recent[:10]
    except Exception:
        pass

    u = current_user()
    if not u:
        return
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO recent_searches (user_id, symbol, viewed_at) VALUES (%s, %s, NOW()) "
            "ON CONFLICT (user_id, symbol) DO UPDATE SET viewed_at = NOW()",
            (u["id"], sym),
        )
        conn.commit()
        cur.close()
    except Exception as e:
        logger.warning("record_recent_search %s: %s" % (sym, e))
    finally:
        conn.close()


@app.route("/api/recent-searches")
def api_recent_searches():
    """Last few symbols this user looked at, each with the engine's own reasoning from that day.

    Falls back to AAPL so the Learn tab always has something real to teach with.
    """
    try:
        limit = max(1, min(int(request.args.get("limit", 3)), 10))
    except Exception:
        limit = 3

    symbols = []
    u = current_user()
    if u:
        conn = get_db()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT symbol FROM recent_searches WHERE user_id=%s "
                    "ORDER BY viewed_at DESC LIMIT %s", (u["id"], limit))
                symbols = [r[0] for r in (cur.fetchall() or [])]
                cur.close()
            except Exception as e:
                logger.warning("api_recent_searches: %s" % e)
            finally:
                conn.close()
    if not symbols:
        symbols = (session.get("recent_searches") or [])[:limit]

    fallback = not symbols
    if fallback:
        symbols = ["AAPL"]

    out = []
    conn = get_db()
    for sym in symbols:
        row = {"symbol": sym, "alpha_score": None, "verdict": None, "breakdown": [], "as_of": None}
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT alpha_score, verdict, features, snap_date FROM feature_snapshots "
                    "WHERE symbol=%s ORDER BY snap_date DESC LIMIT 1", (sym,))
                r = cur.fetchone()
                cur.close()
                if r:
                    row["alpha_score"] = r[0]
                    row["verdict"] = r[1]
                    feats = r[2] or {}
                    if isinstance(feats, dict):
                        row["breakdown"] = feats.get("alpha_breakdown") or []
                    row["as_of"] = str(r[3]) if r[3] else None
            except Exception as e:
                logger.warning("recent-searches lookup %s: %s" % (sym, e))
        out.append(row)
    if conn:
        conn.close()

    return jsonify({"symbols": out, "is_sample": fallback})


@app.route("/api/tts-status")
def api_tts_status():
    """Is the neural voice actually working? Hit this instead of guessing from the sound.

    Returns which model answered, or the exact error Google sent back. The whole reason the voice
    regressed to a robot is that a model rename failed SILENTLY -- this makes that loud.
    """
    key = os.environ.get("GEMINI_KEY", "").strip()
    if not key:
        return jsonify({"ok": False, "reason": "GEMINI_KEY is not set in Railway"}), 200
    voice = os.environ.get("TTS_VOICE", "Achernar").strip() or "Achernar"
    models = [m.strip() for m in os.environ.get(
        "TTS_MODEL",
        "gemini-3.1-flash-tts-preview,gemini-2.5-flash-preview-tts,gemini-2.5-pro-preview-tts"
    ).split(",") if m.strip()]
    tried = []
    for m in models:
        try:
            r = requests.post(
                "https://generativelanguage.googleapis.com/v1beta/models/" + m + ":generateContent?key=" + key,
                json={
                    "contents": [{"parts": [{"text": "Say: Apex Q voice check."}]}],
                    "generationConfig": {
                        "responseModalities": ["AUDIO"],
                        "speechConfig": {"voiceConfig": {"prebuiltVoiceConfig": {"voiceName": voice}}},
                    },
                },
                timeout=30,
            )
            if r.status_code == 200:
                return jsonify({"ok": True, "model": m, "voice": voice, "tried": tried})
            tried.append({"model": m, "status": r.status_code, "error": r.text[:220]})
        except Exception as e:
            tried.append({"model": m, "error": str(e)[:220]})
    return jsonify({"ok": False, "voice": voice, "tried": tried,
                    "hint": "Every model failed. Set TTS_MODEL in Railway to a model name from "
                            "ai.google.dev/gemini-api/docs/speech-generation"}), 200


@app.route("/api/economic-calendar")
def api_economic_calendar():
    try:
        ahead = int(request.args.get("days_ahead", 7))
        back = int(request.args.get("days_back", 3))
    except Exception:
        ahead, back = 7, 3
    try:
        return jsonify(build_economic_calendar(
            days_ahead=max(1, min(ahead, 60)),
            days_back=max(0, min(back, 30)),
        ))
    except Exception as e:
        logger.error("api_economic_calendar: %s" % e)
        return jsonify({"error": str(e), "events": [], "upcoming": []}), 500


# =========================================================================== #
# DoD CONTRACT TRACKER — app integration
#
# The engine lives in dod_contracts.py (a separate file in the repo, same
# pattern as sec_edgar_pipeline). Imported lazily and defensively: if that file
# is missing, these routes return 503 and NOTHING ELSE IN THE APP BREAKS.
# =========================================================================== #

_DOD = None
_DOD_IMPORT_ERROR = None
try:
    import dod_contracts as _DOD
except Exception as _de:
    _DOD_IMPORT_ERROR = str(_de)
    logger.warning("dod_contracts module not available: %s" % _de)

_CT = None
_CT_READY = False
try:
    import clinicaltrials_pipeline as _CT
    _CT_READY = True
except Exception as _cte:
    logger.warning("clinicaltrials_pipeline not available: %s" % _cte)


def _dod_ready():
    return _DOD is not None


def _dod_market_cap(ticker):
    """Market cap for the pure-play ratio. Cached 6h -- a contract's signal doesn't
    hinge on intraday cap moves, and this runs across hundreds of rows."""
    ckey = "dodcap_" + str(ticker)
    c = CACHE.get(ckey)
    if c and (time.time() - c[1]) < 21600:
        return c[0]
    mc = None
    try:
        mc = (yf.Ticker(ticker).info or {}).get("marketCap")
    except Exception:
        mc = None
    CACHE[ckey] = (mc, time.time())
    return mc


def dod_label_impact(max_rows=200):
    """Attach realized outcomes to contract announcements.

    Same point-in-time rule as the feature store: every price used is strictly AFTER
    announced_date. That is what will let you say "sole-source awards to sub-$10B caps
    returned X% excess over five days" and have it be TRUE rather than hindsight.
    """
    if not _dod_ready():
        return {"error": "dod module unavailable"}
    started = time.time()
    out = {"labeled": 0, "symbols": 0, "errors": 0}
    conn = get_db()
    if not conn:
        return {"error": "no database"}
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT ticker FROM historical_stock_impact "
            "WHERE labeled_at IS NULL AND announced_date <= CURRENT_DATE - 8 LIMIT %s",
            (max(1, max_rows // 5),))
        tickers = [r[0] for r in (cur.fetchall() or [])]
        cur.close()
    except Exception as e:
        conn.close()
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not tickers:
        return out

    bench = _fs_closes(_FS_BENCH)
    bmap, bdates = dict(bench), [d for d, _ in bench]

    def fwd(cmap, cdates, start, n):
        fut = [i for i, d in enumerate(cdates) if d > start]
        if len(fut) < n:
            return None
        return cmap[cdates[fut[n - 1]]]

    for t in tickers:
        out["symbols"] += 1
        closes = _fs_closes(t)
        if len(closes) < 10:
            out["errors"] += 1
            continue
        cmap, cdates = dict(closes), [d for d, _ in closes]

        conn = get_db()
        if not conn:
            break
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, announced_date FROM historical_stock_impact "
                "WHERE ticker=%s AND labeled_at IS NULL AND announced_date <= CURRENT_DATE - 8",
                (t,))
            for rid, adate in (cur.fetchall() or []):
                prior = [d for d in cdates if d <= adate]
                if not prior:
                    continue
                base = cmap[prior[-1]]
                if not base:
                    continue

                r1 = fwd(cmap, cdates, adate, 1)
                r5 = fwd(cmap, cdates, adate, 5)
                r20 = fwd(cmap, cdates, adate, 20)
                pct = lambda p: ((p - base) / base * 100.0) if p else None

                nxt = [i for i, d in enumerate(cdates) if d > adate][:5]
                hi = max((cmap[cdates[i]] for i in nxt), default=None)
                mx = ((hi - base) / base * 100.0) if hi else None

                bret = None
                if bdates:
                    bp = [d for d in bdates if d <= adate]
                    bf = fwd(bmap, bdates, adate, 5)
                    if bp and bf and bmap[bp[-1]]:
                        bret = (bf - bmap[bp[-1]]) / bmap[bp[-1]] * 100.0

                exc = (pct(r5) - bret) if (pct(r5) is not None and bret is not None) else None

                cur.execute(
                    "UPDATE historical_stock_impact SET price_at_announce=%s, ret_1d=%s, ret_5d=%s,"
                    " ret_20d=%s, max_move_5d=%s, bench_ret_5d=%s, excess_ret_5d=%s,"
                    " labeled_at=CASE WHEN %s THEN NOW() ELSE NULL END WHERE id=%s",
                    (base, pct(r1), pct(r5), pct(r20), mx, bret, exc,
                     pct(r20) is not None, rid))
                out["labeled"] += 1
            conn.commit()
            cur.close()
        except Exception as e:
            out["errors"] += 1
            logger.warning("dod_label_impact %s: %s" % (t, e))
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            conn.close()
        time.sleep(0.2)

    out["elapsed_sec"] = round(time.time() - started, 1)
    return out


def dod_contracts_for(symbol, limit=5):
    """Recent $100M+ awards mapped to this ticker. [] when there are none."""
    if not _dod_ready() or not symbol:
        return []
    conn = get_db()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT announced_date, recipient_name, headline_amount, award_type, is_idiq,"
            " is_sole_source, is_pure_play, pure_play_ratio, signal_score, signal_note,"
            " branch, source_url, description "
            "FROM dod_contracts WHERE ticker=%s AND needs_review=FALSE "
            "ORDER BY announced_date DESC, headline_amount DESC LIMIT %s",
            (symbol.upper(), limit))
        rows = []
        for r in (cur.fetchall() or []):
            rows.append({
                "date": str(r[0]), "recipient": r[1], "amount": float(r[2] or 0),
                "award_type": r[3], "is_idiq": bool(r[4]), "is_sole_source": bool(r[5]),
                "is_pure_play": bool(r[6]),
                "pct_of_market_cap": round(float(r[7]) * 100, 1) if r[7] else None,
                "signal_score": r[8], "signal_note": r[9], "branch": r[10],
                "url": r[11], "description": (r[12] or "")[:300],
            })
        cur.close()
        return rows
    except Exception as e:
        logger.warning("dod_contracts_for %s: %s" % (symbol, e))
        return []
    finally:
        conn.close()


@app.route("/cron/dod-contracts")
def cron_dod_contracts():
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    if not _dod_ready():
        return jsonify({"error": "dod_contracts.py not deployed", "detail": _DOD_IMPORT_ERROR}), 503
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        return jsonify(_DOD.run_dod_pipeline(conn, market_cap_lookup=_dod_market_cap))
    except Exception as e:
        logger.error("cron_dod_contracts: %s" % e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/cron/dod-exhibit21")
def cron_dod_exhibit21():
    """Harvest Exhibit 21 subsidiary lists, then re-resolve anything still unmapped.

    Run WEEKLY, not hourly -- 10-Ks are annual, and the SEC will block an IP that hammers them.
    """
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    if not _dod_ready():
        return jsonify({"error": "dod_contracts.py not deployed"}), 503
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        harvest = _DOD.harvest_exhibit21(conn, limit=int(request.args.get("limit", 40)))
        backfill = _DOD.backfill_unresolved(conn)
        return jsonify({"harvest": harvest, "backfill": backfill})
    except Exception as e:
        logger.error("cron_dod_exhibit21: %s" % e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/cron/dod-label-impact")
def cron_dod_label_impact():
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    return jsonify(dod_label_impact())


@app.route("/api/dod-contracts")
def api_dod_contracts():
    """The feed. Defaults to the highest-signal recent awards, not merely the biggest."""
    if not _dod_ready():
        return jsonify({"error": "dod_contracts.py not deployed", "contracts": []}), 503
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database", "contracts": []}), 503
    try:
        days = max(1, min(int(request.args.get("days", 30)), 365))
        pure_only = request.args.get("pure_play") == "1"
        cur = conn.cursor()
        cur.execute(
            "SELECT announced_date, ticker, parent_name, recipient_name, headline_amount,"
            " award_type, is_idiq, is_sole_source, is_pure_play, pure_play_ratio,"
            " signal_score, signal_note, branch, source_url "
            "FROM dod_contracts "
            "WHERE announced_date >= CURRENT_DATE - %s AND needs_review = FALSE "
            + ("AND is_pure_play = TRUE " if pure_only else "") +
            "ORDER BY signal_score DESC, headline_amount DESC LIMIT 50",
            (days,))
        out = []
        for r in (cur.fetchall() or []):
            out.append({
                "date": str(r[0]), "ticker": r[1], "company": r[2], "recipient": r[3],
                "amount": float(r[4] or 0), "award_type": r[5], "is_idiq": bool(r[6]),
                "is_sole_source": bool(r[7]), "is_pure_play": bool(r[8]),
                "pct_of_market_cap": round(float(r[9]) * 100, 1) if r[9] else None,
                "signal_score": r[10], "signal_note": r[11], "branch": r[12], "url": r[13],
            })
        cur.execute("SELECT COUNT(*) FROM dod_contracts WHERE needs_review")
        review = (cur.fetchone() or [0])[0]
        cur.close()
        return jsonify({"contracts": out, "value_floor": 100000000,
                        "unresolved_awaiting_review": review})
    except Exception as e:
        return jsonify({"error": str(e), "contracts": []}), 500
    finally:
        conn.close()


@app.route("/api/dod-contracts/<symbol>")
def api_dod_contracts_symbol(symbol):
    return jsonify({"symbol": symbol.upper(), "contracts": dod_contracts_for(symbol)})


@app.route("/api/dod-review-queue")
def api_dod_review_queue():
    """Contracts we could not confidently map to a ticker. This is an OPERATIONAL SURFACE,
    not a nice-to-have: entity resolution cannot be fully automated, and a wrong ticker on a
    $100M headline is the one error that would genuinely damage the product."""
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, announced_date, recipient_name, headline_amount, resolution_conf "
            "FROM dod_contracts WHERE needs_review ORDER BY headline_amount DESC LIMIT 100")
        rows = [{"id": r[0], "date": str(r[1]), "recipient": r[2],
                 "amount": float(r[3] or 0), "confidence": r[4]} for r in (cur.fetchall() or [])]
        cur.close()
        return jsonify({"queue": rows, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# =========================================================================== #
# PORTFOLIO TRIAL TRACKER
#
# Built for a live 30-day run with real money against the S&P 500.
#
# READ THIS BEFORE YOU TRUST THE NUMBER IT PRODUCES:
#
# A portfolio of a handful of stocks has a tracking error against SPY of roughly
# 5-8% over 30 days FROM NOISE ALONE. So beating the index by 6% in a month is
# not evidence the engine works. One earnings surprise on one holding swamps the
# entire signal. Thirty days measures weather, not climate.
#
# What this DOES measure honestly, and what makes the trial worth running:
#
#   CHURN -- how often the engine flips its own verdict on the same stock with
#   nothing new happening. That is a real, detectable defect, 30 days is plenty
#   to catch it, and no amount of backtesting surfaces it as clearly as watching
#   your own holdings. If AAPL goes APPROVE -> PASS -> APPROVE in three weeks on
#   no news, the engine is unstable and the score is noise.
#
# The alpha question is answered by feature_snapshots + /api/backtest, on its own
# clock, in about 90 days -- whether or not a dollar is ever traded. So the
# trial's benchmark number is reported here with its own honesty warning
# attached, and it is not allowed to pretend to be a result.
# =========================================================================== #

TRIAL_BENCH = "SPY"


def trial_create_tables():
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        # One trial per user. Records the starting line so performance is measured from a fixed
        # point rather than recomputed from whatever the portfolio happens to look like today.
        cur.execute(
            "CREATE TABLE IF NOT EXISTS portfolio_trial ("
            "user_id INTEGER PRIMARY KEY,"
            "started_at DATE NOT NULL,"
            "start_value NUMERIC(14,2),"
            "bench_start_price NUMERIC(12,4),"
            "target_days INTEGER DEFAULT 30,"
            "notes TEXT,"
            "created_at TIMESTAMP DEFAULT NOW())"
        )
        # The attention dot. One row per held symbol. changed_at is set when the engine's opinion
        # moves; acknowledged_at is set when the user actually looks. The dot is the difference.
        cur.execute(
            "CREATE TABLE IF NOT EXISTS portfolio_watch ("
            "id SERIAL PRIMARY KEY,"
            "user_id INTEGER NOT NULL,"
            "symbol TEXT NOT NULL,"
            "last_verdict TEXT,"
            "last_alpha INTEGER,"
            "prev_verdict TEXT,"
            "prev_alpha INTEGER,"
            "change_note TEXT,"
            "changed_at TIMESTAMP,"
            "acknowledged_at TIMESTAMP,"
            "flip_count INTEGER DEFAULT 0,"      # <- the churn counter
            "checked_at TIMESTAMP DEFAULT NOW(),"
            "UNIQUE (user_id, symbol))"
        )
        conn.commit()
        cur.close()
        logger.info("trial tracker: tables ready")
    except Exception as e:
        logger.error("trial_create_tables: %s" % e)
    finally:
        conn.close()


def _trial_bench_price():
    try:
        closes = _mom_closes(TRIAL_BENCH)
        return closes[-1] if closes else None
    except Exception:
        return None


@app.route("/api/trial/start", methods=["POST"])
def api_trial_start():
    """Fix the starting line. Everything after is measured from here."""
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    trial_create_tables()
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        p = compute_portfolio(u["id"]) or {}
        start_val = p.get("total_value") or p.get("total") or 0
        bench = _trial_bench_price()
        body = request.get_json(silent=True) or {}
        days = int(body.get("days", 30))

        cur = conn.cursor()
        cur.execute(
            "INSERT INTO portfolio_trial (user_id, started_at, start_value, bench_start_price, target_days, notes) "
            "VALUES (%s, CURRENT_DATE, %s, %s, %s, %s) "
            "ON CONFLICT (user_id) DO UPDATE SET started_at=CURRENT_DATE, start_value=EXCLUDED.start_value,"
            " bench_start_price=EXCLUDED.bench_start_price, target_days=EXCLUDED.target_days",
            (u["id"], start_val, bench, days, (body.get("notes") or "")[:500]),
        )
        conn.commit()
        cur.close()
        return jsonify({"ok": True, "started": str(_sec_date.today()),
                        "start_value": float(start_val or 0),
                        "bench_start_price": float(bench or 0), "days": days})
    except Exception as e:
        logger.error("api_trial_start: %s" % e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/trial")
def api_trial():
    """Performance vs SPY, plus the churn number -- and the honesty warning that belongs with both."""
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        cur = conn.cursor()
        cur.execute("SELECT started_at, start_value, bench_start_price, target_days "
                    "FROM portfolio_trial WHERE user_id=%s", (u["id"],))
        t = cur.fetchone()
        if not t:
            cur.close()
            return jsonify({"active": False})
        started, start_val, bench_start, days = t

        cur.execute("SELECT COUNT(*), COALESCE(SUM(flip_count),0) FROM portfolio_watch WHERE user_id=%s",
                    (u["id"],))
        n_pos, flips = cur.fetchone() or (0, 0)
        cur.close()

        p = compute_portfolio(u["id"]) or {}
        cur_val = float(p.get("total_value") or p.get("total") or 0)
        bench_now = _trial_bench_price()

        port_ret = ((cur_val - float(start_val)) / float(start_val) * 100.0) if start_val else None
        bench_ret = ((bench_now - float(bench_start)) / float(bench_start) * 100.0) if (bench_now and bench_start) else None
        excess = (port_ret - bench_ret) if (port_ret is not None and bench_ret is not None) else None
        elapsed = (_sec_date.today() - started).days

        return jsonify({
            "active": True,
            "started": str(started),
            "day": elapsed,
            "target_days": days,
            "start_value": float(start_val or 0),
            "current_value": cur_val,
            "portfolio_return_pct": round(port_ret, 2) if port_ret is not None else None,
            "benchmark_return_pct": round(bench_ret, 2) if bench_ret is not None else None,
            "excess_vs_spy_pct": round(excess, 2) if excess is not None else None,
            "positions": n_pos,
            # THE NUMBER THAT ACTUALLY MEANS SOMETHING IN 30 DAYS.
            "verdict_flips": int(flips or 0),
            "churn_note": (
                "The engine has changed its mind %d time(s) across %d position(s). "
                "Frequent flips on quiet news mean the score is unstable -- that is a real defect, "
                "and it is the thing this trial can actually prove." % (int(flips or 0), int(n_pos or 0))
            ),
            # Drift scales with 1/sqrt(n): ~10% annualized tracking error for a large-cap book,
            # over roughly a twelfth of a year. 8 names -> ~3.5%; 20 names -> ~2.2%. Computed
            # rather than hardcoded, because quoting a stale figure is its own small dishonesty.
            "expected_noise_pct": round(10.0 / max(1.0, float(n_pos or 1) ** 0.5) / (12 ** 0.5) * 3.46, 1),
            "honesty": (
                "Noise alone moves a %d-stock portfolio about %.1f%% away from the S&P over 30 days. "
                "A good engine might add ~0.25%% of real alpha in that window -- so the signal is "
                "roughly a TENTH of the noise. This number cannot tell you whether the engine works, "
                "in either direction, and more stocks does not fix that: 30 days is still ONE "
                "observation. What IS evidence: %d positions x 30 days = ~%d stock-level rows in the "
                "feature store, each with the engine's reasoning, labeled forward. That is the real "
                "test, and it runs whether or not you trade. Do not market this result."
                % (int(n_pos or 0),
                   10.0 / max(1.0, float(n_pos or 1) ** 0.5) / (12 ** 0.5) * 3.46,
                   int(n_pos or 0), int(n_pos or 0) * 30)
            ),
        })
    except Exception as e:
        logger.error("api_trial: %s" % e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


def refresh_one_user(conn, user):
    """Re-run the engine on every holding; flag what changed. Returns a summary."""
    uid = user["id"]
    out = {"checked": 0, "changed": 0}
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM holdings WHERE user_id=%s", (uid,))
    syms = [r[0] for r in (cur.fetchall() or [])]
    cur.close()

    for sym in syms:
        out["checked"] += 1
        try:
            r = compute_full_report(sym)
            if not r:
                continue
            verdict = r.get("verdict")
            alpha = r.get("alpha_score")

            cur = conn.cursor()
            cur.execute("SELECT last_verdict, last_alpha, flip_count FROM portfolio_watch "
                        "WHERE user_id=%s AND symbol=%s", (uid, sym))
            prev = cur.fetchone()

            if not prev:
                cur.execute(
                    "INSERT INTO portfolio_watch (user_id, symbol, last_verdict, last_alpha, checked_at) "
                    "VALUES (%s,%s,%s,%s,NOW()) ON CONFLICT (user_id, symbol) DO NOTHING",
                    (uid, sym, verdict, alpha))
                conn.commit(); cur.close()
                continue

            pv, pa, flips = prev[0], prev[1], (prev[2] or 0)
            verdict_moved = bool(pv and verdict and pv != verdict)
            # An 8-point swing is a real move; 1-2 points is the engine breathing. Dotting the
            # portfolio for noise would make the dot worthless within a week.
            alpha_moved = bool(pa is not None and alpha is not None and abs(alpha - pa) >= 8)

            if verdict_moved or alpha_moved:
                bits = []
                if verdict_moved:
                    bits.append("Verdict moved from %s to %s." % (pv, verdict))
                if alpha_moved:
                    bits.append("Alpha Score moved %s%d, from %s to %s."
                                % ("+" if alpha > pa else "", alpha - pa, pa, alpha))
                note = " ".join(bits)

                cur.execute(
                    "UPDATE portfolio_watch SET prev_verdict=%s, prev_alpha=%s, last_verdict=%s,"
                    " last_alpha=%s, change_note=%s, changed_at=NOW(), acknowledged_at=NULL,"
                    " flip_count=%s, checked_at=NOW() WHERE user_id=%s AND symbol=%s",
                    (pv, pa, verdict, alpha, note, flips + (1 if verdict_moved else 0), uid, sym))
                conn.commit()
                out["changed"] += 1

                try:
                    _deliver_alert(conn, user, "verdict_change", sym,
                                   "%s: %s" % (sym, (verdict or "updated")),
                                   note + " Worth a look before the next open.",
                                   "/?symbol=" + sym)
                except Exception as ae:
                    logger.warning("trial alert %s: %s" % (sym, ae))
            else:
                cur.execute("UPDATE portfolio_watch SET last_verdict=%s, last_alpha=%s, checked_at=NOW() "
                            "WHERE user_id=%s AND symbol=%s", (verdict, alpha, uid, sym))
                conn.commit()
            cur.close()
        except Exception as e:
            logger.warning("refresh_one_user %s/%s: %s" % (uid, sym, e))
            try:
                conn.rollback()
            except Exception:
                pass
    return out


@app.route("/cron/portfolio-refresh")
def cron_portfolio_refresh():
    """Re-run the engine across every user's holdings. Schedule TWICE daily:
    ~7:00am ET (before the open, so you can act) and ~4:45pm ET (after the close, so the day's
    move is in). Running it more often than that would just manufacture churn."""
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    trial_create_tables()
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT h.user_id, u.username FROM holdings h "
                    "JOIN users u ON u.id = h.user_id")
        users = [{"id": r[0], "username": r[1]} for r in (cur.fetchall() or [])]
        cur.close()

        total = {"users": len(users), "checked": 0, "changed": 0}
        for user in users:
            s = refresh_one_user(conn, user)
            total["checked"] += s["checked"]
            total["changed"] += s["changed"]
        logger.info("portfolio refresh: %s" % total)
        return jsonify(total)
    except Exception as e:
        logger.error("cron_portfolio_refresh: %s" % e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/portfolio-alerts")
def api_portfolio_alerts():
    """Which holdings changed since the user last looked. This drives the dot."""
    u = current_user()
    if not u:
        return jsonify({"alerts": {}})
    conn = get_db()
    if not conn:
        return jsonify({"alerts": {}})
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT symbol, change_note, changed_at, last_verdict, prev_verdict, flip_count "
            "FROM portfolio_watch WHERE user_id=%s AND changed_at IS NOT NULL "
            "AND (acknowledged_at IS NULL OR acknowledged_at < changed_at)", (u["id"],))
        out = {}
        for r in (cur.fetchall() or []):
            out[r[0]] = {"note": r[1], "changed_at": str(r[2]), "verdict": r[3],
                         "prev_verdict": r[4], "flips": r[5]}
        cur.close()
        return jsonify({"alerts": out, "count": len(out)})
    except Exception as e:
        logger.warning("api_portfolio_alerts: %s" % e)
        return jsonify({"alerts": {}})
    finally:
        conn.close()


@app.route("/api/portfolio-ack", methods=["POST"])
def api_portfolio_ack():
    """Mark a holding as seen. The dot goes out."""
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    sym = ((request.get_json(silent=True) or {}).get("symbol") or "").upper()
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        cur = conn.cursor()
        if sym:
            cur.execute("UPDATE portfolio_watch SET acknowledged_at=NOW() "
                        "WHERE user_id=%s AND symbol=%s", (u["id"], sym))
        else:
            cur.execute("UPDATE portfolio_watch SET acknowledged_at=NOW() WHERE user_id=%s", (u["id"],))
        conn.commit()
        cur.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# =========================================================================== #
# FORWARD SIGNALS + RISK OVERLAY + CONVERGENCE
#
# The spec asked to add points to "Forward Signals" and deduct from a "Risk
# Overlay". Neither layer existed -- the engine had seven flat factors and an
# alignment bonus. So they are built here.
#
# The design choice that matters: RISK IS AN OVERLAY, NOT A FACTOR. It does not
# compete with momentum in the same pool of points; it DEDUCTS from the total
# and can CAP the verdict. Good momentum should not be able to buy its way past
# a going-concern letter.
# =========================================================================== #

SEC_ACTIVIST_FORMS = {"SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"}
SEC_COMMENT_FORMS = {"UPLOAD", "CORRESP"}

# 13D = ACTIVIST. The filer intends to influence control. This is the signal.
# 13G = PASSIVE. Vanguard, BlackRock and State Street file these on essentially every large company
#       in America. Scoring a 13G would hand +10 to every large cap -- a constant, which is the same
#       as no signal at all. We STORE 13G for completeness and never score it.
def _is_activist_form(form):
    return str(form or "").upper().startswith("SC 13D")


# Item 4 of a 13D is "Purpose of Transaction". These phrases are the ones that separate a fund
# that bought a stake from a fund that intends to do something about it.
_ACTIVIST_INTENT = re.compile(
    r"board seat|board of directors|board representation|nominate|director nominee|"
    r"asset sale|sale of the (company|issuer)|strategic alternatives|spin[- ]off|divest|"
    r"proxy contest|consent solicitation|remove.{0,20}(director|officer)|"
    r"replace.{0,20}management|unlock (shareholder )?value|maximize (shareholder )?value",
    re.IGNORECASE,
)

# Comment letters: MOST ARE ROUTINE. The SEC asks companies to reword a non-GAAP table or clarify a
# segment disclosure all the time, and it means nothing. Blanket-penalizing every letter would drag
# down a large slice of perfectly healthy companies at random. Only these subjects are substantive.
_COMMENT_SEVERE = re.compile(
    r"revenue recognition|restat(e|ement)|material weakness|internal control over financial|"
    r"going concern|impairment|goodwill|fraud|misstat|non-reliance|item 4\.02|"
    r"revis(e|ion) (of )?(prior|previously issued)|audit committee",
    re.IGNORECASE,
)
_COMMENT_ROUTINE = re.compile(
    r"non-gaap|segment (reporting|disclosure)|cover page|exhibit index|xbrl|"
    r"management'?s discussion|risk factor|iXBRL|signature",
    re.IGNORECASE,
)


def signals_create_tables():
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS activist_filings ("
            "id SERIAL PRIMARY KEY,"
            "accession TEXT UNIQUE,"
            "ticker TEXT,"
            "cik TEXT,"
            "form_type TEXT,"
            "is_activist BOOLEAN DEFAULT FALSE,"   # 13D true, 13G false -- see note above
            "fund_name TEXT,"
            "shares BIGINT,"
            "percentage REAL,"
            "purpose_text TEXT,"
            "has_control_intent BOOLEAN DEFAULT FALSE,"
            "filing_date DATE,"
            "raw_json JSONB,"
            "created_at TIMESTAMP DEFAULT NOW())"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS ix_act_ticker ON activist_filings(ticker, filing_date DESC)")

        cur.execute(
            "CREATE TABLE IF NOT EXISTS sec_comment_letters ("
            "id SERIAL PRIMARY KEY,"
            "accession TEXT UNIQUE,"
            "ticker TEXT,"
            "cik TEXT,"
            "form_type TEXT,"
            "issue_description TEXT,"
            "severity TEXT,"                        # 'severe' | 'routine'
            "filing_date DATE,"
            "raw_json JSONB,"
            "created_at TIMESTAMP DEFAULT NOW())"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS ix_cl_ticker ON sec_comment_letters(ticker, filing_date DESC)")

        cur.execute(
            "CREATE TABLE IF NOT EXISTS lobbying_registrations ("
            "id SERIAL PRIMARY KEY,"
            "filing_uuid TEXT UNIQUE,"
            "ticker TEXT,"
            "client_name TEXT,"
            "lobbying_firm TEXT,"
            "issues TEXT,"
            "bill_numbers TEXT,"
            "form_type TEXT DEFAULT 'LD-1',"
            "is_defensive BOOLEAN DEFAULT FALSE,"   # investigation / enforcement / regulation
            "filing_date DATE,"
            "raw_json JSONB,"
            "created_at TIMESTAMP DEFAULT NOW())"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS ix_lr_ticker ON lobbying_registrations(ticker, filing_date DESC)")
        conn.commit()
        cur.close()
        logger.info("new signal tables ready")
    except Exception as e:
        logger.error("signals_create_tables: %s" % e)
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Readers. These feed both the report cards and the scoring layers.
# --------------------------------------------------------------------------- #

def activist_for(symbol, days=90):
    """Recent 13D filings. 13G is excluded on purpose -- passive stakes are not a signal."""
    if not symbol:
        return []
    conn = get_db()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT fund_name, shares, percentage, purpose_text, has_control_intent, filing_date, form_type "
            "FROM activist_filings WHERE ticker=%s AND is_activist=TRUE "
            "AND filing_date >= CURRENT_DATE - %s ORDER BY filing_date DESC LIMIT 5",
            (symbol.upper(), days))
        out = []
        for r in (cur.fetchall() or []):
            out.append({
                "fund": r[0], "shares": int(r[1] or 0), "pct": float(r[2] or 0),
                "purpose": (r[3] or "")[:600], "control_intent": bool(r[4]),
                "date": str(r[5]), "form": r[6],
            })
        cur.close()
        return out
    except Exception as e:
        logger.warning("activist_for %s: %s" % (symbol, e))
        return []
    finally:
        conn.close()


def comment_letters_for(symbol, days=180):
    if not symbol:
        return []
    conn = get_db()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT issue_description, severity, filing_date, form_type FROM sec_comment_letters "
            "WHERE ticker=%s AND filing_date >= CURRENT_DATE - %s "
            "ORDER BY (severity='severe') DESC, filing_date DESC LIMIT 5",
            (symbol.upper(), days))
        out = [{"issue": r[0], "severity": r[1], "date": str(r[2]), "form": r[3]}
               for r in (cur.fetchall() or [])]
        cur.close()
        return out
    except Exception as e:
        logger.warning("comment_letters_for %s: %s" % (symbol, e))
        return []
    finally:
        conn.close()


def lobbying_registrations_for(symbol, days=60):
    if not symbol:
        return []
    conn = get_db()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT lobbying_firm, issues, bill_numbers, is_defensive, filing_date "
            "FROM lobbying_registrations WHERE ticker=%s AND filing_date >= CURRENT_DATE - %s "
            "ORDER BY filing_date DESC LIMIT 5", (symbol.upper(), days))
        out = [{"firm": r[0], "issues": r[1], "bills": r[2], "defensive": bool(r[3]),
                "date": str(r[4])} for r in (cur.fetchall() or [])]
        cur.close()
        return out
    except Exception as e:
        logger.warning("lobbying_registrations_for %s: %s" % (symbol, e))
        return []
    finally:
        conn.close()


def clinical_changes_for(symbol, days=90):
    if not _CT_READY or not symbol:
        return []
    conn = get_db()
    if not conn:
        return []
    try:
        return _CT.trial_changes_for(conn, symbol, days=days)
    except Exception as e:
        logger.warning("clinical_changes_for %s: %s" % (symbol, e))
        return []
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# THE TWO NEW LAYERS
# --------------------------------------------------------------------------- #

def compute_forward_signals(symbol, activist, lobbying, trials):
    """Forward Signals, 0-20. Things that are known but not yet in the price.

    Not a prediction. A measure of information that has been disclosed and probably not absorbed.
    """
    pts = 10  # neutral midpoint; absence of these signals is not bearish, it is just quiet
    notes = []

    for a in (activist or []):
        if a["control_intent"]:
            pts += 15
            notes.append("%s filed a 13D stating an intent to influence control -- board seats, "
                         "asset sales or a strategic review. Activists with stated intent have "
                         "historically been followed by real corporate change." % (a["fund"] or "An activist fund"))
        else:
            pts += 10
            notes.append("%s disclosed an activist stake (13D) of %.1f%%." % (a["fund"] or "A fund", a["pct"]))
        break  # one activist is the signal; three do not make it three times truer

    for l in (lobbying or []):
        if l["defensive"]:
            notes.append("New lobbying registration, but the issues involve investigation or "
                         "regulation -- this is defence, not offence.")
        else:
            pts += 5
            notes.append("New lobbying relationship established: %s hired to lobby on %s. Companies "
                         "build political strategy before they need it."
                         % (l["firm"] or "a firm", (l["issues"] or "unspecified issues")[:80]))
        break

    for t in (trials or []):
        if t["direction"] == "setback" and t["significance"] >= 75:
            pts -= 10
            notes.append("A clinical trial was %s. %s" % (t["new"], t["why"][:110]))
        elif t["direction"] == "advance" and t["significance"] >= 70:
            pts += 10
            notes.append("A clinical programme advanced (%s: %s to %s)." % (t["field"], t["old"], t["new"]))
        break

    pts = max(0, min(20, pts))
    return {"points": pts, "notes": notes}


def compute_risk_overlay(symbol, comments, lobbying, trials, ins_csells=0):
    """Risk Overlay. Returns a NEGATIVE deduction and an optional verdict CAP.

    This does not compete for points inside the score. It subtracts from the total and can cap the
    verdict outright -- because a going-concern letter should not be outvoted by good momentum.
    """
    deduction = 0
    cap = None
    notes = []

    severe_letter = False
    for c in (comments or []):
        if c["severity"] == "severe":
            severe_letter = True
            deduction += 5
            notes.append("The SEC has an open comment letter on a substantive accounting matter: %s"
                         % (c["issue"] or "an accounting or disclosure issue")[:140])
            break
    # Routine letters are noted for transparency but cost NOTHING. Most comment letters are the SEC
    # asking a company to reword a table. Penalising those would punish healthy companies at random.

    for l in (lobbying or []):
        if l["defensive"]:
            deduction += 3
            notes.append("The company just hired lobbyists on issues involving investigation or "
                         "regulation. Companies lobby hardest when something is coming.")
            break

    for t in (trials or []):
        if t["direction"] == "setback" and t["significance"] >= 75:
            deduction += 5
            notes.append("A clinical trial setback adds real risk to the pipeline story.")
            break

    # THE BEARISH CAP. Kept exactly as specified, and I would keep it even if it had not been:
    # cluster selling by executives WHILE the SEC is asking about the accounting is the single
    # combination most worth being cautious about. Capping costs a missed gain. Not capping costs money.
    if severe_letter and (ins_csells or 0) >= 3:
        cap = "WATCH"
        deduction += 15
        notes.append("Three or more executives sold while the SEC has an open substantive comment "
                     "letter. The verdict is capped at WATCH regardless of the score. Insiders "
                     "selling into an accounting inquiry is the pattern that most deserves patience.")

    return {"deduction": min(30, deduction), "cap": cap, "notes": notes}


# --------------------------------------------------------------------------- #
# CONVERGENCE
#
# READ BEFORE TRUSTING THIS: convergence is a HYPOTHESIS, not a finding.
#
# With ten signals there are 120 possible three-way combinations. The chance that SOME three of
# them land inside the same 14-day window is far higher than intuition suggests -- so "three
# signals aligned, this is rare" is a claim that has to be MEASURED, not asserted.
#
# So this detects convergence, shows it, and adds a BOUNDED bonus. It does NOT override the
# verdict to APPROVE, which is what the spec asked for. An override in the bullish direction, on
# an untested rule, on a product about to be used with real money, is the one thing here that
# could actually cost someone. The bearish cap stays, because the asymmetry is right: a wrong cap
# costs a missed gain, a wrong bullish override costs cash.
#
# Every convergence event is written into the feature store. In ~90 days /api/backtest can say
# whether convergence actually predicts anything. THEN turn the override on, with evidence.
# --------------------------------------------------------------------------- #

def detect_convergence(symbol, sig, activist, trials, comments, cong):
    """Returns {converged: bool, signals: [...], bonus: int, cap: str|None, implication: str}."""
    out = {"converged": False, "signals": [], "bonus": 0, "cap": None, "implication": "",
           "hypothesis_note": ""}
    try:
        hits = []

        ins_cbuys = int(sig.get("ins_cbuys") or 0)
        ins_csells = int(sig.get("ins_csells") or 0)
        analyst_recent = int(sig.get("analyst_recent") or 0)
        cong_buys = int(sig.get("cong_buys") or 0)
        cong_committee = bool(sig.get("cong_committee"))

        cluster_buy = ins_cbuys >= 2
        upgrade = analyst_recent > 0
        committee_buy = cong_buys > 0 and cong_committee
        has_activist = bool(activist)
        control_intent = any(a.get("control_intent") for a in (activist or []))
        trial_advance = any(t["direction"] == "advance" and t["significance"] >= 70 for t in (trials or []))
        severe_letter = any(c["severity"] == "severe" for c in (comments or []))

        # 1. Insiders buying + the street turning bullish, together.
        if cluster_buy and upgrade:
            hits.append("Two or more executives bought shares on the open market, and analysts "
                        "raised their view -- the people inside and the people covering it moved "
                        "the same way.")
            out["bonus"] += 10

        # 2. A committee-relevant congressional buy alongside a real pipeline advance.
        if committee_buy and trial_advance:
            hits.append("A member of Congress on a relevant committee bought shares, and a clinical "
                        "programme advanced in the same window.")
            out["bonus"] += 10

        # 3. The one the spec wanted to force APPROVE on. Detected and shown -- NOT overriding.
        if has_activist and committee_buy:
            hits.append("An activist fund disclosed a stake%s, and a committee-relevant "
                        "congressional buy landed in the same window."
                        % (" with a stated intent to influence control" if control_intent else ""))
            out["bonus"] += 10
            out["hypothesis_note"] = (
                "This is the pattern the engine was told to treat as a high-conviction buy signal. "
                "It is being SHOWN, not acted on. Apex Q has never tested whether this combination "
                "predicts anything, and with this many signals some three of them will line up by "
                "chance more often than it feels like they should. Every occurrence is being logged; "
                "once there are enough of them, the backtest can answer the question properly. Until "
                "then it is a pattern worth your attention, not a reason to buy."
            )

        # 4. The bearish one. This DOES bind.
        if ins_csells >= 3 and severe_letter:
            hits.append("Three or more executives sold while the SEC has an open substantive "
                        "comment letter on the accounting.")
            out["cap"] = "WATCH"
            out["bonus"] -= 15

        if not hits:
            return out

        out["converged"] = True
        out["signals"] = hits
        out["bonus"] = max(-15, min(20, out["bonus"]))

        n = len(hits)
        if out["cap"]:
            out["implication"] = (
                "Signals are aligning in a cautionary direction. When executives sell into an open "
                "accounting inquiry, the safest read is to wait for it to resolve. The verdict is "
                "held at WATCH regardless of the score."
            )
        else:
            out["implication"] = (
                "%d independent signal%s aligned within the last 14 days. Signals that come from "
                "genuinely different places -- insiders, the street, Congress, a regulator, a trial "
                "readout -- carry more weight together than any of them does alone, because they are "
                "unlikely to be wrong in the same direction for the same reason."
                % (n, "s" if n != 1 else "")
            )
        return out
    except Exception as e:
        logger.warning("detect_convergence %s: %s" % (symbol, e))
        return out

# =========================================================================== #
# SIGNAL PIPELINES: 13D/G, SEC comment letters, LD-1, clinical trials
# =========================================================================== #

_CT = None
_CT_READY = False
try:
    import clinicaltrials_pipeline as _CT
    _CT_READY = True
except Exception as _cte:
    logger.warning("clinicaltrials_pipeline unavailable: %s" % _cte)


def _sig_doc_text(cik, accession, max_chars=60000):
    """Primary document text of a filing. Best effort; empty string on failure."""
    try:
        acc = str(accession).replace("-", "")
        base = "https://www.sec.gov/Archives/edgar/data/%s/%s" % (int(cik), acc)
        idx = _sec_get(base + "/index.json", as_json=True) if _dod_ready() else None
        if not idx:
            r = requests.get(base + "/index.json", headers={"User-Agent": SEC_UA_STR}, timeout=20)
            idx = r.json() if r.status_code == 200 else None
        if not idx:
            return ""
        for item in ((idx.get("directory") or {}).get("item") or []):
            nm = str(item.get("name", "")).lower()
            if nm.endswith((".htm", ".html", ".txt")) and "index" not in nm:
                r = requests.get(base + "/" + item.get("name"),
                                 headers={"User-Agent": SEC_UA_STR}, timeout=20)
                if r.status_code != 200:
                    continue
                t = re.sub(r"<[^>]+>", " ", r.text)
                t = html.unescape(t)
                return re.sub(r"\s+", " ", t)[:max_chars]
    except Exception as e:
        logger.warning("_sig_doc_text %s: %s" % (accession, e))
    return ""


SEC_UA_STR = os.environ.get("SEC_USER_AGENT", "ApexQ contact@apexq.io")


def sec_process_activist_filing(conn, f):
    """13D / 13G. Only 13D is treated as a signal -- see the note on SEC_ACTIVIST_FORMS."""
    accession = f.get("accession_number")
    form = f.get("form_type", "")
    cik = f.get("cik")
    ticker = f.get("ticker") or _sec_cik_to_ticker(cik)
    activist = _is_activist_form(form)

    txt = _sig_doc_text(cik, accession) if activist else ""

    fund = None
    m = re.search(r"(?:Name of Reporting Person[s]?\.?|NAME OF REPORTING PERSON)\s*:?\s*([A-Z][^\n\r\.]{3,80})",
                  txt, re.IGNORECASE)
    if m:
        fund = m.group(1).strip()[:120]

    pct = None
    m = re.search(r"(?:PERCENT OF CLASS REPRESENTED BY AMOUNT|Percent of Class)[^0-9]{0,60}([\d.]+)\s*%",
                  txt, re.IGNORECASE)
    if m:
        try:
            pct = float(m.group(1))
        except Exception:
            pct = None

    shares = None
    m = re.search(r"(?:AGGREGATE AMOUNT BENEFICIALLY OWNED[^0-9]{0,60})([\d,]{4,})", txt, re.IGNORECASE)
    if m:
        try:
            shares = int(m.group(1).replace(",", ""))
        except Exception:
            shares = None

    # Item 4, "Purpose of Transaction" -- the part that separates a stake from an agenda.
    purpose = ""
    m = re.search(r"Item\s*4[.\s\-]*Purpose of (?:the )?Transaction(.{0,3000}?)Item\s*5",
                  txt, re.IGNORECASE | re.DOTALL)
    if m:
        purpose = re.sub(r"\s+", " ", m.group(1)).strip()[:2500]

    intent = bool(_ACTIVIST_INTENT.search(purpose)) if purpose else False

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO activist_filings (accession, ticker, cik, form_type, is_activist, fund_name,"
        " shares, percentage, purpose_text, has_control_intent, filing_date, raw_json) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (accession) DO NOTHING",
        (accession, ticker, str(cik), form, activist, fund, shares, pct, purpose, intent,
         f.get("filing_date"), _SecJson({"form": form})))
    conn.commit()
    cur.close()


def sec_process_comment_letter(conn, f):
    """UPLOAD (SEC -> company) and CORRESP (company -> SEC).

    Classified by severity. MOST COMMENT LETTERS ARE ROUTINE -- the SEC asking a company to reword
    a non-GAAP table means nothing. Only substantive accounting matters are scored.
    """
    accession = f.get("accession_number")
    cik = f.get("cik")
    ticker = f.get("ticker") or _sec_cik_to_ticker(cik)
    txt = _sig_doc_text(cik, accession, max_chars=40000)

    severe = bool(_COMMENT_SEVERE.search(txt))
    routine = bool(_COMMENT_ROUTINE.search(txt))
    severity = "severe" if severe else "routine"

    issue = ""
    pat = _COMMENT_SEVERE if severe else _COMMENT_ROUTINE
    m = pat.search(txt)
    if m:
        start = max(0, m.start() - 160)
        issue = re.sub(r"\s+", " ", txt[start:m.end() + 320]).strip()[:600]
    if not issue:
        issue = re.sub(r"\s+", " ", txt[:400]).strip()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO sec_comment_letters (accession, ticker, cik, form_type, issue_description,"
        " severity, filing_date, raw_json) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (accession) DO NOTHING",
        (accession, ticker, str(cik), f.get("form_type"), issue, severity,
         f.get("filing_date"), _SecJson({"routine_match": routine})))
    conn.commit()
    cur.close()


def _sec_cik_to_ticker(cik):
    try:
        m = sec_ticker_to_cik() if _dod_ready() else {}
        inv = {v: k for k, v in m.items()}
        return inv.get(str(cik).zfill(10))
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# LD-1 LOBBYING REGISTRATIONS
# --------------------------------------------------------------------------- #

_LDA_API = "https://lda.senate.gov/api/v1/filings/"
_DEFENSIVE_ISSUE = re.compile(
    r"investigat|enforcement|subpoena|antitrust|monopol|litigation|settlement|"
    r"regulat|compliance|penalt|sanction|inquiry|oversight",
    re.IGNORECASE,
)


def fetch_ld1_registrations(days_back=30, max_pages=5):
    """New lobbying REGISTRATIONS -- filed when a relationship starts, before any money is spent.

    That is the forward-looking part. By the time a quarterly LD-2 spending report lands, the
    lobbying has already happened. An LD-1 says it is about to.
    """
    signals_create_tables()
    conn = get_db()
    if not conn:
        return {"error": "no database"}
    summary = {"fetched": 0, "stored": 0, "resolved": 0}
    try:
        since = (_sec_date.today() - timedelta(days=days_back)).isoformat()
        for page in range(1, max_pages + 1):
            try:
                r = requests.get(_LDA_API, params={
                    "filing_type": "RR",          # Registration
                    "filing_dt_posted_after": since,
                    "page": page, "page_size": 100,
                }, headers={"User-Agent": SEC_UA_STR}, timeout=25)
                if r.status_code != 200:
                    break
                data = r.json()
                rows = data.get("results") or []
                if not rows:
                    break
                for f in rows:
                    summary["fetched"] += 1
                    client = ((f.get("client") or {}).get("name") or "").strip()
                    firm = ((f.get("registrant") or {}).get("name") or "").strip()
                    issues, bills = [], []
                    for a in (f.get("lobbying_activities") or []):
                        gi = a.get("general_issue_code_display") or a.get("general_issue_code")
                        if gi:
                            issues.append(str(gi))
                        d = a.get("description") or ""
                        bills += re.findall(r"\b(?:H\.?R\.?|S\.?)\s?\d{1,5}\b", d)
                    issue_str = ", ".join(sorted(set(issues)))[:400]
                    bill_str = ", ".join(sorted(set(bills)))[:200]
                    defensive = bool(_DEFENSIVE_ISSUE.search(issue_str + " " +
                                     " ".join((a.get("description") or "") for a in (f.get("lobbying_activities") or []))))

                    ticker = None
                    if _dod_ready():
                        try:
                            ent = _DOD.resolve_entity(conn, client)
                            if ent["ticker"] and not ent["needs_review"]:
                                ticker = ent["ticker"]
                                summary["resolved"] += 1
                        except Exception:
                            ticker = None

                    cur = conn.cursor()
                    cur.execute(
                        "INSERT INTO lobbying_registrations (filing_uuid, ticker, client_name,"
                        " lobbying_firm, issues, bill_numbers, form_type, is_defensive, filing_date, raw_json) "
                        "VALUES (%s,%s,%s,%s,%s,%s,'LD-1',%s,%s,%s) ON CONFLICT (filing_uuid) DO NOTHING",
                        (f.get("filing_uuid"), ticker, client[:200], firm[:200], issue_str,
                         bill_str, defensive, (f.get("dt_posted") or "")[:10] or None,
                         _SecJson({"filing_type": f.get("filing_type")})))
                    conn.commit()
                    cur.close()
                    summary["stored"] += 1
                time.sleep(0.3)
            except Exception as e:
                logger.warning("fetch_ld1 page %s: %s" % (page, e))
                break
        return summary
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# CRON ROUTES
# --------------------------------------------------------------------------- #

@app.route("/cron/ld1-registrations")
def cron_ld1_registrations():
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    try:
        return jsonify(fetch_ld1_registrations())
    except Exception as e:
        logger.error("cron_ld1: %s" % e)
        return jsonify({"error": str(e)}), 500


@app.route("/cron/clinical-trials")
def cron_clinical_trials():
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    if not _CT_READY:
        return jsonify({"error": "clinicaltrials_pipeline.py not deployed"}), 503
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        resolver = None
        if _dod_ready():
            resolver = lambda name: _DOD.resolve_entity(conn, name)
        return jsonify(_CT.fetch_clinical_trial_changes(conn, resolve_ticker_fn=resolver))
    except Exception as e:
        logger.error("cron_clinical_trials: %s" % e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/activist/<symbol>")
def api_activist(symbol):
    return jsonify({"symbol": symbol.upper(), "filings": activist_for(symbol)})


@app.route("/api/comment-letters/<symbol>")
def api_comment_letters(symbol):
    return jsonify({"symbol": symbol.upper(), "letters": comment_letters_for(symbol)})


@app.route("/api/clinical-trials/<symbol>")
def api_clinical_trials(symbol):
    return jsonify({"symbol": symbol.upper(), "changes": clinical_changes_for(symbol)})



# --------------------------------------------------------------------------- #
# CONVERGENCE EVENT LOG
#
# detect_convergence() already finds the overlaps and applies a bounded bonus. What was missing is
# the part that makes it ANSWERABLE: writing the event down on the day it fires, before the outcome
# exists, so that in 90 days the labeled data can say whether convergence predicts anything at all.
#
# Without this table, the bullish override the spec asked for could never be earned -- there would
# be no evidence to earn it with, only the feeling that it ought to work.
# --------------------------------------------------------------------------- #

def convergence_create_tables():
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS convergence_events ("
            "id SERIAL PRIMARY KEY,"
            "symbol TEXT NOT NULL,"
            "event_date DATE NOT NULL DEFAULT CURRENT_DATE,"
            "pattern TEXT,"
            "signals JSONB,"
            "direction TEXT,"
            "bonus_applied INTEGER,"
            "cap_applied TEXT,"
            "alpha_at_event INTEGER,"
            "verdict_at_event TEXT,"
            "implication TEXT,"
            "created_at TIMESTAMP DEFAULT NOW(),"
            "CONSTRAINT uq_conv UNIQUE (symbol, event_date, pattern))"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS ix_conv_sym ON convergence_events(symbol, event_date DESC)")
        conn.commit()
        cur.close()
        logger.info("convergence event log: table ready")
    except Exception as e:
        logger.error("convergence_create_tables: %s" % e)
    finally:
        conn.close()


def log_convergence(symbol, conv, alpha_score, verdict):
    """Write the event the day it fires. Never raises; a logging failure must not break a report."""
    if not conv or not conv.get("converged"):
        return
    conn = get_db()
    if not conn:
        return
    try:
        sigs = conv.get("signals") or []
        pattern = " + ".join([str(x)[:60] for x in sigs][:4]) or "unspecified"
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO convergence_events (symbol, pattern, signals, direction, bonus_applied,"
            " cap_applied, alpha_at_event, verdict_at_event, implication) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (symbol, event_date, pattern) DO NOTHING",
            (str(symbol).upper(), pattern[:300], _SecJson(sigs),
             ("bearish" if conv.get("cap") else "bullish"),
             int(conv.get("bonus") or 0), conv.get("cap"),
             (int(alpha_score) if alpha_score is not None else None),
             verdict, (conv.get("implication") or "")[:1000]))
        conn.commit()
        cur.close()
    except Exception as e:
        logger.warning("log_convergence %s: %s" % (symbol, e))
    finally:
        conn.close()


@app.route("/api/convergence")
def api_convergence():
    """Every convergence event, joined to what actually happened next.

    This endpoint is the referee. When `labeled` passes ~30 and mean_excess_ret_20d is
    convincingly positive, the bullish override the spec asked for has EARNED the right to exist.
    Until then it does not, and the honest answer says so.
    """
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT c.symbol, c.event_date, c.pattern, c.direction, c.bonus_applied,"
            " c.alpha_at_event, c.verdict_at_event, f.fwd_ret_20d, f.excess_ret_20d "
            "FROM convergence_events c "
            "LEFT JOIN feature_snapshots f ON f.symbol=c.symbol AND f.snap_date=c.event_date "
            "ORDER BY c.event_date DESC LIMIT 200")
        rows = [{"symbol": r[0], "date": str(r[1]), "pattern": r[2], "direction": r[3],
                 "bonus": r[4], "alpha_at_event": r[5], "verdict_at_event": r[6],
                 "fwd_ret_20d": (float(r[7]) if r[7] is not None else None),
                 "excess_ret_20d": (float(r[8]) if r[8] is not None else None)}
                for r in (cur.fetchall() or [])]
        cur.close()

        bull = [x for x in rows if x["direction"] == "bullish" and x["excess_ret_20d"] is not None]
        avg = (sum(x["excess_ret_20d"] for x in bull) / len(bull)) if bull else None

        if len(bull) < 30:
            verdict_txt = ("Only %d labeled bullish convergence events. Far too few to conclude "
                           "anything. Convergence is currently a HYPOTHESIS being measured -- it adds "
                           "a small bonus and a flag, and it does NOT override any verdict. It has to "
                           "earn that." % len(bull))
        else:
            verdict_txt = ("Across %d labeled bullish convergence events, mean excess return vs SPY "
                           "over the following 20 days is %.2f%%. THIS is the number that decides "
                           "whether the verdict override is justified. If it is near zero, the "
                           "pattern was a coincidence and should be dropped." % (len(bull), avg))

        return jsonify({
            "events": rows, "total": len(rows), "labeled_bullish": len(bull),
            "mean_excess_ret_20d": (round(avg, 2) if avg is not None else None),
            "override_enabled": False,
            "verdict": verdict_txt,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# =========================================================================== #
# EARNINGS INTELLIGENCE — app integration
#
# The engine lives in earnings_pipeline.py. Imported defensively: if that file
# is missing, the earnings routes return 503 and nothing else in the app breaks.
#
# ARCHITECTURE NOTE THAT MATTERS FOR COST:
#
#   The RELEASE is ingested from the SEC 8-K (Item 2.02, press release attached
#   as EX-99.1). That lands on EDGAR within MINUTES of the release crossing --
#   often before the wires carry it. It is free, authoritative, public domain.
#
#   The TRANSCRIPT is enrichment, not the foundation. Real-time streaming
#   transcript feeds cost thousands a month. This module is fully functional
#   with zero paid data; a transcript deepens the analysis when one exists and
#   its absence never blocks anything.
#
#   Set LIVE_TRANSCRIPT_PROVIDER later, if and when the budget justifies it.
# =========================================================================== #

_EARN = None
_EARN_READY = False
try:
    import earnings_pipeline as _EARN
    _EARN_READY = True
except Exception as _ee2:
    logger.warning("earnings_pipeline not available: %s" % _ee2)


def _earn_llm(prompt):
    """LLM hook for tone/theme analysis. Uses whichever model is already configured."""
    try:
        key = os.environ.get("GEMINI_KEY", "").strip()
        if not key:
            return None
        r = requests.post(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=" + key,
            json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=45)
        if r.status_code != 200:
            return None
        return (((r.json().get("candidates") or [{}])[0].get("content") or {})
                .get("parts") or [{}])[0].get("text")
    except Exception as e:
        logger.warning("_earn_llm: %s" % e)
        return None


def _earn_cik(symbol):
    if not _dod_ready():
        return None
    try:
        return (_DOD.sec_ticker_to_cik() or {}).get(str(symbol).upper())
    except Exception:
        return None


def run_earnings_cycle(conn, symbols=None, max_symbols=60):
    """Advance every tracked event through the state machine.

    Each transition is guarded by can_transition(), and each stage writes ONE immutable feature
    row. A stage is never rewritten -- which is what makes a later backtest honest rather than
    merely well-intentioned.
    """
    started = time.time()
    out = {"scheduled": 0, "briefed": 0, "released": 0, "analyzed": 0, "errors": 0}
    if not _EARN_READY:
        return {"error": "earnings_pipeline not deployed"}

    _EARN.earnings_create_tables(conn) if hasattr(_EARN, "earnings_create_tables") else None

    if not symbols:
        cur = conn.cursor()
        cur.execute("SELECT symbol FROM snapshot_universe WHERE active "
                    "ORDER BY hits DESC LIMIT %s", (max_symbols,))
        symbols = [r[0] for r in (cur.fetchall() or [])]
        cur.close()

    fmp = os.environ.get("FMP_KEY", "").strip()

    for sym in symbols:
        try:
            # 1. CALENDAR -> SCHEDULED
            cal = _EARN.fetch_earnings_calendar(sym, fmp_key=fmp)
            if cal:
                out["scheduled"] += 1

            # 2. PRE_BRIEF at T-5d. Frozen the moment it is written.
            #    Everything after this is forbidden from touching it.
            hist = _EARN.past_quarters(conn, sym, n=8)
            ev = _EARN.earnings_for(conn, sym)
            if ev and ev.get("state") == "SCHEDULED":
                # BUGFIX: this read ev["days_until"] and ev["id"], neither of which
                # earnings_for() returns -- it returns countdown_seconds and event_id. The result
                # was that `days` was always None, the branch never fired, and the pre-earnings
                # brief was NEVER BUILT for any stock. Silent: no error, no log, just nothing.
                secs = ev.get("countdown_seconds")
                days = (secs / 86400.0) if secs is not None else None
                if days is not None and 0 <= days <= 5 and ev.get("event_id"):
                    brief = _EARN.build_pre_brief(ev, hist)
                    _EARN.log_features(conn, ev["event_id"], sym, "PRE", brief)
                    out["briefed"] += 1

            # 3. RELEASE. The 8-K is the real-time source, and it is free.
            if ev and ev.get("state") in ("PRE_BRIEF", "AWAITING_RELEASE"):
                cik = _earn_cik(sym)
                if cik and _dod_ready():
                    rel = _EARN.fetch_8k_release(sym, cik, _DOD._sec_get)
                    if rel:
                        _EARN.log_features(conn, ev["id"], sym, "RELEASE", rel)
                        out["released"] += 1

            # 4. TRANSCRIPT + ANALYSIS. Optional. Missing transcript is a normal state.
            ev = _EARN.earnings_for(conn, sym)
            if ev and ev.get("state") == "RELEASE_PARSED":
                try:
                    prov = _EARN.get_transcript_provider()
                    txt = prov.fetch(sym) if prov else None
                    if txt:
                        nlp = _EARN.analyze_transcript(txt, _earn_llm)
                        _EARN.log_features(conn, ev["id"], sym, "CALL", nlp or {})
                except Exception as te:
                    logger.info("no transcript for %s: %s" % (sym, te))

                sc = _EARN.score_event(ev)
                _EARN.log_features(conn, ev["id"], sym, "POST", sc)
                out["analyzed"] += 1

            time.sleep(0.25)
        except Exception as e:
            out["errors"] += 1
            logger.warning("earnings cycle %s: %s" % (sym, e))
            try:
                conn.rollback()
            except Exception:
                pass

    out["elapsed_sec"] = round(time.time() - started, 1)
    logger.info("earnings cycle: %s" % out)
    return out


@app.route("/cron/earnings")
def cron_earnings():
    """Advance the state machine. Run HOURLY during earnings season, and at :05 past the hour --
    8-Ks cluster right after the close and right before the open."""
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    if not _EARN_READY:
        return jsonify({"error": "earnings_pipeline.py not deployed"}), 503
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        return jsonify(run_earnings_cycle(conn))
    except Exception as e:
        logger.error("cron_earnings: %s" % e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/cron/earnings-label")
def cron_earnings_label():
    """Measure the stock reaction. T+2. Reads ONLY prices dated after the release."""
    if request.args.get("token") != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "unauthorized"}), 403
    if not _EARN_READY:
        return jsonify({"error": "earnings_pipeline.py not deployed"}), 503
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        return jsonify(_EARN.label_earnings_reactions(
            conn,
            closes_fn=lambda s: _fs_closes(s),
            bench_fn=lambda: _fs_closes(_FS_BENCH),
        ))
    except Exception as e:
        logger.error("cron_earnings_label: %s" % e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/earnings/<symbol>")
def api_earnings(symbol):
    """Everything the stock page needs: countdown, brief, release, analysis, reaction."""
    if not _EARN_READY:
        return jsonify({"error": "earnings_pipeline.py not deployed"}), 503
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        ev = _EARN.earnings_for(conn, symbol)

        # The pre-earnings brief is READ BACK from the feature store, never recomputed. It was
        # written before the release and must show what was believed THEN -- if we rebuilt it now,
        # with the result already known, the "bull case" would be contaminated by hindsight and the
        # whole point of writing it early would be lost.
        brief = None
        if ev and ev.get("event_id"):
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT features, as_of FROM earnings_features "
                    "WHERE event_id=%s AND stage='PRE' ORDER BY as_of ASC LIMIT 1",
                    (ev["event_id"],))
                r = cur.fetchone()
                cur.close()
                if r:
                    brief = r[0] if isinstance(r[0], dict) else json.loads(r[0] or "{}")
                    if isinstance(brief, dict):
                        brief["as_of"] = str(r[1])
            except Exception as be:
                logger.warning("pre-brief read %s: %s" % (symbol, be))

        return jsonify({
            "symbol": symbol.upper(),
            "event": ev,
            "pre_brief": brief,
            "history": _EARN.past_quarters(conn, symbol, n=8),
            "transcript_available": bool(os.environ.get("FMP_KEY")),
            # Honest about the limit: FMP transcripts land AFTER the call, not during it. A true
            # live feed is a paid enterprise product Apex Q does not have.
            "live_transcript": bool(os.environ.get("LIVE_TRANSCRIPT_PROVIDER")),
        })
    except Exception as e:
        logger.error("api_earnings %s: %s" % (symbol, e))
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


def earnings_for_report(symbol):
    """Compact earnings block for the main report payload. None when there's nothing to show."""
    if not _EARN_READY or not symbol:
        return None
    conn = get_db()
    if not conn:
        return None
    try:
        return _EARN.earnings_for(conn, symbol)
    except Exception as e:
        logger.warning("earnings_for_report %s: %s" % (symbol, e))
        return None
    finally:
        conn.close()


# =========================================================================== #
# DATA MATURITY
#
# Tells the user where a number sits in the interpretation pipeline:
#
#   PRELIMINARY  numbers parsed from the 8-K. Transcript pending. Scores may move.
#   UPDATED      transcript analyzed. Scores stable but may refine.
#   FINAL        48h+ elapsed, market reaction recorded. Historical record.
#
# THREE DELIBERATE NARROWINGS FROM THE ORIGINAL SPEC:
#
# 1. Maturity is attached to EARNINGS and to the overall score -- NOT to every
#    card. A 13D filing does not mature. A congressional trade does not mature.
#    A DoD award does not mature. Badging all ten cards turns badges into
#    wallpaper, and within a week nobody sees any of them.
#
# 2. The confidence downgrade is a LEVEL STEP, not "15%". `confidence` is the
#    string High/Medium/Low; there is nothing to multiply. econ_downgrade()
#    already steps it correctly, so we reuse it.
#
# 3. FINAL is defined MECHANICALLY -- 48 hours elapsed plus a recorded market
#    reaction. The original spec said "analyst notes reviewed" and "flag for
#    human review". There is no human in this loop and there is not going to be
#    one; a state that depends on a review that never happens is a state nothing
#    ever reaches.
#
# What IS kept from the human-review idea: if the transcript's EPS contradicts
# the 8-K's EPS, one of the two parsers is wrong. That is a DATA QUALITY ALARM,
# and it is worth having.
#
# The transition log is the real prize, and not mainly for users: if a stock's
# score swings 30 points between PRELIMINARY and FINAL, the engine is
# over-reacting to thin data. That is a measurable defect, and right now there
# is no way to see it.
# =========================================================================== #

MATURITY_STATES = ("PRELIMINARY", "UPDATED", "FINAL")


def maturity_create_tables():
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS maturity_transitions ("
            "id SERIAL PRIMARY KEY,"
            "symbol TEXT NOT NULL,"
            "event_id INTEGER,"
            "state TEXT NOT NULL,"
            "alpha_at_state INTEGER,"
            "verdict_at_state TEXT,"
            "confidence_at_state TEXT,"
            "entered_at TIMESTAMP DEFAULT NOW(),"
            "CONSTRAINT uq_mat UNIQUE (symbol, event_id, state))"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS ix_mat_sym ON maturity_transitions(symbol, entered_at DESC)")
        conn.commit()
        cur.close()
        logger.info("data maturity: table ready")
    except Exception as e:
        logger.error("maturity_create_tables: %s" % e)
    finally:
        conn.close()


def _hours_since(ts):
    if not ts:
        return None
    try:
        s = str(ts).replace("Z", "").replace("T", " ")[:19]
        return (datetime.utcnow() - datetime.strptime(s, "%Y-%m-%d %H:%M:%S")).total_seconds() / 3600.0
    except Exception:
        return None


def compute_data_maturity(earn):
    """Where the data sits. Returns None when there is no recent earnings event --
    and returning None is the point: no event, no badge, no wallpaper."""
    if not earn or not isinstance(earn, dict):
        return None

    rel_h = _hours_since(earn.get("release_at"))
    has_tx = bool(earn.get("has_transcript"))

    # No release in the last 10 days -> this stock is simply not in an earnings window.
    if rel_h is None or rel_h > 240:
        return None

    if rel_h >= 48 and has_tx:
        state = "FINAL"
        label = "Post-event interpretation logged. Market reaction recorded."
        nxt = None
    elif has_tx:
        state = "UPDATED"
        label = "Transcript analyzed. Key metrics extracted."
        nxt = "Final in about %d hours, once 48 hours have passed and the reaction is recorded." % max(1, int(48 - rel_h))
    else:
        state = "PRELIMINARY"
        label = "Numbers parsed from the 8-K. Transcript pending."
        nxt = "Updates when the call transcript is published, usually within a day."

    # DATA QUALITY ALARM. The transcript and the 8-K should agree on EPS. If they do not,
    # one of the two parsers is wrong, and the score resting on them is not trustworthy.
    contradiction = None
    try:
        a, b = earn.get("eps_actual"), earn.get("eps_actual_transcript")
        if a is not None and b is not None and abs(float(a)) > 0.01:
            if abs(float(a) - float(b)) / abs(float(a)) > 0.05:
                contradiction = ("The transcript and the 8-K report different EPS figures "
                                 "(%.2f vs %.2f). One of the two was parsed wrong, so treat "
                                 "this quarter's numbers with caution until it is resolved."
                                 % (float(a), float(b)))
    except Exception:
        contradiction = None

    return {
        "state": state,
        "label": label,
        "since": earn.get("release_at"),
        "hours_since_release": round(rel_h, 1) if rel_h is not None else None,
        "next_expected": nxt,
        "is_preliminary": state == "PRELIMINARY",
        "contradiction": contradiction,
        "period": earn.get("period"),
        "event_id": earn.get("event_id"),
    }


def log_maturity_transition(symbol, mat, alpha, verdict, confidence):
    """Record the score at each state, once. This is what lets us later ask: does the engine
    over-react to thin data? A 30-point swing from PRELIMINARY to FINAL is a defect, not a nuance."""
    if not mat or not symbol:
        return
    conn = get_db()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO maturity_transitions "
            "(symbol, event_id, state, alpha_at_state, verdict_at_state, confidence_at_state) "
            "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (symbol, event_id, state) DO NOTHING",
            (str(symbol).upper(), mat.get("event_id"), mat["state"],
             (int(alpha) if alpha is not None else None), verdict, confidence))
        conn.commit()
        cur.close()
    except Exception as e:
        logger.warning("log_maturity_transition %s: %s" % (symbol, e))
    finally:
        conn.close()


def apply_maturity(symbol, earn, alpha, verdict, confidence, flags):
    """Attach maturity, step confidence down on PRELIMINARY, log the transition.

    Returns (maturity_dict_or_None, confidence, flags).
    """
    try:
        mat = compute_data_maturity(earn)
        if not mat:
            return None, confidence, flags

        flags = list(flags or [])

        if mat["is_preliminary"]:
            # A LEVEL STEP, not a percentage -- confidence is a string, not a number.
            confidence = econ_downgrade(confidence)
            flags.append({"level": "warn", "text":
                "These numbers came straight from the 8-K filed in the last day or so. The call "
                "transcript has not been analyzed yet, so the score can still move meaningfully. "
                "Confidence has been stepped down until it has."})

        if mat.get("contradiction"):
            flags.append({"level": "warn", "text": mat["contradiction"]})

        log_maturity_transition(symbol, mat, alpha, verdict, confidence)
        return mat, confidence, flags
    except Exception as e:
        logger.warning("apply_maturity %s: %s" % (symbol, e))
        return None, confidence, flags


@app.route("/api/maturity/<symbol>")
def api_maturity(symbol):
    """How the score evolved: Preliminary -> Updated -> Final.

    The drift column is the one that matters. Large drift means the engine is drawing strong
    conclusions from thin data, and that is a bug in the engine, not a feature of the market.
    """
    conn = get_db()
    if not conn:
        return jsonify({"error": "no database"}), 503
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT event_id, state, alpha_at_state, verdict_at_state, confidence_at_state, entered_at "
            "FROM maturity_transitions WHERE symbol=%s ORDER BY entered_at DESC LIMIT 24",
            (symbol.upper(),))
        rows = [{"event_id": r[0], "state": r[1], "alpha": r[2], "verdict": r[3],
                 "confidence": r[4], "at": str(r[5])} for r in (cur.fetchall() or [])]
        cur.close()

        # Drift per event: how far the score moved from first sight to final.
        by_event, drift = {}, []
        for r in rows:
            by_event.setdefault(r["event_id"], {})[r["state"]] = r
        for eid, states in by_event.items():
            p = (states.get("PRELIMINARY") or {}).get("alpha")
            f = (states.get("FINAL") or states.get("UPDATED") or {}).get("alpha")
            if p is not None and f is not None:
                drift.append({"event_id": eid, "preliminary": p, "final": f, "drift": f - p})

        avg = (sum(abs(d["drift"]) for d in drift) / len(drift)) if drift else None
        return jsonify({
            "symbol": symbol.upper(), "transitions": rows, "drift": drift,
            "mean_abs_drift": round(avg, 1) if avg is not None else None,
            "note": (
                "Mean absolute drift of %.1f points between the preliminary read and the final one. "
                "Large drift means the engine draws strong conclusions from thin data -- that is a "
                "defect worth fixing, not a quirk to live with." % avg
                if avg is not None else
                "Not enough completed earnings events yet to measure how far the score drifts as the "
                "data matures."
            ),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Build the full schema at startup. gunicorn imports app:app, so this runs once
# on every boot. Each *_create_tables is idempotent and opens its own connection.
# Previously they only ran lazily inside cron endpoints, so a fresh or newly
# connected database errored "relation does not exist" on every query until some
# cron happened to create each table. Isolated per function so one failure can't
# block the rest; safe if DATABASE_URL is unset (get_db() returns None).
# --------------------------------------------------------------------------- #
def ensure_all_tables():
    for _fn in (snap_create_tables, fs_create_tables, signals_create_tables,
                convergence_create_tables, maturity_create_tables, trial_create_tables):
        try:
            _fn()
            logger.info("ensure_all_tables: %s ok" % _fn.__name__)
        except Exception as _e:
            logger.warning("ensure_all_tables: %s failed: %s" % (_fn.__name__, _e))
    try:
        _c = get_db()
        if _c:
            sec_create_tables(_c)
            _c.close()
            logger.info("ensure_all_tables: sec_create_tables ok")
    except Exception as _e:
        logger.warning("ensure_all_tables: sec_create_tables failed: %s" % _e)


try:
    ensure_all_tables()
except Exception as _e:
    logger.warning("ensure_all_tables top-level failure: %s" % _e)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
