from flask import Flask, jsonify, request, Response, session, redirect
from flask_cors import CORS
import yfinance as yf
import requests
import os
import time
import json
import re
import html
import logging
import csv
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

# ============ TWILIO SMS (phone verification) ============
# We call Twilio's REST API with requests, which is already a dependency, so no new package is
# added. If the keys are not set, codes are logged to the server instead of texted, which keeps
# the whole verification flow working in development.
TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE_NUMBER = os.environ.get("TWILIO_PHONE_NUMBER", "")


def _send_sms(to_number, body):
    """Send an SMS via Twilio. Returns True on success. With no Twilio config, logs and returns True."""
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_PHONE_NUMBER):
        logger.info("SMS (Twilio not configured) to %s: %s" % (to_number, body))
        return True
    try:
        url = "https://api.twilio.com/2010-04-01/Accounts/%s/Messages.json" % TWILIO_ACCOUNT_SID
        resp = requests.post(
            url,
            data={"From": TWILIO_PHONE_NUMBER, "To": to_number, "Body": body},
            auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
            timeout=10,
        )
        if resp.status_code >= 400:
            logger.error("Twilio send failed %s: %s" % (resp.status_code, resp.text[:200]))
            return False
        return True
    except Exception as e:
        logger.error("Twilio send error: %s" % e)
        return False
# ============ END TWILIO SMS ============

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
            "CREATE TABLE IF NOT EXISTS phone_verifications ("
            "id SERIAL PRIMARY KEY,"
            "phone TEXT,"
            "code TEXT,"
            "expires_at TIMESTAMP,"
            "verified BOOLEAN DEFAULT false)"
        )
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
    except Exception as e:
        logger.error("ensure_db error: %s" % e)
    finally:
        conn.close()


def get_secret_key():
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


def resolve_ticker(query):
    query = query.strip()
    try:
        s = yf.Search(query, max_results=1)
        if s.quotes:
            return s.quotes[0].get("symbol", query.upper())
    except:
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


@app.route("/")
def home():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    html = open(path, encoding="utf-8").read()
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
                cur.execute("SELECT COALESCE(tier, 'free'), phone, COALESCE(phone_verified, false), email, first_name FROM users WHERE id = %s", (u["id"],))
                row = cur.fetchone()
                cur.close()
                if row:
                    session["tier"] = row[0]
                    u["tier"] = row[0]
                    u["phone"] = row[1]
                    u["phone_verified"] = bool(row[2])
                    u["email"] = row[3]
                    u["first_name"] = row[4]
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


@app.route("/auth/send-verification", methods=["POST"])
def send_verification():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    data = request.get_json(silent=True) or {}
    phone = (data.get("phone") or "").strip()
    cleaned = phone.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if not re.match(r"^\+?[0-9]{7,15}$", cleaned):
        return jsonify({"error": "Enter a valid phone number with country code, like +12035551234."}), 400
    code = "%06d" % _secrets.randbelow(1000000)
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Verification is not available right now."}), 500
    try:
        cur = conn.cursor()
        expires = datetime.now() + timedelta(minutes=10)
        cur.execute("INSERT INTO phone_verifications (phone, code, expires_at, verified) VALUES (%s, %s, %s, false)",
                    (cleaned, code, expires))
        cur.execute("UPDATE users SET phone = %s WHERE id = %s", (cleaned, u["id"]))
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error("send_verification db error: %s" % e)
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify({"error": "Could not start verification. Try again."}), 500
    finally:
        conn.close()
    _send_sms(cleaned, "Your Apex Q verification code is %s. It expires in 10 minutes." % code)
    return jsonify({"ok": True})


@app.route("/auth/verify-phone", methods=["POST"])
def verify_phone():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    data = request.get_json(silent=True) or {}
    code = (data.get("code") or "").strip()
    if not code:
        return jsonify({"error": "Enter the code we sent you."}), 400
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Verification is not available right now."}), 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT phone FROM users WHERE id = %s", (u["id"],))
        prow = cur.fetchone()
        phone = prow[0] if prow else None
        if not phone:
            cur.close()
            return jsonify({"error": "Add a phone number first."}), 400
        cur.execute(
            "SELECT id FROM phone_verifications WHERE phone = %s AND code = %s AND verified = false "
            "AND expires_at > NOW() ORDER BY id DESC LIMIT 1",
            (phone, code),
        )
        match = cur.fetchone()
        if not match:
            cur.close()
            return jsonify({"error": "That code is wrong or has expired."}), 400
        cur.execute("UPDATE phone_verifications SET verified = true WHERE id = %s", (match[0],))
        cur.execute("UPDATE users SET phone_verified = true WHERE id = %s", (u["id"],))
        conn.commit()
        cur.close()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("verify_phone error: %s" % e)
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify({"error": "Could not verify. Try again."}), 500
    finally:
        conn.close()


# SECURITY: lets a logged in user download everything stored about them in one JSON file.
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


@app.route("/analyze")
def analyze():
    query = request.args.get("symbol", "").strip()
    if not query:
        return jsonify({"error": "No symbol provided"}), 400
    symbol = resolve_ticker(query)
    logger.info(f"ANALYZE: {query} -> {symbol}")
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


def compute_alpha_score(eff_chg, pe, debt_to_equity, ins_buys, ins_sells, cong_buys, cong_sells, news, sector):
    """Return {'score': int 0..100, 'breakdown': [str, ...]}. Designed to never raise."""
    breakdown = []

    # a. Technical Momentum (0 to 20)
    ec = _alpha_num(eff_chg)
    if ec is None:
        ec = 0.0
    if ec > 2:
        tech_raw = 5
    elif ec > 0:
        tech_raw = 3
    elif ec > -2:
        tech_raw = 1
    else:
        tech_raw = 0
    tech = tech_raw * 4
    if ec > 0:
        move = "up %s%% today" % ec
    elif ec < 0:
        move = "down %s%% today" % abs(ec)
    else:
        move = "flat today"
    breakdown.append("Technical Momentum: +%d pts (%s)" % (tech, move))

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


def compute_full_report(symbol):
    symbol = _china_symbol(symbol)
    cached = get_cache(f"full_{symbol}")
    if cached:
        return cached

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="5d", timeout=15)
        info = ticker.info

        if hist.empty:
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
        if QUIVER_KEY:
            try:
                url = f"https://api.quiverquant.com/beta/historical/congresstrading/{symbol}"
                h = {"Authorization": f"Token {QUIVER_KEY}", "Accept": "application/json"}
                r = requests.get(url, headers=h, timeout=8)
                if r.status_code == 200:
                    for t in r.json()[:8]:
                        congressional.append({"politician": t.get("Representative", "Unknown"), "party": t.get("Party", ""), "action": t.get("Transaction", "Unknown"), "amount": t.get("Range", ""), "date": t.get("TransactionDate", "")})
            except Exception as e:
                logger.error(f"Congressional error: {e}")

        cong_buys = len([t for t in congressional if "purchase" in str(t.get("action", "")).lower()])
        cong_sells = len([t for t in congressional if "sale" in str(t.get("action", "")).lower()])
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
            it = ticker.insider_transactions
            if it is not None and not it.empty:
                def pick(row, *names):
                    for n in names:
                        if n in row and row.get(n) is not None:
                            return row.get(n)
                    return None
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
        confidence, flags = run_referee(cur, chg, pe, tgt, rec, market_cap, volume, beta, hist, news, congressional, insider)

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
            if isinstance(prof_margin, (int, float)):
                mscore += 2 if prof_margin > 0.15 else (1 if prof_margin > 0.10 else 0)
            if isinstance(roe, (int, float)):
                mscore += 2 if roe > 0.20 else (1 if roe > 0.15 else 0)
            if isinstance(earn_growth, (int, float)):
                mscore += 2 if earn_growth > 0.10 else (1 if earn_growth > 0.05 else 0)
            if isinstance(rev_growth, (int, float)):
                mscore += 2 if rev_growth > 0.10 else (1 if rev_growth > 0.05 else 0)
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
            _alpha = compute_alpha_score(eff_chg, pe, debt_to_equity, ins_buys, ins_sells, cong_buys, cong_sells, news, sector_name)
            alpha_score = _alpha["score"]
            alpha_breakdown = _alpha["breakdown"]
        except Exception as e:
            logger.error("alpha score %s: %s" % (symbol, e))
            alpha_score = 0
            alpha_breakdown = []

        result = {
            "symbol": symbol,
            "name": info.get("longName", symbol),
            "sector": info.get("sector", ""),
            "company_summary": info.get("longBusinessSummary") or "",
            "price": cur,
            "change_pct": chg,
            "price_source": price_source,
            "recommendation": rec,
            "verdict": verdict,
            "alert": alert,
            "verdict_signal_reason": verdict_reason,
            "conviction": conviction,
            "score": score,
            "alpha_score": alpha_score,
            "alpha_breakdown": alpha_breakdown,
            "pe_ratio": pe,
            "analyst_target": tgt,
            "market_cap": market_cap,
            "volume": volume,
            "beta": beta,
            "confidence": confidence,
            "flags": flags,
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
            "suggested_questions": suggested,
            "data_timestamp": int(time.time()),
        }

        set_cache(f"full_{symbol}", result)
        return result

    except Exception as e:
        logger.error(f"Analyze error for {symbol}: {e}")
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
        # CHUNK: multi-turn chat. With history we send a real system message carrying the live facts,
        # then the prior turns, then the new question. The facts are rebuilt and sent every turn so
        # the model stays grounded and cannot drift into invented facts, even on adversarial questions.
        if history:
            system_content = (
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
            return jsonify({"answer": a, "verdict": d.get("verdict"), "symbol": sym})
    if GEMINI_KEY:
        a = ask_gemini(sym, q, d, ins, extra_news=extra_news, extra_insider=ins, history=history)
        if a:
            return jsonify({"answer": a, "verdict": d.get("verdict"), "symbol": sym})
    return jsonify({"answer": ask_fallback(sym, q, d, ins, extra_news=extra_news, extra_insider=ins), "verdict": d.get("verdict"), "symbol": sym})


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


@app.route("/discover")
def discover():
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
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period, interval=interval)
        if df.empty:
            return jsonify({"error": "No data for %s" % symbol}), 404
        data = []
        for idx, row in df.iterrows():
            t = int(idx.timestamp())
            data.append({
                "time": t,
                "open": round(float(row["Open"]), 2),
                "high": round(float(row["High"]), 2),
                "low": round(float(row["Low"]), 2),
                "close": round(float(row["Close"]), 2),
                "volume": int(row["Volume"]),
            })
        payload = {"symbol": symbol, "data": data, "period": period, "interval": interval}
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
    picks = ranked[:count]
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
        return None, None, "not_connected"
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
    if err:
        return jsonify({"error": err}), 502
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
        r = requests.post(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-tts:generateContent?key=" + key,
            json={
                "contents": [{"parts": [{"text": text}]}],
                "generationConfig": {
                    "responseModalities": ["AUDIO"],
                    "speechConfig": {"voiceConfig": {"prebuiltVoiceConfig": {"voiceName": "Kore"}}},
                },
            },
            timeout=60,
        )
        if r.status_code != 200:
            logger.error("tts gemini %s: %s" % (r.status_code, r.text[:200]))
            return jsonify({"error": "tts_unavailable"}), 503
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
