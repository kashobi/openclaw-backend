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
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

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


# CHUNK: send the bare domain to www, a backup in case the Porkbun forward misses
@app.before_request
def force_www():
    host = (request.host or "").split(":")[0].lower()
    if host == "apexq.io":
        return redirect(request.url.replace("://apexq.io", "://www.apexq.io", 1), code=301)


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
        return {"id": uid, "username": uname}
    return None

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
    return Response(html, mimetype="text/html")


def _serve_file(filename, mimetype, binary=False):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    if binary:
        return Response(open(path, "rb").read(), mimetype=mimetype)
    return Response(open(path, encoding="utf-8").read(), mimetype=mimetype)


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
    return jsonify({"user": current_user()})


@app.route("/auth/signup", methods=["POST"])
def auth_signup():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if len(username) < 3 or len(username) > 30:
        return jsonify({"error": "Username must be 3 to 30 characters."}), 400
    if not all(c.isalnum() or c in "_." for c in username):
        return jsonify({"error": "Username can use letters, numbers, underscore, and period only."}), 400
    if len(password) < 8:
        return jsonify({"error": "Password must be at least 8 characters."}), 400
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Accounts are not available right now."}), 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE LOWER(username) = LOWER(%s)", (username,))
        if cur.fetchone():
            cur.close()
            return jsonify({"error": "That username is taken."}), 409
        pw_hash = generate_password_hash(password)
        cur.execute("INSERT INTO users (username, password_hash) VALUES (%s, %s) RETURNING id", (username, pw_hash))
        uid = cur.fetchone()[0]
        conn.commit()
        cur.close()
        session.permanent = True
        session["user_id"] = uid
        session["username"] = username
        return jsonify({"ok": True, "user": {"id": uid, "username": username}})
    except Exception as e:
        logger.error("signup error: %s" % e)
        return jsonify({"error": "Could not create account. Try again."}), 500
    finally:
        conn.close()


@app.route("/auth/login", methods=["POST"])
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
        cur.execute("SELECT id, username, password_hash FROM users WHERE LOWER(username) = LOWER(%s)", (username,))
        row = cur.fetchone()
        cur.close()
        if not row or not check_password_hash(row[2], password):
            return jsonify({"error": "Wrong username or password."}), 401
        session.permanent = True
        session["user_id"] = row[0]
        session["username"] = row[1]
        return jsonify({"ok": True, "user": {"id": row[0], "username": row[1]}})
    except Exception as e:
        logger.error("login error: %s" % e)
        return jsonify({"error": "Could not log in. Try again."}), 500
    finally:
        conn.close()


@app.route("/auth/logout", methods=["POST"])
def auth_logout():
    session.clear()
    return jsonify({"ok": True})


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
@app.route("/portfolio", methods=["GET"])
def portfolio_get():
    u = current_user()
    if not u:
        return jsonify({"error": "not_logged_in"}), 401
    uid = u["id"]
    # 60 second cache so rapid refreshes do not re-run the whole aggregation. The underlying
    # prices come from light_score, which carries its own cache, so this never hammers Yahoo.
    ckey = "portfolio_" + str(uid)
    entry = CACHE.get(ckey)
    if entry and (time.time() - entry[1]) < 60:
        return jsonify(entry[0])
    conn = get_db()
    if conn is None:
        return jsonify({"error": "Database not available."}), 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT symbol, shares, avg_cost FROM holdings WHERE user_id = %s ORDER BY added_at ASC", (uid,))
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        logger.error("portfolio get error: %s" % e)
        return jsonify({"error": "Could not load your portfolio."}), 500
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
    return jsonify(payload)


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
        if count >= 5 and not exists:
            cur.close()
            return jsonify({"error": "free_limit", "message": "Free accounts can track up to 5 holdings. Upgrade to premium for unlimited."}), 402
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
        return jsonify({"results": [{"symbol": x.get("symbol"), "name": x.get("longname") or x.get("shortname")} for x in s.quotes if x.get("symbol")]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
    return jsonify(result)


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


def compute_full_report(symbol):
    cached = get_cache(f"full_{symbol}")
    if cached:
        return cached

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="5d", timeout=15)
        info = ticker.info

        if hist.empty:
            return None

        cur = fmt_price(hist["Close"].iloc[-1])
        prev = fmt_price(hist["Close"].iloc[-2]) if len(hist) > 1 else cur
        chg = round(((cur - prev) / prev) * 100, 2)
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
        sharp_drop = chg <= -8

        if chg > 2:
            score += 2
        elif chg > 0:
            score += 1
        elif chg <= -10:
            score -= 5
        elif chg <= -5:
            score -= 3
        elif chg <= -3:
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
        if tgt and cur and not sharp_drop:
            try:
                up = ((float(tgt) - cur) / cur) * 100
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

        # CHUNK: build the pre/post market move and a plain-English note that keeps the verdict honest
        ext = extended_hours(info, cur)
        # CHUNK: targets set before a just released earnings report are stale, so flag the upside as provisional.
        if earn == "recent" and tgt and tgt != "N/A" and cur:
            try:
                if ((float(tgt) - cur) / cur) * 100 > 0:
                    flags.append({"level": "warn", "text": "These analyst targets were likely set before the recent earnings report, so the upside shown may be stale until analysts revise it. Treat it as provisional."})
            except Exception:
                pass
        ext_note = ""
        if ext:
            direction = "up" if ext["change_pct"] >= 0 else "down"
            ext_note = "%s is %s %s percent in %s trading, at about $%s." % (
                symbol, direction, abs(ext["change_pct"]), ext["session"], ext["price"])
            if earn == "recent":
                ext_note += " This is right after an earnings report. The verdict below is based on the regular session close, so it does not yet reflect this move or how the stock trades at the next open. Big moves right after earnings often settle down, so treat this as fresh news to read, not a new verdict. See the news below."
            else:
                ext_note += " The verdict below is based on the regular session close, not this move."
        elif earn == "recent":
            ext_note = "%s reported earnings within about the last day. The verdict below is based on the most recent regular session close, so check the news below for the latest." % symbol
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

        result = {
            "symbol": symbol,
            "name": info.get("longName", symbol),
            "sector": info.get("sector", ""),
            "price": cur,
            "change_pct": chg,
            "recommendation": rec,
            "verdict": verdict,
            "alert": alert,
            "conviction": conviction,
            "score": score,
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
        cur = fmt_price(hist["Close"].iloc[-1])
        prev = fmt_price(hist["Close"].iloc[-2]) if len(hist) > 1 else cur
        chg = round(((cur - prev) / prev) * 100, 2)
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
            }
            set_cache("disc_" + symbol, res)
            return res
        score = 0
        if chg > 2:
            score += 2
        elif chg > 0:
            score += 1
        elif chg < -3:
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
                # CHUNK: an unusually large upside often means a stale or outlier target, so it
                # carries reduced weight here too, matching the full report.
                if upside > 10:
                    score += 1 if upside >= 40 else 2
                elif upside > 0:
                    score += 1
                elif upside < -5:
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


@app.route("/scan")
def scan():
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
    else:
        lens = "strong"
        cand = [r for r in rows if num(r.get("change_pct")) and r["change_pct"] > 0]
        cand.sort(key=lambda r: r["change_pct"], reverse=True)
        for r in cand[:12]:
            out.append(dict(r, reason="Up %s%% today, among the strongest movers in the group." % r["change_pct"]))

    return jsonify({"lens": lens, "items": out})


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
    msg += "This weighs the same signals you see in each full report. Educational only, never advice."
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
                      "Open each one for its expense ratio, top holdings, and sector mix. Educational only, never advice.")
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


@app.route("/movers")
def movers():
    # The biggest gainers and decliners across the whole market, pulled live and refreshed every
    # half hour. Surfaces names well beyond the usual large caps, which fits the Discover idea.
    now = time.time()
    if _MOVERS["data"] is not None and now - _MOVERS["ts"] < 1800:
        return jsonify(_MOVERS["data"])

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
            gainers = [mv(r) for r in up if r["change_pct"] > 0][:8]
            losers = [mv(r) for r in down if r["change_pct"] < 0][:8]

    out = {"gainers": gainers, "losers": losers, "data_timestamp": int(time.time())}
    # Only cache a real result, so a transient FMP miss does not stick for half an hour.
    if gainers or losers:
        _MOVERS["data"] = out
        _MOVERS["ts"] = now
    return jsonify(out)


@app.route("/alerts")
def alerts():
    # Reads the logged in user's saved stocks, scores each one, and surfaces only the names
    # that warrant a look right now. This is the in app feed. A push to the phone is the next layer.
    u = current_user()
    if not u:
        return jsonify({"status": "logged_out", "alerts": []})
    conn = get_db()
    if conn is None:
        return jsonify({"status": "error", "alerts": []})
    try:
        cur = conn.cursor()
        cur.execute("SELECT symbol, name FROM watchlist WHERE user_id = %s ORDER BY added_at DESC", (u["id"],))
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        logger.error("alerts list error: %s" % e)
        return jsonify({"status": "error", "alerts": []})
    finally:
        conn.close()

    if not rows:
        return jsonify({"status": "empty", "alerts": []})

    out = []
    for sym, nm in rows:
        r = light_score(sym)
        if not r:
            continue
        chg = r.get("change_pct")
        v = r.get("verdict")
        name = r.get("name") or nm or sym
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
    out.sort(key=lambda a: 0 if a["kind"] == "caution" else 1)
    return jsonify({"status": "ok", "alerts": out, "total_saved": len(rows)})


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
                "Do not give financial advice. End every message with: This is educational, not advice. Return plain text only, no markdown."
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
                "Do not give financial advice. End by reminding the reader this is educational, not advice. Return plain text only, no markdown."
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
                "Do not give financial advice. End every message with: This is educational, not advice. Return plain text only, no markdown."
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
                "Do not give financial advice. End by reminding the reader this is educational, not advice. Return plain text only, no markdown."
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
            return "Here are the most recent headlines for " + symbol + ". " + heads + ". Read the full articles in the News Feed section of the report. Educational only, never advice."
    # CHUNK: ETFs are funds, not stocks, so answer on cost and holdings rather than a stock verdict.
    if v == "ETF":
        ans = symbol + " is an exchange traded fund, a single ticker that holds a basket of many investments. "
        er = d.get("expense_ratio")
        cat = d.get("category")
        if er not in (None, "N/A"):
            ans += "Its expense ratio, the yearly cost to own it, is about " + str(er) + " percent. "
        if cat not in (None, "N/A"):
            ans += "Its category is " + str(cat) + ". "
        ans += "Open the full report for its top holdings and sector mix. A fund is judged on what it costs and what it holds, not a stock style verdict. Educational only, never advice."
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
            return "Here is what the engine can see. " + reasons + ". That said, the real reason for a daily move is usually news, an earnings report, an analyst call, or a broader market swing, which the numbers alone do not capture. Check the News Feed section in the full report for the real story. Educational only, never advice."
        return "Here is what the engine can see. The numbers on this one do not explain today's move, which usually means it is being driven by news, earnings, or a broader market swing rather than the signals. Check the News Feed section in the full report for the real story. Educational only, never advice."
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
        parts.append("Educational only, never advice.")
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
    parts.append("None of this is a recommendation. For a real decision with real money, your own homework and a licensed professional are the right next step. Educational only, never advice.")
    return " ".join(parts)


def coach_gemini(q, entities):
    facts = []
    for tkr, label, is_sec in entities[:4]:
        r = light_score(tkr)
        if r:
            facts.append("%s (%s): verdict %s, %s percent today, analyst upside %s percent, PE %s" % (label, tkr, r.get("verdict"), r.get("change_pct"), r.get("upside"), r.get("pe_ratio")))
    if not facts:
        return None
    prompt = (
        "You are the educational explanation layer of a stock app for everyday people and beginners. "
        "The user asked, possibly by voice: \"" + q + "\". "
        "Here are the engine's current live facts: " + "; ".join(facts) + ". "
        "STRICT RULES: You are not a financial advisor. Do not tell the user where to invest, do not recommend a specific stock to buy, and do not suggest how to split any amount of money. "
        "Instead, explain in simple plain language how each option looks based on the facts, what the differences mean, and how a beginner should think the decision through themselves, including risk, time horizon, and not concentrating money in one name. "
        "Make clear the dollar amount does not change what the signals say. "
        "Keep it to about 5 to 8 short sentences, no jargon. Do not use any dashes or hyphens, use plain words. End by clearly stating this is educational only, not advice, and that they should do their own research and consider a licensed professional. Return plain text only, no markdown."
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
        r = light_score(tkr)
        if r:
            facts.append("%s (%s): verdict %s, %s percent today, analyst upside %s percent, PE %s" % (label, tkr, r.get("verdict"), r.get("change_pct"), r.get("upside"), r.get("pe_ratio")))
    if not facts:
        return None
    prompt = (
        "You are the educational explanation layer of a stock app for everyday people and beginners. "
        "The user asked, possibly by voice: \"" + q + "\". "
        "Here are the engine's current live facts: " + "; ".join(facts) + ". "
        "STRICT RULES: You are not a financial advisor. Do not tell the user where to invest, do not recommend a specific stock to buy, and do not suggest how to split any amount of money. "
        "Instead, explain in simple plain language how each option looks based on the facts, what the differences mean, and how a beginner should think the decision through themselves, including risk, time horizon, and not concentrating money in one name. "
        "Make clear the dollar amount does not change what the signals say. "
        "Keep it to about 5 to 8 short sentences, no jargon. Do not use any dashes or hyphens, use plain words. End by clearly stating this is educational only, not advice, and that they should do their own research and consider a licensed professional. Return plain text only, no markdown."
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
