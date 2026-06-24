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
CACHE_TTL = 60 * 60 * 4

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


def insider_selling_cap(ticker_obj, cur_price):
    # Returns True when a cluster of executives is selling, the same rule the full report uses
    # to refuse an APPROVE. Used by the sector list so it never contradicts the full report.
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
            if ratio > 3 or ratio < 0.34:
                warn("The analyst price target is very far from the current price, which usually means it is stale or has not been updated after recent news. Treat the upside it implies with caution.")
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


# CHUNK: shared full-report engine so Ask and the report use the same verdict
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
                if up > 10:
                    score += 2
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
        if ins_buys >= 2:
            score += 3
        elif ins_buys == 1:
            score += 2
        # Insider selling is a softer signal than buying, since executives sell for many
        # ordinary reasons. But a broad cluster of top officers selling at once is a real
        # caution, so it pulls the score down, scaled to how many are selling.
        if ins_sells >= 4:
            score -= 4
        elif ins_sells >= 2:
            score -= 2
        elif ins_sells == 1:
            score -= 1

        # Weigh the size of the selling, not just the count. Estimate the dollar value of each
        # sale using the current price, attach it to every row so it can be shown, and total it.
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

        # Conservative size add-on. Only genuinely large executive selling deepens the penalty,
        # so routine insider sales never trip it. Tuned to dollar value of executive sells.
        if exec_sell_value >= 50000000:
            score -= 2
        elif exec_sell_value >= 20000000:
            score -= 1

        # Very large single block detection (any insider, including big holders). A block this
        # size is market relevant, but big holders sometimes sell for portfolio reasons, so it
        # adds a mild caution and a clear note rather than dominating the verdict.
        mc_num = info.get("marketCap") if isinstance(info.get("marketCap"), (int, float)) else 0
        big_block = False
        if max_single_sell >= 100000000 or (mc_num > 0 and max_single_sell >= 0.01 * mc_num):
            big_block = True
            score -= 1

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

        # Insider selling cap. When a cluster of executives is selling, the verdict cannot sit
        # at APPROVE while the people who know the company best are heading for the exit.
        if heavy_insider_selling and verdict == "APPROVE":
            verdict = "WATCH"
            if alert is None:
                alert = "insider_selling"

        # News from Finnhub
        news = []
        if FINNHUB_KEY:
            try:
                today = datetime.now().strftime("%Y-%m-%d")
                from_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
                url = f"https://finnhub.io/api/v1/company-news?symbol={symbol}&from={from_date}&to={today}&token={FINNHUB_KEY}"
                r = requests.get(url, timeout=8)
                if r.status_code == 200:
                    for n in r.json()[:6]:
                        if n.get("headline"):
                            news.append({"headline": clean_text(n["headline"]), "source": clean_text(n.get("source", "News")), "summary": trim_words(clean_text(n.get("summary", "")))})
                if not news:
                    url2 = f"https://finnhub.io/api/v1/news?category=general&token={FINNHUB_KEY}"
                    r2 = requests.get(url2, timeout=8)
                    if r2.status_code == 200:
                        for n in r2.json()[:4]:
                            if n.get("headline"):
                                news.append({"headline": clean_text(n["headline"]), "source": clean_text(n.get("source", "Market News")) + " (General)", "summary": trim_words(clean_text(n.get("summary", "")))})
            except Exception as e:
                logger.error(f"News error: {e}")

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
                    if firm or new:
                        fmp["grades"].append({"firm": str(firm), "prev": str(prev), "new": str(new), "action": action, "date": gdate})
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
        earn = earnings_flag(info)
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

    try:
        prompt = (
            "You are the live intelligence layer for an educational stock app built for everyday people, "
            "including beginners who have never invested before. The user is looking at " + symbol + ". "
            "Using current market knowledge, return ONLY valid JSON, no markdown, no extra words, with these keys: "
            "current_context (2 to 3 plain sentences on what is happening with this company right now, including any recent earnings, "
            "government or regulatory news, and Wall Street developments), "
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
    "tickers": ["MNDY","PATH","GTLB","ESTC","CFLT","PEGA","FROG","DOCN","BILL"]
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
                if upside > 10:
                    score += 2
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
        # Same insider selling cap the full report uses, so the sector list can never show
        # APPROVE on a stock the full report would hold at WATCH.
        if verdict == "APPROVE" and insider_selling_cap(t, cur):
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
    msg += descr.get(v, "it scores highest of the group") + ". "
    extras = []
    if isinstance(best.get("upside"), (int, float)):
        extras.append("analysts see about %s%% upside to target" % best["upside"])
    if isinstance(best.get("change_pct"), (int, float)):
        extras.append("it is %s%% %s today" % (abs(best["change_pct"]), "up" if best["change_pct"] >= 0 else "down"))
    try:
        extras.append("it trades at about %s times earnings" % round(float(best.get("pe_ratio")), 1))
    except (TypeError, ValueError):
        pass
    if extras:
        msg += "Among the reasons: " + ", ".join(extras) + ". "
    msg += "This weighs the same signals you see in each full report. Educational only, never advice."
    return msg


@app.route("/compare")
def compare():
    raw = request.args.get("symbols", "")
    syms = [s.strip().upper() for s in raw.split(",") if s.strip()][:3]
    items = []
    for s in syms:
        r = light_score(s)
        if r:
            items.append(r)
    strongest = None
    reason = ""
    if items:
        best = max(items, key=lambda r: (r.get("score", 0), r.get("upside") or 0, r.get("change_pct") or 0))
        strongest = best["symbol"]
        reason = compare_reason(best, items)
    return jsonify({"items": items, "strongest": strongest, "reason": reason})


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

    def grab(path):
        data = fmp_get(path)
        out = []
        if isinstance(data, list):
            for d in data[:10]:
                sym = d.get("symbol")
                if not sym or len(sym) > 6:
                    continue
                out.append({
                    "symbol": sym,
                    "name": d.get("name") or sym,
                    "change_pct": pct(d.get("changesPercentage")),
                    "price": d.get("price"),
                })
        return out

    out = {"gainers": grab("/api/v3/stock_market/gainers"), "losers": grab("/api/v3/stock_market/losers"), "data_timestamp": int(time.time())}
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


def ask_gemini(symbol, q, d, ins):
    try:
        facts = ("Current verdict: %s. Conviction: %s. Price: %s. Change today: %s percent. PE ratio: %s. Analyst upside to average target: %s percent."
                 % (d.get("verdict"), d.get("conviction"), d.get("price"), d.get("change_pct"), d.get("pe_ratio"), d.get("upside")))
        if ins is not None:
            facts += " Insider picture: about %s recent C level sales." % ins.get("clevel_sells")
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
def ask_deepseek(symbol, q, d, ins):
    try:
        facts = ("Current verdict: %s. Conviction: %s. Price: %s. Change today: %s percent. PE ratio: %s. Analyst upside to average target: %s percent."
                 % (d.get("verdict"), d.get("conviction"), d.get("price"), d.get("change_pct"), d.get("pe_ratio"), d.get("upside")))
        if ins is not None:
            facts += " Insider picture: about %s recent C level sales." % ins.get("clevel_sells")
        prompt = (
            "You are the explanation layer for an educational stock app for everyday people and beginners. "
            "The user is looking at " + symbol + " (" + str(d.get("name", symbol)) + ") and asks: \"" + q + "\". "
            "Here are the engine's current facts for this stock: " + facts + " "
            "Answer in 2 to 4 short, plain sentences with no jargon, grounded only in these facts and basic investing ideas. "
            "Do not use any dashes or hyphens, use plain words. "
            "Do not give financial advice. End by reminding the reader this is educational, not advice. Return plain text only, no markdown."
        )
        headers = {"Authorization": "Bearer " + DEEPSEEK_KEY, "Content-Type": "application/json"}
        payload = {"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 400}
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


def ask_fallback(symbol, q, d, ins):
    ql = q.lower()
    v = d.get("verdict", "WATCH")
    chg = d.get("change_pct")
    pe = d.get("pe_ratio")
    up = d.get("upside")
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
    if ins is not None:
        if ins.get("selling"):
            n = ins.get("clevel_sells")
            parts.append("On insiders: company people have been selling, about %s C level sale%s recently, roughly %s in value. Executives sell for many reasons, so selling alone is a softer signal than buying, but a cluster is a caution." % (n, "" if n == 1 else "s", fmt_money_py(ins.get("sell_value"))))
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
    if not d:
        return jsonify({"answer": "I could not pull live data for " + sym + " right now. It may be an unusual ticker, or data is briefly unavailable. Try again, or check the symbol."})
    # CHUNK: defer to the authoritative full report verdict so Ask never contradicts the report
    full = compute_full_report(sym)
    if full and full.get("verdict"):
        d = dict(d)
        d["verdict"] = full.get("verdict")
        if full.get("conviction"):
            d["conviction"] = full.get("conviction")
    ins = None
    if any(w in ql for w in ["insider", "executive", "exec", "selling", "sold", "buying", "bought"]):
        ins = insider_brief(sym, d.get("price"))
    # CHUNK: DeepSeek primary, Gemini fallback, rules-based final safety net
    if DEEPSEEK_KEY:
        a = ask_deepseek(sym, q, d, ins)
        if a:
            return jsonify({"answer": a, "verdict": d.get("verdict")})
    if GEMINI_KEY:
        a = ask_gemini(sym, q, d, ins)
        if a:
            return jsonify({"answer": a, "verdict": d.get("verdict")})
    return jsonify({"answer": ask_fallback(sym, q, d, ins), "verdict": d.get("verdict")})


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
    data = fmp_get("/api/v3/stock_market/actives")
    if not isinstance(data, list) or not data:
        data = fmp_get("/api/v3/stock_market/gainers")
    if isinstance(data, list):
        for d in data[:12]:
            sym = d.get("symbol")
            if not sym or len(sym) > 6:
                continue
            items.append({
                "symbol": sym,
                "name": d.get("name") or sym,
                "change_pct": parse_pct(d.get("changesPercentage")),
                "price": d.get("price"),
            })

    out = {"items": items, "data_timestamp": int(time.time())}
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


# CHUNK: temporary debug endpoint — remove after testing
@app.route("/debug/ask")
def debug_ask():
    symbol = "NVDA"
    q = "Why did it drop today?"
    d = light_score(symbol)
    if not d:
        return jsonify({"error": "light_score returned None for " + symbol})
    ins = insider_brief(symbol, d.get("price"))
    result = {
        "symbol": symbol,
        "light_score_ok": True,
        "price": d.get("price"),
        "verdict": d.get("verdict"),
        "change_pct": d.get("change_pct"),
        "deepseek_key_set": bool(DEEPSEEK_KEY),
        "deepseek_key_preview": (DEEPSEEK_KEY[:8] + "...") if DEEPSEEK_KEY else "NOT SET",
        "gemini_key_set": bool(GEMINI_KEY),
        "gemini_key_preview": (GEMINI_KEY[:8] + "...") if GEMINI_KEY else "NOT SET",
        "insider_sells": ins.get("clevel_sells") if ins else 0,
    }
    if DEEPSEEK_KEY:
        try:
            a = ask_deepseek(symbol, q, d, ins)
            result["deepseek_result"] = (a[:200] if a else "None returned")
            result["deepseek_error"] = None
        except Exception as e:
            result["deepseek_result"] = "None returned"
            result["deepseek_error"] = str(e)[:200]
    if GEMINI_KEY:
        try:
            a = ask_gemini(symbol, q, d, ins)
            result["gemini_result"] = (a[:200] if a else "None returned")
            result["gemini_error"] = None
        except Exception as e:
            result["gemini_result"] = "None returned"
            result["gemini_error"] = str(e)[:200]
    fb = ask_fallback(symbol, q, d, ins)
    result["fallback_result"] = (fb[:200] if fb else "")
    return jsonify(result)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
