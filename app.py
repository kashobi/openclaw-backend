from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import yfinance as yf
import requests
import os
import time
import json
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

FINNHUB_KEY = os.environ.get("FINNHUB_KEY", "")
QUIVER_KEY = os.environ.get("QUIVER_KEY", "")
GEMINI_KEY = os.environ.get("GEMINI_KEY", "")

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


@app.route("/analyze")
def analyze():
    query = request.args.get("symbol", "").strip()
    if not query:
        return jsonify({"error": "No symbol provided"}), 400

    symbol = resolve_ticker(query)
    logger.info(f"ANALYZE: {query} -> {symbol}")

    cached = get_cache(f"full_{symbol}")
    if cached:
        return jsonify(cached)

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="5d", timeout=15)
        info = ticker.info

        if hist.empty:
            return jsonify({"error": f"No data found for {symbol}."}), 404

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

        if tgt and cur:
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
        if cong_buys >= 2:
            score += 2
        elif cong_buys == 1:
            score += 1

        # Insider from Quiver
        insider = []
        CLEVEL = ["CEO", "CFO", "COO", "PRESIDENT", "CHAIRMAN", "CTO", "DIRECTOR"]
        if QUIVER_KEY:
            try:
                url = f"https://api.quiverquant.com/beta/historical/insiders/{symbol}"
                h = {"Authorization": f"Token {QUIVER_KEY}", "Accept": "application/json"}
                r = requests.get(url, headers=h, timeout=8)
                if r.status_code == 200:
                    for t in r.json()[:10]:
                        title = str(t.get("Title", "")).upper()
                        insider.append({"name": t.get("Name", "Unknown"), "title": t.get("Title", ""), "action": t.get("AcquiredDisposed", ""), "shares": t.get("Shares", 0), "price": fmt_price(t.get("Price", 0)), "date": t.get("Date", ""), "is_clevel": any(c in title for c in CLEVEL)})
            except Exception as e:
                logger.error(f"Insider error: {e}")

        ins_buys = len([t for t in insider if t.get("is_clevel") and t.get("action") == "A"])
        if ins_buys >= 2:
            score += 3
        elif ins_buys == 1:
            score += 2

        conviction = score_to_conviction(score)

        if score >= 4:
            verdict = "APPROVE"
        elif score <= -2:
            verdict = "PASS"
        else:
            verdict = "WATCH"

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
                            news.append({"headline": n["headline"], "source": n.get("source", "News"), "summary": n.get("summary", "")[:150]})
                if not news:
                    url2 = f"https://finnhub.io/api/v1/news?category=general&token={FINNHUB_KEY}"
                    r2 = requests.get(url2, timeout=8)
                    if r2.status_code == 200:
                        for n in r2.json()[:4]:
                            if n.get("headline"):
                                news.append({"headline": n["headline"], "source": n.get("source", "Market News") + " (General)", "summary": n.get("summary", "")[:150]})
            except Exception as e:
                logger.error(f"News error: {e}")

        result = {
            "symbol": symbol,
            "name": info.get("longName", symbol),
            "sector": info.get("sector", ""),
            "price": cur,
            "change_pct": chg,
            "recommendation": rec,
            "verdict": verdict,
            "conviction": conviction,
            "score": score,
            "pe_ratio": pe,
            "analyst_target": tgt,
            "market_cap": info.get("marketCap", "N/A"),
            "volume": int(hist["Volume"].iloc[-1]),
            "beta": fmt_price(info.get("beta")),
            "news": news,
            "congressional": congressional,
            "insider": insider,
        }

        set_cache(f"full_{symbol}", result)
        return jsonify(result)

    except Exception as e:
        logger.error(f"Analyze error for {symbol}: {e}")
        return jsonify({"error": str(e)}), 500


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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
