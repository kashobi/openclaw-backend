from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
import os

app = Flask(__name__)
CORS(app)

FINNHUB_KEY = os.environ.get("FINNHUB_KEY", "")
FMP_KEY = os.environ.get("FMP_KEY", "")

def resolve_ticker(query):
    query = query.strip()
    try:
        search = yf.Search(query, max_results=1)
        quotes = search.quotes
        if quotes and len(quotes) > 0:
            return quotes[0].get("symbol", query.upper())
    except:
        pass
    return query.upper()

@app.route("/")
def home():
    try:
        filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'index.html')
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        return content, 200, {'Content-Type': 'text/html; charset=utf-8'}
    except Exception as e:
        return f"Error loading page: {str(e)}", 500

@app.route("/search")
def search_ticker():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"error": "No query provided"}), 400
    try:
        search = yf.Search(query, max_results=5)
        quotes = search.quotes
        results = [
            {
                "symbol": q.get("symbol"),
                "name": q.get("longname") or q.get("shortname"),
                "exchange": q.get("exchange")
            }
            for q in quotes if q.get("symbol")
        ]
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/analyze")
def analyze():
    query = request.args.get("symbol", "").strip()
    if not query:
        return jsonify({"error": "No symbol provided"}), 400

    symbol = resolve_ticker(query)

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="5d")
        info = ticker.info

        if hist.empty:
            return jsonify({"error": f"No data found for {symbol}."}), 404

        current = round(hist["Close"].iloc[-1], 2)
        prev = round(hist["Close"].iloc[-2], 2) if len(hist) > 1 else current
        change_pct = round(((current - prev) / prev) * 100, 2)
        rec = info.get("recommendationKey", "hold").upper()

        if rec in ["BUY", "STRONG_BUY"] and change_pct > 0:
            verdict = "APPROVE"
        elif rec in ["SELL", "STRONG_SELL"] or change_pct < -3:
            verdict = "PASS"
        else:
            verdict = "WATCH"

        return jsonify({
            "symbol": symbol,
            "query": query,
            "price": current,
            "change_pct": change_pct,
            "recommendation": rec,
            "verdict": verdict,
            "name": info.get("longName", symbol),
            "sector": info.get("sector", "N/A"),
            "pe_ratio": info.get("trailingPE", "N/A"),
            "market_cap": info.get("marketCap", "N/A"),
            "analyst_target": info.get("targetMeanPrice", "N/A")
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
