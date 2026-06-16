from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import yfinance as yf
import os

app = Flask(__name__)
CORS(app)

FINNHUB_KEY = os.environ.get("FINNHUB_KEY", "")
FMP_KEY = os.environ.get("FMP_KEY", "")

@app.route("/")
def home():
    return send_from_directory('.', 'index.html')

@app.route("/analyze")
def analyze():
    symbol = request.args.get("symbol", "").upper().strip()
    if not symbol:
        return jsonify({"error": "No symbol provided"}), 400

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info

        # Get price from info first, fall back to fast_info
        current = info.get("currentPrice") or info.get("regularMarketPrice")

        if not current:
            fast = ticker.fast_info
            current = getattr(fast, "last_price", None)

        if not current:
            hist = ticker.history(period="5d")
            if not hist.empty:
                current = float(hist["Close"].iloc[-1])

        if not current:
            return jsonify({"error": "Could not fetch price for " + symbol}), 400

        current = round(float(current), 2)

        # Get previous close for change calculation
        prev = info.get("previousClose") or info.get("regularMarketPreviousClose")

        if not prev:
            hist = ticker.history(period="5d")
            if len(hist) >= 2:
                prev = float(hist["Close"].iloc[-2])

        if prev:
            prev = round(float(prev), 2)
            change_pct = round(((current - prev) / prev) * 100, 2)
        else:
            change_pct = 0.0

        rec = (info.get("recommendationKey") or "hold").upper()

        if rec in ["BUY", "STRONG_BUY"] and change_pct > 0:
            verdict = "APPROVE"
        elif rec in ["SELL", "STRONG_SELL"] or change_pct < -3:
            verdict = "PASS"
        else:
            verdict = "WATCH"

        pe = info.get("trailingPE")
        target = info.get("targetMeanPrice")
        market_cap = info.get("marketCap")

        return jsonify({
            "symbol":        symbol,
            "name":          info.get("longName", symbol),
            "price":         current,
            "change_pct":    change_pct,
            "recommendation": rec,
            "verdict":       verdict,
            "pe_ratio":      round(float(pe), 2) if pe else "N/A",
            "analyst_target": round(float(target), 2) if target else "N/A",
            "market_cap":    market_cap or "N/A"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
