app.py
from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
import requests
import os

app = Flask(__name__)
CORS(app)

FINNHUB_KEY = os.environ.get("FINNHUB_KEY", "")
FMP_KEY = os.environ.get("FMP_KEY", "")

@app.route("/")
def home():
    return jsonify({"status": "OpenClaw Intelligence Terminal is live"})

@app.route("/analyze")
def analyze():
    symbol = request.args.get("symbol", "").upper().strip()
    if not symbol:
        return jsonify({"error": "No symbol provided"}), 400
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="5d")
        info = ticker.info
        current = round(hist["Close"].iloc[-1], 2)
        prev = round(hist["Close"].iloc[-2], 2)
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
            "price": current,
            "change_pct": change_pct,
            "recommendation": rec,
            "verdict": verdict,
            "name": info.get("longName", symbol),
            "pe_ratio": info.get("trailingPE", "N/A"),
            "market_cap": info.get("marketCap", "N/A"),
            "analyst_target": info.get("targetMeanPrice", "N/A")
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
