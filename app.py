from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import yfinance as yf
import requests
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

def build_signal_reasons(data):
    reasons = []
    score = 0
    total = 0

    change = data.get("change_pct", 0) or 0
    total += 25
    if change > 2:
        reasons.append({"label": "Price Momentum", "value": "Strong", "detail": f"Up {change}% today. Positive buying pressure.", "positive": True})
        score += 25
    elif change > 0:
        reasons.append({"label": "Price Momentum", "value": "Moderate", "detail": f"Up {change}% today. Mild positive movement.", "positive": True})
        score += 15
    elif change < -3:
        reasons.append({"label": "Price Momentum", "value": "Weak", "detail": f"Down {change}% today. Selling pressure present.", "positive": False})
    else:
        reasons.append({"label": "Price Momentum", "value": "Neutral", "detail": f"Change of {change}% today. No strong direction.", "positive": None})
        score += 10

    rec = data.get("recommendation", "hold") or "hold"
    total += 25
    if rec in ["BUY", "STRONG_BUY"]:
        reasons.append({"label": "Analyst Consensus", "value": "Bullish", "detail": f"Wall Street analysts rate this a {rec.replace('_', ' ').title()}.", "positive": True})
        score += 25
    elif rec in ["SELL", "STRONG_SELL"]:
        reasons.append({"label": "Analyst Consensus", "value": "Bearish", "detail": f"Wall Street analysts rate this a {rec.replace('_', ' ').title()}.", "positive": False})
    else:
        reasons.append({"label": "Analyst Consensus", "value": "Neutral", "detail": "Analysts currently rate this a Hold. No strong directional call.", "positive": None})
        score += 10

    target = data.get("analyst_target")
    current = data.get("price", 0) or 0
    total += 25
    if target and current:
        try:
            upside = round(((float(target) - float(current)) / float(current)) * 100, 1)
            if upside > 10:
                reasons.append({"label": "Price Target Upside", "value": f"+{upside}%", "detail": f"Analyst mean target is ${target}. That is {upside}% above today's price.", "positive": True})
                score += 25
            elif upside > 0:
                reasons.append({"label": "Price Target Upside", "value": f"+{upside}%", "detail": f"Analyst mean target is ${target}. Modest upside of {upside}%.", "positive": True})
                score += 15
            elif upside < -5:
                reasons.append({"label": "Price Target Upside", "value": f"{upside}%", "detail": f"Analyst mean target is ${target}. Stock may be overvalued.", "positive": False})
            else:
                reasons.append({"label": "Price Target Upside", "value": "At Target", "detail": f"Stock is trading near analyst mean target of ${target}.", "positive": None})
                score += 10
        except:
            reasons.append({"label": "Price Target Upside", "value": "N/A", "detail": "Price target data not available.", "positive": None})
            score += 10
    else:
        reasons.append({"label": "Price Target Upside", "value": "N/A", "detail": "No analyst price target available for this security.", "positive": None})
        score += 10

    pe = data.get("pe_ratio")
    total += 25
    if pe and pe != "N/A":
        try:
            pe_val = float(pe)
            if pe_val < 15:
                reasons.append({"label": "Valuation", "value": "Attractive", "detail": f"PE ratio of {round(pe_val,1)} is below market average. Potentially undervalued.", "positive": True})
                score += 25
            elif pe_val < 30:
                reasons.append({"label": "Valuation", "value": "Fair", "detail": f"PE ratio of {round(pe_val,1)} is within normal range.", "positive": True})
                score += 15
            elif pe_val < 50:
                reasons.append({"label": "Valuation", "value": "Elevated", "detail": f"PE ratio of {round(pe_val,1)} is above average. Growth expectations priced in.", "positive": None})
                score += 8
            else:
                reasons.append({"label": "Valuation", "value": "High", "detail": f"PE ratio of {round(pe_val,1)} is very high. Market expects significant future growth.", "positive": False})
        except:
            reasons.append({"label": "Valuation", "value": "N/A", "detail": "PE ratio not available.", "positive": None})
            score += 10
    else:
        reasons.append({"label": "Valuation", "value": "N/A", "detail": "Valuation data not available. Common for new IPOs and crypto assets.", "positive": None})
        score += 10

    confidence = round((score / total) * 100) if total > 0 else 50
    return reasons, confidence

def get_quote(symbol):
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="2d")
        if hist.empty:
            return None
        current = round(hist["Close"].iloc[-1], 2)
        prev = round(hist["Close"].iloc[-2], 2) if len(hist) > 1 else current
        change_pct = round(((current - prev) / prev) * 100, 2)
        return {"symbol": symbol, "price": current, "change_pct": change_pct}
    except:
        return None

@app.route("/")
def home():
    return send_from_directory(os.path.dirname(os.path.abspath(__file__)), 'index.html')

@app.route("/search")
def search_ticker():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"error": "No query provided"}), 400
    try:
        search = yf.Search(query, max_results=5)
        quotes = search.quotes
        results = [
            {"symbol": q.get("symbol"), "name": q.get("longname") or q.get("shortname"), "exchange": q.get("exchange")}
            for q in quotes if q.get("symbol")
        ]
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/market")
def market():
    data = {}

    # Major Indexes
    indexes = {
        "sp500": "^GSPC", "dow": "^DJI", "nasdaq": "^IXIC",
        "russell": "^RUT", "vix": "^VIX"
    }
    for key, sym in indexes.items():
        q = get_quote(sym)
        if q:
            data[key] = q

    # Commodities
    commodities = {"gold": "GC=F", "oil": "CL=F", "silver": "SI=F", "natgas": "NG=F"}
    for key, sym in commodities.items():
        q = get_quote(sym)
        if q:
            data[key] = q

    # Crypto
    crypto = {"bitcoin": "BTC-USD", "ethereum": "ETH-USD", "solana": "SOL-USD"}
    for key, sym in crypto.items():
        q = get_quote(sym)
        if q:
            data[key] = q

    # Options Fear and Greed proxy
    data["options_note"] = "Options flow data coming soon"

    # Prediction markets note
    data["prediction_note"] = "Prediction market integration coming soon"

    # Bonds
    bonds = {"us10y": "^TNX", "us2y": "^IRX"}
    for key, sym in bonds.items():
        q = get_quote(sym)
        if q:
            data[key] = q

    # Major ETFs
    etfs = {"spy": "SPY", "qqq": "QQQ", "dia": "DIA", "iwm": "IWM", "schd": "SCHD"}
    for key, sym in etfs.items():
        q = get_quote(sym)
        if q:
            data[key] = q

    return jsonify(data)

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
        target = info.get("targetMeanPrice")

        data = {
            "symbol": symbol,
            "query": query,
            "price": current,
            "change_pct": change_pct,
            "recommendation": rec,
            "name": info.get("longName", symbol),
            "sector": info.get("sector", "N/A"),
            "pe_ratio": info.get("trailingPE", "N/A"),
            "market_cap": info.get("marketCap", "N/A"),
            "analyst_target": target,
            "volume": int(hist["Volume"].iloc[-1]) if not hist.empty else 0,
            "week_52_high": info.get("fiftyTwoWeekHigh", "N/A"),
            "week_52_low": info.get("fiftyTwoWeekLow", "N/A"),
            "dividend_yield": info.get("dividendYield", "N/A"),
            "beta": info.get("beta", "N/A")
        }

        reasons, confidence = build_signal_reasons(data)

        if confidence >= 65:
            verdict = "APPROVE"
        elif confidence <= 35:
            verdict = "PASS"
        else:
            verdict = "WATCH"

        data["verdict"] = verdict
        data["confidence"] = confidence
        data["reasons"] = reasons

        return jsonify(data)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
