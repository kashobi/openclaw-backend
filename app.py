from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import yfinance as yf
import requests
import os
import time
import json

app = Flask(__name__)
CORS(app)

FINNHUB_KEY = os.environ.get("FINNHUB_KEY", "")
QUIVER_KEY = os.environ.get("QUIVER_KEY", "")

# Simple in-memory cache
CACHE = {}
CACHE_TTL = 60 * 60 * 4  # 4 hours

def get_cache(key):
    if key in CACHE:
        data, timestamp = CACHE[key]
        if time.time() - timestamp < CACHE_TTL:
            return data
    return None

def set_cache(key, data):
    CACHE[key] = (data, time.time())

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

def get_market_data(symbol):
    cached = get_cache(f"market_{symbol}")
    if cached:
        return cached
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="5d")
        info = ticker.info
        if hist.empty:
            return None
        current = round(hist["Close"].iloc[-1], 2)
        prev = round(hist["Close"].iloc[-2], 2) if len(hist) > 1 else current
        change_pct = round(((current - prev) / prev) * 100, 2)
        result = {
            "price": current,
            "change_pct": change_pct,
            "recommendation": info.get("recommendationKey", "hold").upper(),
            "name": info.get("longName", symbol),
            "sector": info.get("sector", "N/A"),
            "pe_ratio": round(info.get("trailingPE", 0), 2) if info.get("trailingPE") else "N/A",
            "market_cap": info.get("marketCap", "N/A"),
            "analyst_target": info.get("targetMeanPrice", "N/A"),
            "volume": int(hist["Volume"].iloc[-1]) if not hist.empty else "N/A",
            "52w_high": info.get("fiftyTwoWeekHigh", "N/A"),
            "52w_low": info.get("fiftyTwoWeekLow", "N/A"),
        }
        set_cache(f"market_{symbol}", result)
        return result
    except:
        return None

def get_finnhub_news(symbol):
    cached = get_cache(f"news_{symbol}")
    if cached:
        return cached
    if not FINNHUB_KEY:
        return []
    try:
        url = f"https://finnhub.io/api/v1/company-news?symbol={symbol}&from=2026-06-01&to=2026-06-16&token={FINNHUB_KEY}"
        resp = requests.get(url, timeout=5).json()
        news = [{"headline": n.get("headline"), "source": n.get("source"), "time": n.get("datetime")} for n in resp[:5] if n.get("headline")]
        set_cache(f"news_{symbol}", news)
        return news
    except:
        return []

def get_quiver_congressional(symbol):
    cached = get_cache(f"congress_{symbol}")
    if cached:
        return cached
    if not QUIVER_KEY:
        return []
    try:
        url = f"https://api.quiverquant.com/beta/historical/congresstrading/{symbol}"
        headers = {"Authorization": f"Token {QUIVER_KEY}", "Accept": "application/json"}
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code == 200:
            trades = resp.json()[:5]
            result = []
            for t in trades:
                action = t.get("Transaction", "Unknown")
                amount = t.get("Range", "Unknown")
                politician = t.get("Representative", "Unknown")
                party = t.get("Party", "")
                date = t.get("TransactionDate", "")
                result.append({
                    "politician": politician,
                    "party": party,
                    "action": action,
                    "amount": amount,
                    "date": date
                })
            set_cache(f"congress_{symbol}", result)
            return result
    except:
        pass
    return []

def get_quiver_insider(symbol):
    cached = get_cache(f"insider_{symbol}")
    if cached:
        return cached
    if not QUIVER_KEY:
        return []
    try:
        url = f"https://api.quiverquant.com/beta/historical/insiders/{symbol}"
        headers = {"Authorization": f"Token {QUIVER_KEY}", "Accept": "application/json"}
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code == 200:
            trades = resp.json()[:10]
            result = []
            for t in trades:
                result.append({
                    "name": t.get("Name", "Unknown"),
                    "title": t.get("Title", "Unknown"),
                    "action": t.get("AcquiredDisposed", "Unknown"),
                    "shares": t.get("Shares", 0),
                    "price": t.get("Price", 0),
                    "date": t.get("Date", "")
                })
            set_cache(f"insider_{symbol}", result)
            return result
    except:
        pass
    return []

def get_sec_filings(symbol):
    cached = get_cache(f"sec_{symbol}")
    if cached:
        return cached
    try:
        url = f"https://efts.sec.gov/LATEST/search-index?q=%22{symbol}%22&dateRange=custom&startdt=2026-01-01&forms=4,13F"
        resp = requests.get(url, timeout=5, headers={"User-Agent": "ApexQ apexq@apexq.ai"}).json()
        hits = resp.get("hits", {}).get("hits", [])
        result = {"form4_count": len([h for h in hits if "4" in str(h.get("_source", {}).get("form_type", ""))]), "total_filings": len(hits)}
        set_cache(f"sec_{symbol}", result)
        return result
    except:
        return {"form4_count": 0, "total_filings": 0}

def synthesize_verdict(market, congressional, insider, sec):
    score = 0
    reasons = []
    signals = []

    # Market data signals
    change_pct = market.get("change_pct", 0)
    rec = market.get("recommendation", "HOLD")
    target = market.get("analyst_target", 0)
    price = market.get("price", 0)
    pe = market.get("pe_ratio", "N/A")

    if change_pct > 2:
        score += 2
        signals.append("STRONG_MOMENTUM")
        reasons.append({"icon": "&#128200;", "label": f"Strong Price Momentum +{change_pct}%", "detail": "Buyers are in control. Strong upward price action today signals institutional demand."})
    elif change_pct > 0:
        score += 1
        signals.append("POSITIVE_MOMENTUM")
        reasons.append({"icon": "&#128202;", "label": f"Positive Price Action +{change_pct}%", "detail": "Modest positive momentum. Stock trending in the right direction."})
    elif change_pct < -3:
        score -= 2
        signals.append("SELLING_PRESSURE")
        reasons.append({"icon": "&#128201;", "label": f"Significant Decline {change_pct}%", "detail": "Sellers dominating. Elevated risk at current levels. Caution advised."})
    else:
        reasons.append({"icon": "&#10145;", "label": f"Neutral Price Action {change_pct}%", "detail": "No clear directional momentum. Market is undecided on this name."})

    if rec in ["BUY", "STRONG_BUY"]:
        score += 2
        signals.append("ANALYST_BUY")
        reasons.append({"icon": "&#9989;", "label": f"Analyst Consensus: {rec.replace('_', ' ')}", "detail": "Wall Street professionals rate this a Buy. Professional money sees upside ahead."})
    elif rec in ["SELL", "STRONG_SELL"]:
        score -= 2
        signals.append("ANALYST_SELL")
        reasons.append({"icon": "&#9940;", "label": f"Analyst Consensus: {rec.replace('_', ' ')}", "detail": "Professional consensus is negative. Analysts see downside risk."})
    else:
        reasons.append({"icon": "&#9888;", "label": "Analyst Consensus: Hold", "detail": "Neutral professional outlook. No strong conviction either way."})

    if target and price and float(str(target)) > 0:
        upside = round(((float(str(target)) - price) / price) * 100, 1)
        if upside > 10:
            score += 1
            reasons.append({"icon": "&#127919;", "label": f"{upside}% Upside to Analyst Target", "detail": f"Target ${target} vs current ${price}. Significant room to grow based on professional projections."})
        elif upside > 0:
            reasons.append({"icon": "&#127919;", "label": f"{upside}% Upside to Target", "detail": f"Target ${target} vs current ${price}. Modest upside from current levels."})
        else:
            score -= 1
            reasons.append({"icon": "&#127919;", "label": "Trading Above Analyst Target", "detail": f"${price} exceeds analyst target of ${target}. May be overvalued at current levels."})

    # Congressional trading signals
    if congressional:
        buys = [t for t in congressional if "Purchase" in str(t.get("action", "")) or "Buy" in str(t.get("action", ""))]
        sells = [t for t in congressional if "Sale" in str(t.get("action", "")) or "Sell" in str(t.get("action", ""))]
        if len(buys) > len(sells) and buys:
            score += 2
            signals.append("CONGRESS_BUYING")
            politicians = ", ".join([b.get("politician", "Unknown") for b in buys[:2]])
            reasons.append({"icon": "&#127963;", "label": f"Congressional Buying Detected", "detail": f"Politicians including {politicians} recently purchased this stock. Congress members have access to information most investors do not."})
        elif len(sells) > len(buys) and sells:
            score -= 1
            signals.append("CONGRESS_SELLING")
            reasons.append({"icon": "&#127963;", "label": "Congressional Selling Detected", "detail": f"{len(sells)} congressional members recently sold this stock. Worth monitoring for potential headwinds."})
        else:
            reasons.append({"icon": "&#127963;", "label": f"Congressional Activity: {len(congressional)} trades", "detail": "Congressional trading activity detected. Mixed signals from political insiders."})

    # Insider trading signals
    if insider:
        c_level_titles = ["CEO", "CFO", "COO", "President", "Chairman", "Director", "CTO", "CIO"]
        c_buys = [t for t in insider if any(title in str(t.get("title", "")).upper() for title in c_level_titles) and t.get("action") == "A"]
        c_sells = [t for t in insider if any(title in str(t.get("title", "")).upper() for title in c_level_titles) and t.get("action") == "D"]
        if len(c_buys) >= 2:
            score += 3
            signals.append("INSIDER_CLUSTER_BUY")
            reasons.append({"icon": "&#128188;", "label": f"C-Level Cluster Buy Detected", "detail": f"{len(c_buys)} executives are buying their own stock. When multiple insiders buy at once this is one of the strongest signals in the market."})
        elif len(c_buys) == 1:
            score += 1
            signals.append("INSIDER_BUY")
            name = c_buys[0].get("name", "Executive")
            title = c_buys[0].get("title", "")
            reasons.append({"icon": "&#128188;", "label": f"Insider Buying: {title}", "detail": f"{name} recently purchased shares. Insiders only buy for one reason: they believe the stock will go up."})
        if len(c_sells) >= 2:
            score -= 2
            signals.append("INSIDER_CLUSTER_SELL")
            reasons.append({"icon": "&#128188;", "label": "Insider Cluster Selling", "detail": f"{len(c_sells)} executives recently sold shares. Heavy insider selling is a caution flag worth monitoring."})

    # SEC filing signals
    if sec and sec.get("form4_count", 0) > 5:
        reasons.append({"icon": "&#128196;", "label": f"SEC Activity: {sec.get('form4_count')} Form 4 Filings", "detail": "High volume of insider transaction filings with the SEC. Significant activity by insiders in this stock."})

    # Confluence bonus
    if "CONGRESS_BUYING" in signals and "INSIDER_BUY" in signals:
        score += 2
        reasons.append({"icon": "&#9889;", "label": "CONFLUENCE SIGNAL DETECTED", "detail": "Both congressional members AND company insiders are buying simultaneously. This rare alignment is one of the highest conviction signals in Apex Q."})

    if score >= 4:
        verdict = "APPROVE"
        confidence = f"Multiple independent intelligence layers are aligned bullish on this stock. Score {score}/10. This is a high conviction opportunity based on price momentum, analyst consensus, and insider activity."
    elif score <= -2:
        verdict = "PASS"
        confidence = f"Multiple signals are pointing negative. Score {score}/10. The data suggests sitting this one out and waiting for a better setup before taking action."
    else:
        verdict = "WATCH"
        confidence = f"Mixed signals across intelligence layers. Score {score}/10. Not strong enough to approve and not weak enough to pass. Monitor for a clearer confluence point."

    return verdict, confidence, reasons, score, signals

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Apex Q Intelligence Terminal</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
  :root {
    --bg:#f0f4f8;--surface:#ffffff;--surface2:#e8edf2;--border:#c8d4e0;
    --accent:#0066cc;--green:#007a3d;--green-bg:#e6f4ed;--red:#cc0000;
    --red-bg:#fce8e8;--yellow:#b36b00;--yellow-bg:#fff3e0;
    --text:#0d1f2d;--muted:#5a7a9a;--card:#ffffff;--header:#0d1f2d;
  }
  *{margin:0;padding:0;box-sizing:border-box;}
  body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',sans-serif;min-height:100vh;}
  .ticker-bar{background:var(--header);overflow:hidden;white-space:nowrap;height:34px;display:flex;align-items:center;}
  .ticker-track{display:flex;animation:ticker 60s linear infinite;}
  .ticker-track:hover{animation-play-state:paused;}
  .ticker-item{display:inline-flex;align-items:center;gap:8px;padding:0 18px;height:34px;font-family:'JetBrains Mono',monospace;font-size:11px;border-right:1px solid #1e3550;cursor:pointer;transition:background 0.2s;}
  .ticker-item:hover{background:#1e3550;}
  .ticker-sym{color:#fff;font-weight:700;}
  .ticker-price{color:#a0b8cc;font-size:11px;}
  .ticker-change.up{color:#00cc66;font-weight:600;}
  .ticker-change.down{color:#ff6666;font-weight:600;}
  @keyframes ticker{0%{transform:translateX(0);}100%{transform:translateX(-50%);}}
  .header{padding:14px 24px;display:flex;align-items:center;justify-content:space-between;background:var(--surface);border-bottom:2px solid var(--border);}
  .logo{display:flex;align-items:center;gap:10px;}
  .logo-icon{width:36px;height:36px;background:linear-gradient(135deg,#0066cc,#0044aa);border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:18px;}
  .logo-text{font-size:22px;font-weight:700;color:var(--text);}
  .logo-text span{color:var(--accent);}
  .status-dot{display:flex;align-items:center;gap:6px;font-size:12px;color:var(--green);font-family:'JetBrains Mono',monospace;font-weight:600;}
  .dot{width:8px;height:8px;background:var(--green);border-radius:50%;animation:pulse 2s infinite;}
  @keyframes pulse{0%,100%{opacity:1;}50%{opacity:0.4;}}
  .market-bar{display:flex;border-bottom:2px solid var(--border);background:var(--surface);overflow-x:auto;scrollbar-width:none;}
  .market-bar::-webkit-scrollbar{display:none;}
  .market-item{display:flex;flex-direction:column;padding:10px 20px;border-right:1px solid var(--border);cursor:pointer;transition:background 0.2s;min-width:130px;}
  .market-item:hover{background:var(--surface2);}
  .market-label{font-size:10px;color:var(--muted);font-family:'JetBrains Mono',monospace;text-transform:uppercase;margin-bottom:2px;font-weight:600;}
  .market-value{font-size:13px;font-weight:700;font-family:'JetBrains Mono',monospace;}
  .market-value.up{color:var(--green);}
  .market-value.down{color:var(--red);}
  .search-section{padding:20px 24px 14px;background:var(--surface);border-bottom:1px solid var(--border);}
  .search-label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:2px;margin-bottom:10px;font-family:'JetBrains Mono',monospace;font-weight:600;}
  .search-box{display:flex;gap:12px;max-width:700px;position:relative;}
  .search-input{flex:1;background:var(--bg);border:2px solid var(--border);border-radius:10px;padding:13px 18px;color:var(--text);font-family:'Space Grotesk',sans-serif;font-size:15px;outline:none;transition:border-color 0.2s;}
  .search-input::placeholder{color:var(--muted);}
  .search-input:focus{border-color:var(--accent);}
  .search-btn{background:var(--accent);color:#fff;border:none;border-radius:10px;padding:13px 28px;font-family:'Space Grotesk',sans-serif;font-size:14px;font-weight:700;cursor:pointer;}
  .search-btn:hover{background:#0055aa;}
  .autocomplete{position:absolute;top:100%;left:0;right:80px;background:var(--surface);border:2px solid var(--border);border-radius:8px;z-index:100;display:none;box-shadow:0 4px 20px rgba(0,0,0,0.15);}
  .autocomplete-item{padding:10px 16px;cursor:pointer;font-size:13px;display:flex;gap:12px;align-items:center;border-bottom:1px solid var(--border);}
  .autocomplete-item:hover{background:var(--surface2);}
  .autocomplete-item:last-child{border-bottom:none;}
  .autocomplete-sym{font-family:'JetBrains Mono',monospace;color:var(--accent);font-weight:700;min-width:60px;}
  .autocomplete-name{color:var(--muted);font-size:12px;}
  .quick-picks{display:flex;gap:8px;margin-top:10px;flex-wrap:wrap;}
  .quick-pick{background:var(--bg);border:1px solid var(--border);border-radius:20px;padding:5px 14px;font-size:12px;color:var(--muted);cursor:pointer;transition:all 0.2s;font-family:'JetBrains Mono',monospace;font-weight:600;}
  .quick-pick:hover{border-color:var(--accent);color:var(--accent);background:#e6f0ff;}
  .main{padding:20px 24px 40px;display:grid;grid-template-columns:1fr 320px;gap:20px;}
  @media(max-width:900px){.main{grid-template-columns:1fr;}}
  .section-title{font-size:10px;text-transform:uppercase;letter-spacing:2px;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-bottom:12px;display:flex;align-items:center;gap:8px;font-weight:700;}
  .section-title::after{content:'';flex:1;height:1px;background:var(--border);}
  .report-card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:16px;box-shadow:0 2px 8px rgba(0,0,0,0.06);}
  .stock-header{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:20px;}
  .stock-name{font-size:28px;font-weight:700;color:var(--text);}
  .stock-full{font-size:13px;color:var(--muted);margin-top:2px;}
  .stock-sector{font-size:11px;color:var(--accent);margin-top:4px;font-family:'JetBrains Mono',monospace;font-weight:600;}
  .price-block{text-align:right;}
  .price{font-size:28px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--text);}
  .price-change{font-size:13px;font-family:'JetBrains Mono',monospace;margin-top:2px;font-weight:600;}
  .price-change.up{color:var(--green);}
  .price-change.down{color:var(--red);}
  .verdict-display{border-radius:12px;padding:20px;margin-bottom:20px;transition:all 0.3s;}
  .verdict-display.approve{background:linear-gradient(135deg,#e6f4ed,#c8ebd8);border:2px solid var(--green);}
  .verdict-display.pass{background:linear-gradient(135deg,#fce8e8,#f5c6c6);border:2px solid var(--red);}
  .verdict-display.watch{background:linear-gradient(135deg,#fff3e0,#ffe0b2);border:2px solid var(--yellow);}
  .verdict-top{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;flex-wrap:wrap;gap:10px;}
  .verdict-badge{font-size:22px;font-weight:900;font-family:'JetBrains Mono',monospace;letter-spacing:3px;padding:10px 24px;border-radius:8px;}
  .verdict-badge.approve{background:var(--green);color:#fff;}
  .verdict-badge.pass{background:var(--red);color:#fff;}
  .verdict-badge.watch{background:var(--yellow);color:#fff;}
  .verdict-score{font-size:12px;font-family:'JetBrains Mono',monospace;color:var(--muted);font-weight:600;}
  .verdict-confidence{font-size:13px;color:var(--text);margin-bottom:14px;line-height:1.6;font-weight:500;}
  .verdict-reasons{display:flex;flex-direction:column;gap:8px;}
  .verdict-reason{display:flex;align-items:flex-start;gap:10px;padding:10px 14px;background:rgba(255,255,255,0.85);border-radius:8px;font-size:13px;line-height:1.5;}
  .reason-icon{font-size:16px;flex-shrink:0;margin-top:1px;}
  .reason-label{font-weight:700;display:block;margin-bottom:2px;color:var(--text);}
  .reason-detail{color:var(--muted);font-size:12px;}
  .metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:16px;}
  .metric{background:var(--surface2);border-radius:8px;padding:12px;border:1px solid var(--border);}
  .metric-label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px;font-family:'JetBrains Mono',monospace;font-weight:600;}
  .metric-value{font-size:15px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--text);}
  .metric-value.positive{color:var(--green);}
  .metric-value.negative{color:var(--red);}
  .metric-value.neutral{color:var(--accent);}
  .intel-card{background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:14px;margin-bottom:8px;}
  .intel-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;}
  .intel-title{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--muted);font-family:'JetBrains Mono',monospace;}
  .intel-badge{font-size:10px;font-weight:700;padding:2px 8px;border-radius:10px;font-family:'JetBrains Mono',monospace;}
  .badge-green{background:var(--green-bg);color:var(--green);border:1px solid var(--green);}
  .badge-red{background:var(--red-bg);color:var(--red);border:1px solid var(--red);}
  .badge-yellow{background:var(--yellow-bg);color:var(--yellow);border:1px solid var(--yellow);}
  .intel-text{font-size:13px;color:var(--text);line-height:1.6;}
  .trade-item{padding:8px 0;border-bottom:1px solid var(--border);font-size:12px;}
  .trade-item:last-child{border-bottom:none;}
  .trade-action-buy{color:var(--green);font-weight:700;}
  .trade-action-sell{color:var(--red);font-weight:700;}
  .news-item{padding:10px 0;border-bottom:1px solid var(--border);}
  .news-item:last-child{border-bottom:none;}
  .news-source{font-size:10px;color:var(--accent);font-family:'JetBrains Mono',monospace;text-transform:uppercase;font-weight:700;margin-bottom:3px;}
  .news-headline{font-size:13px;color:var(--text);line-height:1.5;}
  .loading{display:none;text-align:center;padding:40px;color:var(--accent);font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;animation:blink 1s infinite;}
  .loading.active{display:block;}
  @keyframes blink{0%,100%{opacity:1;}50%{opacity:0.3;}}
  .signal-card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:10px;cursor:pointer;transition:all 0.2s;box-shadow:0 1px 4px rgba(0,0,0,0.05);}
  .signal-card:hover{border-color:var(--accent);}
  .signal-top{display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;}
  .signal-sym{font-size:15px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--text);}
  .signal-verdict{font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px;font-family:'JetBrains Mono',monospace;}
  .verdict-approve-badge{background:var(--green-bg);color:var(--green);border:1px solid var(--green);}
  .verdict-pass-badge{background:var(--red-bg);color:var(--red);border:1px solid var(--red);}
  .verdict-watch-badge{background:var(--yellow-bg);color:var(--yellow);border:1px solid var(--yellow);}
  .signal-price{font-size:12px;font-family:'JetBrains Mono',monospace;color:var(--muted);}
  .confluence-alert{background:linear-gradient(135deg,#0d1f2d,#1a3a5c);border:2px solid var(--accent);border-radius:10px;padding:14px;margin-bottom:10px;color:#fff;}
  .confluence-title{font-size:12px;font-weight:700;color:var(--accent);font-family:'JetBrains Mono',monospace;margin-bottom:4px;}
  .confluence-text{font-size:12px;color:#a0b8cc;line-height:1.5;}
  .footer{text-align:center;padding:16px;border-top:1px solid var(--border);font-size:10px;color:var(--muted);font-family:'JetBrains Mono',monospace;background:var(--surface);line-height:1.8;}
</style>
</head>
<body>
<div class="ticker-bar"><div class="ticker-track" id="tickerTrack"><span class="ticker-item"><span class="ticker-sym">Loading market data...</span></span></div></div>
<div class="header">
  <div class="logo"><div class="logo-icon">&#9889;</div><div class="logo-text">Apex <span>Q</span></div></div>
  <div class="status-dot"><div class="dot"></div>LIVE INTEL ACTIVE</div>
</div>
<div class="market-bar">
  <div class="market-item" onclick="tickerClick('^GSPC')"><div class="market-label">S&amp;P 500</div><div class="market-value up" id="m-SP">Loading...</div></div>
  <div class="market-item" onclick="tickerClick('^IXIC')"><div class="market-label">NASDAQ</div><div class="market-value up" id="m-NQ">Loading...</div></div>
  <div class="market-item" onclick="tickerClick('^DJI')"><div class="market-label">DOW JONES</div><div class="market-value up" id="m-DJ">Loading...</div></div>
  <div class="market-item" onclick="tickerClick('^RUT')"><div class="market-label">RUSSELL 2000</div><div class="market-value up" id="m-RU">Loading...</div></div>
  <div class="market-item" onclick="tickerClick('^VIX')"><div class="market-label">VIX FEAR</div><div class="market-value" id="m-VX">Loading...</div></div>
  <div class="market-item" onclick="tickerClick('GC=F')"><div class="market-label">GOLD</div><div class="market-value up" id="m-GD">Loading...</div></div>
  <div class="market-item" onclick="tickerClick('CL=F')"><div class="market-label">OIL WTI</div><div class="market-value" id="m-OL">Loading...</div></div>
  <div class="market-item" onclick="tickerClick('BTC-USD')"><div class="market-label">BITCOIN</div><div class="market-value up" id="m-BT">Loading...</div></div>
</div>
<div class="search-section">
  <div class="search-label">Search any stock or company name</div>
  <div class="search-box">
    <input class="search-input" type="text" id="searchInput" placeholder="Type a company name or ticker... e.g. Apple, Tesla, SpaceX, SOFI" autocomplete="off"/>
    <div class="autocomplete" id="autocomplete"></div>
    <button class="search-btn" onclick="analyzeStock()">ANALYZE</button>
  </div>
  <div class="quick-picks">
    <div class="quick-pick" onclick="tickerClick('SOFI')">SOFI</div>
    <div class="quick-pick" onclick="tickerClick('SPCX')">SpaceX</div>
    <div class="quick-pick" onclick="tickerClick('NVDA')">NVDA</div>
    <div class="quick-pick" onclick="tickerClick('AAPL')">Apple</div>
    <div class="quick-pick" onclick="tickerClick('AMD')">AMD</div>
    <div class="quick-pick" onclick="tickerClick('TSLA')">Tesla</div>
    <div class="quick-pick" onclick="tickerClick('MSFT')">Microsoft</div>
    <div class="quick-pick" onclick="tickerClick('AMZN')">Amazon</div>
  </div>
</div>
<div class="main">
  <div class="left-col">
    <div class="section-title">Full Intelligence Report</div>
    <div id="loading" class="loading">Running intelligence agents... stand by</div>
    <div class="report-card" id="report">
      <div class="stock-header">
        <div>
          <div class="stock-name" id="stockSymbol">APEX Q</div>
          <div class="stock-full" id="stockName">Search a stock above to begin full analysis</div>
          <div class="stock-sector" id="stockSector"></div>
        </div>
        <div class="price-block">
          <div class="price" id="stockPrice">--</div>
          <div class="price-change up" id="stockChange">-- today</div>
        </div>
      </div>
      <div class="verdict-display watch" id="verdictDisplay">
        <div class="verdict-top">
          <div class="verdict-badge watch" id="verdictBadge">&#9889; READY</div>
          <div class="verdict-score" id="verdictScore"></div>
        </div>
        <div class="verdict-confidence" id="verdictConfidence">Search any stock or company above. Apex Q will run all four intelligence agents simultaneously and synthesize a data-driven verdict with full reasoning.</div>
        <div class="verdict-reasons" id="verdictReasons">
          <div class="verdict-reason"><span class="reason-icon">&#128202;</span><div class="reason-text"><span class="reason-label">Analyst Agent</span><span class="reason-detail">Price momentum and valuation analysis via yFinance and Finnhub</span></div></div>
          <div class="verdict-reason"><span class="reason-icon">&#127963;</span><div class="reason-text"><span class="reason-label">Regulatory Agent</span><span class="reason-detail">Congressional trading patterns via Quiver Quantitative</span></div></div>
          <div class="verdict-reason"><span class="reason-icon">&#128188;</span><div class="reason-text"><span class="reason-label">Insider Agent</span><span class="reason-detail">C-Level cluster buy and sell detection via SEC Form 4</span></div></div>
          <div class="verdict-reason"><span class="reason-icon">&#128196;</span><div class="reason-text"><span class="reason-label">SEC Agent</span><span class="reason-detail">Filing activity and regulatory disclosure monitoring</span></div></div>
        </div>
      </div>
      <div class="metrics">
        <div class="metric"><div class="metric-label">Current Price</div><div class="metric-value neutral" id="metricPrice">--</div></div>
        <div class="metric"><div class="metric-label">Change Today</div><div class="metric-value" id="metricChange">--</div></div>
        <div class="metric"><div class="metric-label">Signal Score</div><div class="metric-value neutral" id="metricScore">--</div></div>
        <div class="metric"><div class="metric-label">PE Ratio</div><div class="metric-value" id="metricPE">--</div></div>
        <div class="metric"><div class="metric-label">Analyst Target</div><div class="metric-value positive" id="metricTarget">--</div></div>
        <div class="metric"><div class="metric-label">Market Cap</div><div class="metric-value neutral" id="metricMcap">--</div></div>
      </div>

      <div class="section-title">Congressional Trading Intelligence</div>
      <div id="congressSection"><div class="intel-card"><div class="intel-header"><div class="intel-title">Quiver Quantitative</div><div class="intel-badge badge-yellow">WAITING</div></div><div class="intel-text">Congressional trading data will load after analysis.</div></div></div>

      <div class="section-title">Insider Activity</div>
      <div id="insiderSection"><div class="intel-card"><div class="intel-header"><div class="intel-title">Insider Trades</div><div class="intel-badge badge-yellow">WAITING</div></div><div class="intel-text">C-Level insider buy and sell activity will appear here.</div></div></div>

      <div class="section-title">News Intelligence Feed</div>
      <div id="newsSection"><div class="intel-card"><div class="intel-header"><div class="intel-title">Finnhub News</div><div class="intel-badge badge-yellow">WAITING</div></div><div class="intel-text">Live news feed will load after analysis.</div></div></div>
    </div>
  </div>
  <div class="right-col">
    <div class="section-title">Live Signals</div>
    <div id="signalsPanel">
      <div class="signal-card" onclick="tickerClick('SOFI')"><div class="signal-top"><div class="signal-sym">SOFI</div><div class="signal-verdict verdict-watch-badge">WATCH</div></div><div class="signal-price">Click to analyze</div></div>
      <div class="signal-card" onclick="tickerClick('NVDA')"><div class="signal-top"><div class="signal-sym">NVDA</div><div class="signal-verdict verdict-watch-badge">WATCH</div></div><div class="signal-price">Click to analyze</div></div>
      <div class="signal-card" onclick="tickerClick('SPCX')"><div class="signal-top"><div class="signal-sym">SPCX</div><div class="signal-verdict verdict-watch-badge">WATCH</div></div><div class="signal-price">Click to analyze</div></div>
      <div class="signal-card" onclick="tickerClick('AMD')"><div class="signal-top"><div class="signal-sym">AMD</div><div class="signal-verdict verdict-watch-badge">WATCH</div></div><div class="signal-price">Click to analyze</div></div>
      <div class="signal-card" onclick="tickerClick('TSLA')"><div class="signal-top"><div class="signal-sym">TSLA</div><div class="signal-verdict verdict-watch-badge">WATCH</div></div><div class="signal-price">Click to analyze</div></div>
      <div class="signal-card" onclick="tickerClick('AAPL')"><div class="signal-top"><div class="signal-sym">AAPL</div><div class="signal-verdict verdict-watch-badge">WATCH</div></div><div class="signal-price">Click to analyze</div></div>
    </div>
  </div>
</div>
<div class="footer">
  APEX Q INTELLIGENCE TERMINAL &nbsp;|&nbsp; ANALYST AGENT + REGULATORY AGENT + INSIDER AGENT + SEC AGENT<br>
  The insights provided are generated by our analytical engine for educational and illustrative purposes only.<br>
  They are not intended as financial, investment, or legal advice. Every market participant is unique.<br>
  We encourage you to perform your own due diligence or consult with a qualified professional before making any financial decisions.
</div>
<script>
const API = window.location.origin;
const TICKERS = ['AAPL','MSFT','NVDA','AMD','TSLA','AMZN','GOOGL','META','SOFI','SPCX','SCHD','JPM','BAC','NFLX','BTC-USD'];

function formatNum(n) {
  if (!n || n === 'N/A') return 'N/A';
  const num = parseFloat(n);
  if (isNaN(num)) return 'N/A';
  if (num >= 1e12) return '$' + (num/1e12).toFixed(2) + 'T';
  if (num >= 1e9) return '$' + (num/1e9).toFixed(2) + 'B';
  if (num >= 1e6) return '$' + (num/1e6).toFixed(2) + 'M';
  return '$' + num.toLocaleString();
}

function updateCongressSection(trades) {
  const section = document.getElementById('congressSection');
  if (!trades || trades.length === 0) {
    section.innerHTML = '<div class="intel-card"><div class="intel-header"><div class="intel-title">Congressional Trading</div><div class="intel-badge badge-green">CLEAN</div></div><div class="intel-text">No recent congressional trading activity detected for this stock.</div></div>';
    return;
  }
  const buys = trades.filter(t => t.action && (t.action.includes('Purchase') || t.action.includes('Buy')));
  const sells = trades.filter(t => t.action && (t.action.includes('Sale') || t.action.includes('Sell')));
  const badge = buys.length > sells.length ? 'badge-green' : sells.length > buys.length ? 'badge-red' : 'badge-yellow';
  const label = buys.length > sells.length ? 'BUYING' : sells.length > buys.length ? 'SELLING' : 'MIXED';
  section.innerHTML = `<div class="intel-card">
    <div class="intel-header"><div class="intel-title">Congressional Trading — ${trades.length} trades</div><div class="intel-badge ${badge}">${label}</div></div>
    <div class="intel-text">
      ${trades.map(t => `<div class="trade-item">
        <span class="${t.action && t.action.includes('Purchase') ? 'trade-action-buy' : 'trade-action-sell'}">${t.action || 'Unknown'}</span>
        &nbsp; ${t.politician || 'Unknown'} (${t.party || ''}) &nbsp; ${t.amount || ''} &nbsp; <span style="color:var(--muted)">${t.date || ''}</span>
      </div>`).join('')}
    </div>
  </div>`;
}

function updateInsiderSection(trades) {
  const section = document.getElementById('insiderSection');
  if (!trades || trades.length === 0) {
    section.innerHTML = '<div class="intel-card"><div class="intel-header"><div class="intel-title">Insider Activity</div><div class="intel-badge badge-green">CLEAN</div></div><div class="intel-text">No recent insider trading activity detected. Clean insider slate.</div></div>';
    return;
  }
  const buys = trades.filter(t => t.action === 'A');
  const sells = trades.filter(t => t.action === 'D');
  const badge = buys.length > sells.length ? 'badge-green' : sells.length > buys.length ? 'badge-red' : 'badge-yellow';
  const label = buys.length > sells.length ? 'BUYING' : sells.length > buys.length ? 'SELLING' : 'MIXED';
  section.innerHTML = `<div class="intel-card">
    <div class="intel-header"><div class="intel-title">Insider Trades — ${trades.length} filings</div><div class="intel-badge ${badge}">${label}</div></div>
    <div class="intel-text">
      ${trades.slice(0,5).map(t => `<div class="trade-item">
        <span class="${t.action === 'A' ? 'trade-action-buy' : 'trade-action-sell'}">${t.action === 'A' ? 'BUY' : 'SELL'}</span>
        &nbsp; ${t.name || 'Unknown'} &nbsp; <span style="color:var(--muted)">${t.title || ''}</span>
        &nbsp; ${t.shares ? t.shares.toLocaleString() + ' shares' : ''} &nbsp; <span style="color:var(--muted)">${t.date || ''}</span>
      </div>`).join('')}
    </div>
  </div>`;
}

function updateNewsSection(news) {
  const section = document.getElementById('newsSection');
  if (!news || news.length === 0) {
    section.innerHTML = '<div class="intel-card"><div class="intel-header"><div class="intel-title">News Feed</div><div class="intel-badge badge-yellow">NO DATA</div></div><div class="intel-text">Add FINNHUB_KEY to Railway variables to enable live news intelligence.</div></div>';
    return;
  }
  section.innerHTML = news.map(n => `<div class="news-item">
    <div class="news-source">${n.source || 'Market News'}</div>
    <div class="news-headline">${n.headline}</div>
  </div>`).join('');
}

async function loadTicker(sym) {
  try {
    const res = await fetch(`${API}/analyze?symbol=${encodeURIComponent(sym)}`);
    const d = await res.json();
    if (d.price) {
      const cc = d.change_pct>=0?'up':'down';
      const cs = (d.change_pct>=0?'+':'')+d.change_pct+'%';
      return `<span class="ticker-item" onclick="tickerClick('${sym}')"><span class="ticker-sym">${d.symbol}</span><span class="ticker-price">$${d.price.toLocaleString()}</span><span class="ticker-change ${cc}">${cs}</span></span>`;
    }
  } catch(e){}
  return '';
}

async function buildTicker() {
  const track = document.getElementById('tickerTrack');
  let html = '';
  for (const sym of TICKERS) { html += await loadTicker(sym); }
  if (html) track.innerHTML = html + html;
}

async function loadMarketBar() {
  const markets = [
    {sym:'^GSPC',id:'m-SP'},{sym:'^IXIC',id:'m-NQ'},{sym:'^DJI',id:'m-DJ'},
    {sym:'^RUT',id:'m-RU'},{sym:'^VIX',id:'m-VX'},{sym:'GC=F',id:'m-GD'},
    {sym:'CL=F',id:'m-OL'},{sym:'BTC-USD',id:'m-BT'}
  ];
  for (const m of markets) {
    try {
      const res = await fetch(`${API}/analyze?symbol=${encodeURIComponent(m.sym)}`);
      const d = await res.json();
      if (d.price) {
        const el = document.getElementById(m.id);
        if (el) {
          el.textContent = d.price.toLocaleString() + ' (' + (d.change_pct>=0?'+':'') + d.change_pct + '%)';
          el.className = 'market-value ' + (d.change_pct>=0?'up':'down');
        }
      }
    } catch(e){}
  }
}

function tickerClick(sym) {
  document.getElementById('searchInput').value = sym;
  analyzeStock();
}

let searchTimeout;
document.getElementById('searchInput').addEventListener('input', function() {
  clearTimeout(searchTimeout);
  const val = this.value.trim();
  if (val.length < 2) { document.getElementById('autocomplete').style.display='none'; return; }
  searchTimeout = setTimeout(() => fetchSuggestions(val), 300);
});

async function fetchSuggestions(query) {
  try {
    const res = await fetch(`${API}/search?q=${encodeURIComponent(query)}`);
    const data = await res.json();
    const ac = document.getElementById('autocomplete');
    if (data.results && data.results.length > 0) {
      ac.innerHTML = data.results.map(r=>`<div class="autocomplete-item" onclick="tickerClick('${r.symbol}')"><span class="autocomplete-sym">${r.symbol}</span><span class="autocomplete-name">${r.name||''}</span></div>`).join('');
      ac.style.display = 'block';
    } else { ac.style.display = 'none'; }
  } catch(e){}
}

document.addEventListener('click', function(e) {
  if (!e.target.closest('.search-box')) document.getElementById('autocomplete').style.display = 'none';
});

async function analyzeStock() {
  const val = document.getElementById('searchInput').value.trim();
  if (!val) return;
  document.getElementById('autocomplete').style.display = 'none';
  document.getElementById('loading').classList.add('active');
  document.getElementById('report').style.opacity = '0.4';

  try {
    const res = await fetch(`${API}/analyze?symbol=${encodeURIComponent(val)}`);
    const data = await res.json();

    if (data.error) {
      document.getElementById('stockSymbol').textContent = 'NOT FOUND';
      document.getElementById('stockName').textContent = data.error;
      document.getElementById('loading').classList.remove('active');
      document.getElementById('report').style.opacity = '1';
      return;
    }

    document.getElementById('stockSymbol').textContent = data.symbol || val;
    document.getElementById('stockName').textContent = data.name || val;
    document.getElementById('stockSector').textContent = data.sector || '';
    document.getElementById('stockPrice').textContent = '$' + (data.price||0).toLocaleString();
    document.getElementById('metricPrice').textContent = '$' + (data.price||0).toLocaleString();
    const change = data.change_pct || 0;
    document.getElementById('metricChange').textContent = (change>=0?'+':'') + change + '%';
    document.getElementById('metricChange').className = 'metric-value ' + (change>=0?'positive':'negative');
    document.getElementById('metricScore').textContent = (data.score||0) + '/10';
    document.getElementById('metricPE').textContent = data.pe_ratio || 'N/A';
    document.getElementById('metricTarget').textContent = data.analyst_target ? '$' + data.analyst_target : 'N/A';
    document.getElementById('metricMcap').textContent = formatNum(data.market_cap);

    const changeEl = document.getElementById('stockChange');
    changeEl.textContent = (change>=0?'+':'') + change + '% today';
    changeEl.className = 'price-change ' + (change>=0?'up':'down');

    const verdict = data.verdict || 'WATCH';
    const display = document.getElementById('verdictDisplay');
    const badge = document.getElementById('verdictBadge');
    const confidence = document.getElementById('verdictConfidence');
    const reasonsEl = document.getElementById('verdictReasons');
    const scoreEl = document.getElementById('verdictScore');

    display.className = 'verdict-display ' + verdict.toLowerCase();
    badge.className = 'verdict-badge ' + verdict.toLowerCase();
    const icons = {APPROVE:'&#9989;',PASS:'&#10060;',WATCH:'&#9889;'};
    badge.innerHTML = icons[verdict] + ' ' + verdict;
    scoreEl.textContent = 'Confidence Score: ' + (data.score||0) + '/10';
    confidence.textContent = data.confidence || '';

    if (data.reasons) {
      reasonsEl.innerHTML = data.reasons.map(r=>`<div class="verdict-reason"><span class="reason-icon">${r.icon}</span><div class="reason-text"><span class="reason-label">${r.label}</span><span class="reason-detail">${r.detail}</span></div></div>`).join('');
    }

    updateCongressSection(data.congressional || []);
    updateInsiderSection(data.insider || []);
    updateNewsSection(data.news || []);

    const vc = verdict==='APPROVE'?'verdict-approve-badge':verdict==='PASS'?'verdict-pass-badge':'verdict-watch-badge';
    const panel = document.getElementById('signalsPanel');
    const existing = panel.querySelector(`[data-sym="${data.symbol}"]`);
    const card = `<div class="signal-card" data-sym="${data.symbol}" onclick="tickerClick('${data.symbol}')">
      <div class="signal-top"><div class="signal-sym">${data.symbol}</div><div class="signal-verdict ${vc}">${verdict}</div></div>
      <div class="signal-price">$${(data.price||0).toLocaleString()} &nbsp; Score: ${data.score||0}/10</div>
    </div>`;
    if (existing) { existing.outerHTML = card; } else { panel.insertAdjacentHTML('afterbegin', card); }

  } catch(e) {
    document.getElementById('stockSymbol').textContent = 'ERROR';
    document.getElementById('stockName').textContent = 'Could not connect. Try again.';
  }

  document.getElementById('loading').classList.remove('active');
  document.getElementById('report').style.opacity = '1';
}

document.getElementById('searchInput').addEventListener('keypress', function(e) {
  if (e.key === 'Enter') analyzeStock();
});

buildTicker();
loadMarketBar();
</script>
</body>
</html>"""

@app.route("/")
def home():
    return Response(HTML, mimetype='text/html')

@app.route("/search")
def search_ticker():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"error": "No query provided"}), 400
    try:
        search = yf.Search(query, max_results=5)
        quotes = search.quotes
        results = [{"symbol": q.get("symbol"), "name": q.get("longname") or q.get("shortname"), "exchange": q.get("exchange")} for q in quotes if q.get("symbol")]
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/analyze")
def analyze():
    query = request.args.get("symbol", "").strip()
    if not query:
        return jsonify({"error": "No symbol provided"}), 400

    symbol = resolve_ticker(query)

    market = get_market_data(symbol)
    if not market:
        return jsonify({"error": f"No data found for {symbol}."}), 404

    news = get_finnhub_news(symbol)
    congressional = get_quiver_congressional(symbol)
    insider = get_quiver_insider(symbol)
    sec = get_sec_filings(symbol)

    verdict, confidence, reasons, score, signals = synthesize_verdict(market, congressional, insider, sec)

    return jsonify({
        "symbol": symbol,
        "query": query,
        "price": market["price"],
        "change_pct": market["change_pct"],
        "recommendation": market["recommendation"],
        "verdict": verdict,
        "confidence": confidence,
        "score": score,
        "signals": signals,
        "reasons": reasons,
        "name": market["name"],
        "sector": market["sector"],
        "pe_ratio": market["pe_ratio"],
        "market_cap": market["market_cap"],
        "analyst_target": market["analyst_target"],
        "news": news,
        "congressional": congressional,
        "insider": insider,
        "sec": sec
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
