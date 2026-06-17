from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import yfinance as yf
import requests
import os
import time
import json
import logging
from datetime import datetime, timedelta

# Setup logging for audit trail
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

FINNHUB_KEY = os.environ.get("FINNHUB_KEY", "")
QUIVER_KEY = os.environ.get("QUIVER_KEY", "")

# In-memory cache with 4 hour TTL
CACHE = {}
CACHE_TTL = 60 * 60 * 4

def get_cache(key):
    if key in CACHE:
        data, timestamp = CACHE[key]
        if time.time() - timestamp < CACHE_TTL:
            logger.info(f"CACHE HIT: {key}")
            return data
    return None

def set_cache(key, data):
    CACHE[key] = (data, time.time())
    logger.info(f"CACHE SET: {key}")

# ─────────────────────────────────────────
# AGENT 1: Market Data Agent
# ─────────────────────────────────────────
class MarketDataAgent:
    def get(self, symbol):
        cached = get_cache(f"market_{symbol}")
        if cached:
            return cached
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="5d")
            info = ticker.info
            if hist.empty:
                return None
            current = round(float(hist["Close"].iloc[-1]), 2)
            prev = round(float(hist["Close"].iloc[-2]), 2) if len(hist) > 1 else current
            change_pct = round(((current - prev) / prev) * 100, 2)
            result = {
                "price": current,
                "change_pct": change_pct,
                "recommendation": info.get("recommendationKey", "hold").upper(),
                "name": info.get("longName", symbol),
                "sector": info.get("sector", "N/A"),
                "pe_ratio": round(float(info.get("trailingPE", 0)), 2) if info.get("trailingPE") else "N/A",
                "market_cap": info.get("marketCap", "N/A"),
                "analyst_target": info.get("targetMeanPrice", "N/A"),
                "volume": int(hist["Volume"].iloc[-1]),
                "52w_high": info.get("fiftyTwoWeekHigh", "N/A"),
                "52w_low": info.get("fiftyTwoWeekLow", "N/A"),
                "dividend_yield": info.get("dividendYield", "N/A"),
                "beta": info.get("beta", "N/A"),
            }
            set_cache(f"market_{symbol}", result)
            logger.info(f"MARKET AGENT: {symbol} price=${current} change={change_pct}%")
            return result
        except Exception as e:
            logger.error(f"MARKET AGENT ERROR: {symbol} - {e}")
            return None

# ─────────────────────────────────────────
# AGENT 2: News Agent (Finnhub)
# ─────────────────────────────────────────
class NewsAgent:
    def get(self, symbol):
        cached = get_cache(f"news_{symbol}")
        if cached is not None:
            return cached
        if not FINNHUB_KEY:
            logger.warning("NEWS AGENT: No FINNHUB_KEY found in environment")
            return []
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            week_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            url = f"https://finnhub.io/api/v1/company-news?symbol={symbol}&from={week_ago}&to={today}&token={FINNHUB_KEY}"
            resp = requests.get(url, timeout=8)
            logger.info(f"NEWS AGENT: Finnhub status={resp.status_code} for {symbol}")
            if resp.status_code == 200:
                data = resp.json()
                news = []
                for n in data[:8]:
                    if n.get("headline"):
                        news.append({
                            "headline": n.get("headline"),
                            "source": n.get("source", "Market News"),
                            "summary": n.get("summary", ""),
                            "url": n.get("url", ""),
                            "datetime": n.get("datetime", 0)
                        })
                set_cache(f"news_{symbol}", news)
                logger.info(f"NEWS AGENT: {len(news)} articles found for {symbol}")
                return news
            else:
                logger.warning(f"NEWS AGENT: Bad response {resp.status_code} - {resp.text[:200]}")
                return []
        except Exception as e:
            logger.error(f"NEWS AGENT ERROR: {symbol} - {e}")
            return []

# ─────────────────────────────────────────
# AGENT 3: Regulatory Agent (Quiver - Congressional)
# ─────────────────────────────────────────
class RegulatoryAgent:
    def get_congressional(self, symbol):
        cached = get_cache(f"congress_{symbol}")
        if cached is not None:
            return cached
        if not QUIVER_KEY:
            logger.warning("REGULATORY AGENT: No QUIVER_KEY found")
            return []
        try:
            url = f"https://api.quiverquant.com/beta/historical/congresstrading/{symbol}"
            headers = {"Authorization": f"Token {QUIVER_KEY}", "Accept": "application/json"}
            resp = requests.get(url, headers=headers, timeout=8)
            logger.info(f"REGULATORY AGENT: Quiver congress status={resp.status_code} for {symbol}")
            if resp.status_code == 200:
                trades = resp.json()
                result = []
                for t in trades[:10]:
                    result.append({
                        "politician": t.get("Representative", "Unknown"),
                        "party": t.get("Party", ""),
                        "action": t.get("Transaction", "Unknown"),
                        "amount": t.get("Range", "Unknown"),
                        "date": t.get("TransactionDate", ""),
                        "ticker": t.get("Ticker", symbol)
                    })
                set_cache(f"congress_{symbol}", result)
                logger.info(f"REGULATORY AGENT: {len(result)} congressional trades for {symbol}")
                return result
            else:
                logger.warning(f"REGULATORY AGENT: {resp.status_code} - {resp.text[:200]}")
                return []
        except Exception as e:
            logger.error(f"REGULATORY AGENT ERROR: {e}")
            return []

# ─────────────────────────────────────────
# AGENT 4: Insider Agent (Quiver - Insiders)
# ─────────────────────────────────────────
class InsiderAgent:
    C_LEVEL = ["CEO", "CFO", "COO", "PRESIDENT", "CHAIRMAN", "CTO", "CIO", "DIRECTOR", "FOUNDER"]

    def get(self, symbol):
        cached = get_cache(f"insider_{symbol}")
        if cached is not None:
            return cached
        if not QUIVER_KEY:
            logger.warning("INSIDER AGENT: No QUIVER_KEY found")
            return []
        try:
            url = f"https://api.quiverquant.com/beta/historical/insiders/{symbol}"
            headers = {"Authorization": f"Token {QUIVER_KEY}", "Accept": "application/json"}
            resp = requests.get(url, headers=headers, timeout=8)
            logger.info(f"INSIDER AGENT: Quiver insiders status={resp.status_code} for {symbol}")
            if resp.status_code == 200:
                trades = resp.json()
                result = []
                for t in trades[:15]:
                    title = str(t.get("Title", "")).upper()
                    is_clevel = any(c in title for c in self.C_LEVEL)
                    result.append({
                        "name": t.get("Name", "Unknown"),
                        "title": t.get("Title", "Unknown"),
                        "action": t.get("AcquiredDisposed", "Unknown"),
                        "shares": t.get("Shares", 0),
                        "price": t.get("Price", 0),
                        "date": t.get("Date", ""),
                        "is_clevel": is_clevel
                    })
                set_cache(f"insider_{symbol}", result)
                logger.info(f"INSIDER AGENT: {len(result)} insider trades for {symbol}")
                return result
            else:
                logger.warning(f"INSIDER AGENT: {resp.status_code} - {resp.text[:200]}")
                return []
        except Exception as e:
            logger.error(f"INSIDER AGENT ERROR: {e}")
            return []

# ─────────────────────────────────────────
# ORCHESTRATOR: Synthesis Engine
# ─────────────────────────────────────────
class Orchestrator:
    def synthesize(self, symbol, market, congressional, insider, news):
        score = 0
        reasons = []
        signals = []

        # Market signals
        change_pct = market.get("change_pct", 0)
        rec = market.get("recommendation", "HOLD")
        target = market.get("analyst_target")
        price = market.get("price", 0)
        pe = market.get("pe_ratio", "N/A")

        if change_pct > 3:
            score += 3
            signals.append("STRONG_MOMENTUM")
            reasons.append({"icon": "&#128200;", "label": f"Strong Price Momentum: +{change_pct}%", "detail": f"{symbol} is up {change_pct}% today. Strong buying pressure with institutional demand driving the move."})
        elif change_pct > 1:
            score += 2
            signals.append("POSITIVE_MOMENTUM")
            reasons.append({"icon": "&#128202;", "label": f"Positive Price Action: +{change_pct}%", "detail": f"Stock trending upward {change_pct}% today. Buyers are in control with steady accumulation."})
        elif change_pct > 0:
            score += 1
            reasons.append({"icon": "&#10145;", "label": f"Slight Upward Drift: +{change_pct}%", "detail": "Modest positive movement. No strong conviction yet but direction is favorable."})
        elif change_pct < -4:
            score -= 3
            signals.append("HEAVY_SELLING")
            reasons.append({"icon": "&#128201;", "label": f"Heavy Selling Pressure: {change_pct}%", "detail": f"Down {abs(change_pct)}% today. Sellers are dominating. This level of decline signals elevated risk."})
        elif change_pct < -2:
            score -= 2
            signals.append("SELLING_PRESSURE")
            reasons.append({"icon": "&#128201;", "label": f"Significant Decline: {change_pct}%", "detail": f"Down {abs(change_pct)}% today. Bearish price action. Wait for stabilization before considering entry."})
        else:
            score -= 1
            reasons.append({"icon": "&#10145;", "label": f"Slight Decline: {change_pct}%", "detail": "Minor pullback. Could be normal profit taking or the beginning of a trend change."})

        # Analyst consensus
        if rec in ["BUY", "STRONG_BUY"]:
            score += 2
            signals.append("ANALYST_BUY")
            reasons.append({"icon": "&#9989;", "label": f"Wall Street Rating: {rec.replace('_', ' ')}", "detail": "Professional analysts rate this stock a Buy. Institutional money managers see significant upside potential ahead."})
        elif rec in ["SELL", "STRONG_SELL"]:
            score -= 2
            signals.append("ANALYST_SELL")
            reasons.append({"icon": "&#9940;", "label": f"Wall Street Rating: {rec.replace('_', ' ')}", "detail": "Analysts are negative on this stock. Professional consensus sees downside risk. Proceed with extreme caution."})
        elif rec == "HOLD":
            reasons.append({"icon": "&#9888;", "label": "Wall Street Rating: Hold", "detail": "Analysts are neutral. No strong conviction either direction. Watch for a catalyst to break the stalemate."})

        # Price target analysis
        if target and price and float(str(target)) > 0:
            try:
                upside = round(((float(str(target)) - price) / price) * 100, 1)
                if upside > 15:
                    score += 2
                    reasons.append({"icon": "&#127919;", "label": f"{upside}% Upside to Analyst Target: ${target}", "detail": f"At ${price} today the stock has {upside}% room to grow to reach the analyst consensus target of ${target}. Strong projected upside."})
                elif upside > 5:
                    score += 1
                    reasons.append({"icon": "&#127919;", "label": f"{upside}% Upside to Target: ${target}", "detail": f"Modest upside of {upside}% from current price ${price} to analyst target ${target}."})
                elif upside < -5:
                    score -= 1
                    reasons.append({"icon": "&#127919;", "label": f"Trading {abs(upside)}% Above Target", "detail": f"Current price ${price} exceeds analyst target ${target}. The stock may be overvalued at current levels."})
            except:
                pass

        # PE valuation
        if pe and pe != "N/A":
            try:
                pe_num = float(str(pe))
                if pe_num < 12:
                    score += 2
                    reasons.append({"icon": "&#128176;", "label": f"PE Ratio {pe_num:.1f} — Deeply Undervalued", "detail": f"PE of {pe_num:.1f} is significantly below market average. The stock appears cheap relative to its earnings power."})
                elif pe_num < 20:
                    score += 1
                    reasons.append({"icon": "&#128176;", "label": f"PE Ratio {pe_num:.1f} — Fair Value", "detail": f"PE of {pe_num:.1f} is reasonable relative to market averages. Stock is not overpriced at current levels."})
                elif pe_num > 60:
                    score -= 1
                    reasons.append({"icon": "&#128184;", "label": f"PE Ratio {pe_num:.1f} — Premium Valuation", "detail": f"High PE of {pe_num:.1f} means investors are paying a premium. Growth expectations must be met or the stock could drop significantly."})
                else:
                    reasons.append({"icon": "&#128203;", "label": f"PE Ratio {pe_num:.1f} — Moderate Valuation", "detail": f"PE of {pe_num:.1f} is above average but not extreme. Reasonable for a growth company with strong fundamentals."})
            except:
                pass

        # Congressional trading
        if congressional:
            buys = [t for t in congressional if t.get("action") and ("purchase" in t["action"].lower() or "buy" in t["action"].lower())]
            sells = [t for t in congressional if t.get("action") and ("sale" in t["action"].lower() or "sell" in t["action"].lower())]
            if len(buys) >= 3:
                score += 3
                signals.append("CONGRESS_CLUSTER_BUY")
                politicians = ", ".join([b.get("politician", "Unknown") for b in buys[:3]])
                reasons.append({"icon": "&#127963;", "label": f"Congressional Cluster Buy: {len(buys)} Politicians", "detail": f"Multiple members of Congress including {politicians} recently purchased this stock. Congressional members have access to regulatory and policy information before the public."})
            elif len(buys) > len(sells) and buys:
                score += 2
                signals.append("CONGRESS_BUYING")
                politician = buys[0].get("politician", "Unknown")
                reasons.append({"icon": "&#127963;", "label": f"Congressional Buying: {politician}", "detail": f"{politician} and others recently purchased shares. When politicians buy with their own money it is one of the most meaningful signals in the market."})
            elif len(sells) > len(buys) and sells:
                score -= 1
                signals.append("CONGRESS_SELLING")
                reasons.append({"icon": "&#127963;", "label": f"Congressional Selling: {len(sells)} trades", "detail": f"{len(sells)} congressional members recently sold this stock. Worth monitoring as political insiders may have information about upcoming regulatory changes."})

        # Insider trading
        if insider:
            c_buys = [t for t in insider if t.get("is_clevel") and t.get("action") == "A"]
            c_sells = [t for t in insider if t.get("is_clevel") and t.get("action") == "D"]
            all_buys = [t for t in insider if t.get("action") == "A"]

            if len(c_buys) >= 3:
                score += 4
                signals.append("INSIDER_CLUSTER_BUY")
                names = ", ".join([f"{t.get('name', 'Unknown')} ({t.get('title', '')})" for t in c_buys[:3]])
                reasons.append({"icon": "&#128188;", "label": f"C-Level Cluster Buy — HIGHEST CONVICTION SIGNAL", "detail": f"Multiple executives buying simultaneously: {names}. When 3 or more C-level insiders buy at once this is one of the most powerful signals in the entire market. Insiders only buy for one reason."})
            elif len(c_buys) == 2:
                score += 3
                signals.append("INSIDER_CLUSTER_BUY")
                reasons.append({"icon": "&#128188;", "label": f"Dual Executive Buy Signal", "detail": f"{c_buys[0].get('name')} ({c_buys[0].get('title')}) and {c_buys[1].get('name')} ({c_buys[1].get('title')}) both purchased shares recently. Strong insider conviction signal."})
            elif len(c_buys) == 1:
                score += 2
                signals.append("INSIDER_BUY")
                reasons.append({"icon": "&#128188;", "label": f"Executive Buy: {c_buys[0].get('title')}", "detail": f"{c_buys[0].get('name')} recently purchased {c_buys[0].get('shares', 0):,} shares. Insiders only buy for one reason: they believe the stock is going higher."})
            elif len(c_sells) >= 2:
                score -= 2
                signals.append("INSIDER_CLUSTER_SELL")
                reasons.append({"icon": "&#128188;", "label": f"Executive Cluster Selling: {len(c_sells)} officers", "detail": f"Multiple C-level executives are selling shares. While executives sell for many reasons, heavy cluster selling is a caution flag that warrants attention."})

        # Confluence bonus — the most powerful signal
        confluence_signals = [s for s in ["CONGRESS_BUYING", "CONGRESS_CLUSTER_BUY", "INSIDER_BUY", "INSIDER_CLUSTER_BUY", "ANALYST_BUY", "STRONG_MOMENTUM"] if s in signals]
        if len(confluence_signals) >= 3:
            score += 3
            reasons.append({"icon": "&#9889;", "label": f"CONFLUENCE DETECTED — {len(confluence_signals)} Signals Aligned", "detail": f"Rare alignment across {len(confluence_signals)} independent intelligence layers: {', '.join(confluence_signals)}. When multiple unrelated data sources point in the same direction simultaneously this is the highest conviction signal Apex Q can produce."})

        # Final verdict
        if score >= 5:
            verdict = "APPROVE"
            confidence = f"HIGH CONVICTION BUY SIGNAL. Intelligence score {score}/15. Multiple independent data sources are aligned bullish. Price momentum, analyst consensus, and smart money activity are pointing in the same direction. This is the type of setup Apex Q is built to find."
        elif score >= 3:
            verdict = "APPROVE"
            confidence = f"MODERATE BUY SIGNAL. Intelligence score {score}/15. The data leans bullish across multiple indicators. Not a perfect setup but the weight of evidence favors the upside. Manage your position size appropriately."
        elif score <= -4:
            verdict = "PASS"
            confidence = f"HIGH CONVICTION AVOID. Intelligence score {score}/15. Multiple independent signals are negative. The data strongly suggests avoiding this position right now. Wait for a better setup."
        elif score <= -2:
            verdict = "PASS"
            confidence = f"CAUTION SIGNAL. Intelligence score {score}/15. More signals are negative than positive. The risk/reward does not favor entry at current levels. Monitor for improvement."
        else:
            verdict = "WATCH"
            confidence = f"MIXED SIGNALS. Intelligence score {score}/15. The data is not conclusive in either direction. Put this on your watchlist and wait for a catalyst that creates a clearer signal before acting."

        logger.info(f"ORCHESTRATOR: {symbol} verdict={verdict} score={score} signals={signals}")
        return verdict, confidence, reasons, score, signals

# Instantiate agents
market_agent = MarketDataAgent()
news_agent = NewsAgent()
regulatory_agent = RegulatoryAgent()
insider_agent = InsiderAgent()
orchestrator = Orchestrator()

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

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Apex Q Intelligence Terminal</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
  :root{--bg:#f0f4f8;--surface:#fff;--surface2:#e8edf2;--border:#c8d4e0;--accent:#0055cc;--green:#006b35;--green-bg:#e0f2ea;--red:#b30000;--red-bg:#fce8e8;--yellow:#8a5000;--yellow-bg:#fff3e0;--text:#0a1628;--muted:#4a6080;--card:#fff;--navy:#0a1628;}
  *{margin:0;padding:0;box-sizing:border-box;}
  body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',sans-serif;min-height:100vh;}
  /* TICKER */
  .ticker-bar{background:var(--navy);overflow:hidden;height:36px;display:flex;align-items:center;}
  .ticker-track{display:flex;animation:scroll 80s linear infinite;white-space:nowrap;}
  .ticker-track:hover{animation-play-state:paused;}
  .ticker-item{display:inline-flex;align-items:center;gap:8px;padding:0 20px;height:36px;font-family:'JetBrains Mono',monospace;font-size:11px;border-right:1px solid #1a2d4a;cursor:pointer;transition:background 0.2s;flex-shrink:0;}
  .ticker-item:hover{background:#1a2d4a;}
  .tsym{color:#fff;font-weight:700;font-size:12px;}
  .tprice{color:#7a9ab8;}
  .tup{color:#00e676;font-weight:600;}
  .tdown{color:#ff5252;font-weight:600;}
  @keyframes scroll{0%{transform:translateX(0);}100%{transform:translateX(-50%);}}
  /* HEADER */
  .header{background:var(--surface);border-bottom:2px solid var(--border);padding:14px 28px;display:flex;align-items:center;justify-content:space-between;}
  .logo{display:flex;align-items:center;gap:12px;}
  .logo-mark{width:40px;height:40px;background:linear-gradient(135deg,#0055cc,#003399);border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:20px;box-shadow:0 2px 8px rgba(0,85,204,0.3);}
  .logo-name{font-size:24px;font-weight:800;color:var(--text);letter-spacing:-0.5px;}
  .logo-name span{color:var(--accent);}
  .live-badge{display:flex;align-items:center;gap:6px;background:#e0f2ea;border:1px solid var(--green);border-radius:20px;padding:5px 12px;font-size:11px;color:var(--green);font-family:'JetBrains Mono',monospace;font-weight:700;}
  .live-dot{width:7px;height:7px;background:var(--green);border-radius:50%;animation:blink 1.5s infinite;}
  /* MARKET BAR */
  .market-bar{background:var(--surface);border-bottom:1px solid var(--border);display:flex;overflow-x:auto;scrollbar-width:none;}
  .market-bar::-webkit-scrollbar{display:none;}
  .mkt{padding:10px 22px;border-right:1px solid var(--border);cursor:pointer;transition:background 0.2s;min-width:140px;flex-shrink:0;}
  .mkt:hover{background:var(--surface2);}
  .mkt-label{font-size:9px;color:var(--muted);font-family:'JetBrains Mono',monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:3px;font-weight:700;}
  .mkt-val{font-size:13px;font-weight:700;font-family:'JetBrains Mono',monospace;}
  .mkt-val.up{color:var(--green);}
  .mkt-val.down{color:var(--red);}
  /* SEARCH */
  .search-wrap{background:var(--surface);border-bottom:1px solid var(--border);padding:18px 28px 14px;}
  .search-label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:2px;margin-bottom:10px;font-family:'JetBrains Mono',monospace;font-weight:700;}
  .search-row{display:flex;gap:10px;max-width:720px;position:relative;}
  .search-input{flex:1;background:var(--bg);border:2px solid var(--border);border-radius:10px;padding:14px 18px;color:var(--text);font-family:'Space Grotesk',sans-serif;font-size:15px;outline:none;transition:border-color 0.2s;}
  .search-input::placeholder{color:var(--muted);}
  .search-input:focus{border-color:var(--accent);background:#fff;}
  .search-btn{background:var(--accent);color:#fff;border:none;border-radius:10px;padding:14px 30px;font-family:'Space Grotesk',monospace;font-size:14px;font-weight:700;cursor:pointer;letter-spacing:0.5px;box-shadow:0 2px 8px rgba(0,85,204,0.3);}
  .search-btn:hover{background:#0044aa;}
  .ac{position:absolute;top:calc(100% + 4px);left:0;right:100px;background:#fff;border:2px solid var(--border);border-radius:10px;z-index:200;display:none;box-shadow:0 8px 24px rgba(0,0,0,0.12);}
  .ac-item{padding:11px 16px;cursor:pointer;font-size:13px;display:flex;gap:14px;align-items:center;border-bottom:1px solid var(--border);}
  .ac-item:last-child{border-bottom:none;}
  .ac-item:hover{background:var(--bg);}
  .ac-sym{font-family:'JetBrains Mono',monospace;color:var(--accent);font-weight:700;min-width:64px;}
  .ac-name{color:var(--muted);font-size:12px;}
  .quick-row{display:flex;gap:8px;margin-top:10px;flex-wrap:wrap;}
  .qpick{background:var(--bg);border:1px solid var(--border);border-radius:20px;padding:5px 14px;font-size:11px;color:var(--muted);cursor:pointer;transition:all 0.2s;font-family:'JetBrains Mono',monospace;font-weight:600;}
  .qpick:hover{border-color:var(--accent);color:var(--accent);background:#e6f0ff;}
  /* MAIN LAYOUT */
  .main{padding:20px 28px 60px;display:grid;grid-template-columns:1fr 330px;gap:22px;}
  @media(max-width:960px){.main{grid-template-columns:1fr;}}
  .sec-title{font-size:10px;text-transform:uppercase;letter-spacing:2px;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-bottom:12px;display:flex;align-items:center;gap:8px;font-weight:700;}
  .sec-title::after{content:'';flex:1;height:1px;background:var(--border);}
  /* REPORT CARD */
  .rcard{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:22px;margin-bottom:18px;box-shadow:0 2px 12px rgba(0,0,0,0.05);}
  .stock-hdr{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:20px;gap:12px;}
  .sname{font-size:30px;font-weight:800;color:var(--text);letter-spacing:-0.5px;}
  .sfull{font-size:13px;color:var(--muted);margin-top:3px;}
  .ssect{font-size:10px;color:var(--accent);margin-top:5px;font-family:'JetBrains Mono',monospace;font-weight:700;text-transform:uppercase;letter-spacing:1px;}
  .price-blk{text-align:right;flex-shrink:0;}
  .sprice{font-size:30px;font-weight:800;font-family:'JetBrains Mono',monospace;color:var(--text);}
  .schg{font-size:13px;font-family:'JetBrains Mono',monospace;margin-top:3px;font-weight:700;}
  .schg.up{color:var(--green);}
  .schg.down{color:var(--red);}
  /* VERDICT */
  .verdict-box{border-radius:14px;padding:22px;margin-bottom:20px;transition:all 0.3s;}
  .verdict-box.approve{background:linear-gradient(135deg,#e0f2ea,#c0e8d4);border:2px solid var(--green);}
  .verdict-box.pass{background:linear-gradient(135deg,#fce8e8,#f5c0c0);border:2px solid var(--red);}
  .verdict-box.watch{background:linear-gradient(135deg,#fff3e0,#ffe0b0);border:2px solid var(--yellow);}
  .verdict-top{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;flex-wrap:wrap;gap:10px;}
  .vbadge{font-size:20px;font-weight:900;font-family:'JetBrains Mono',monospace;letter-spacing:4px;padding:11px 28px;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,0.15);}
  .vbadge.approve{background:var(--green);color:#fff;}
  .vbadge.pass{background:var(--red);color:#fff;}
  .vbadge.watch{background:var(--yellow);color:#fff;}
  .vscore{font-size:12px;font-family:'JetBrains Mono',monospace;color:var(--muted);font-weight:700;background:rgba(255,255,255,0.7);padding:6px 12px;border-radius:20px;}
  .vconf{font-size:13px;color:var(--text);margin-bottom:16px;line-height:1.7;font-weight:500;background:rgba(255,255,255,0.6);padding:12px 16px;border-radius:8px;}
  .vreasons{display:flex;flex-direction:column;gap:8px;}
  .vreason{display:flex;align-items:flex-start;gap:12px;padding:11px 14px;background:rgba(255,255,255,0.85);border-radius:10px;transition:transform 0.2s;}
  .vreason:hover{transform:translateX(3px);}
  .vicon{font-size:18px;flex-shrink:0;margin-top:1px;}
  .vlabel{font-weight:700;display:block;margin-bottom:3px;color:var(--text);font-size:13px;}
  .vdetail{color:var(--muted);font-size:12px;line-height:1.5;}
  /* METRICS */
  .metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:18px;}
  .met{background:var(--surface2);border-radius:10px;padding:13px;border:1px solid var(--border);}
  .met-lbl{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:5px;font-family:'JetBrains Mono',monospace;font-weight:700;}
  .met-val{font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--text);}
  .met-val.pos{color:var(--green);}
  .met-val.neg{color:var(--red);}
  .met-val.neu{color:var(--accent);}
  /* INTEL CARDS */
  .icard{background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:10px;}
  .ihead{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;}
  .ititle{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);font-family:'JetBrains Mono',monospace;}
  .ibadge{font-size:10px;font-weight:700;padding:3px 10px;border-radius:20px;font-family:'JetBrains Mono',monospace;}
  .bg{background:var(--green-bg);color:var(--green);border:1px solid var(--green);}
  .br{background:var(--red-bg);color:var(--red);border:1px solid var(--red);}
  .by{background:var(--yellow-bg);color:var(--yellow);border:1px solid var(--yellow);}
  .itext{font-size:13px;color:var(--text);line-height:1.6;}
  .trade-row{padding:8px 0;border-bottom:1px solid var(--border);font-size:12px;display:flex;flex-wrap:wrap;gap:6px;align-items:center;}
  .trade-row:last-child{border-bottom:none;}
  .tbuy{color:var(--green);font-weight:700;font-family:'JetBrains Mono',monospace;}
  .tsell{color:var(--red);font-weight:700;font-family:'JetBrains Mono',monospace;}
  .tgray{color:var(--muted);font-size:11px;}
  /* NEWS */
  .news-item{padding:11px 0;border-bottom:1px solid var(--border);}
  .news-item:last-child{border-bottom:none;}
  .nsrc{font-size:10px;color:var(--accent);font-family:'JetBrains Mono',monospace;text-transform:uppercase;font-weight:700;margin-bottom:4px;}
  .nhd{font-size:13px;color:var(--text);line-height:1.5;font-weight:500;}
  .nsum{font-size:11px;color:var(--muted);margin-top:3px;line-height:1.4;}
  /* LOADING */
  .loading{display:none;text-align:center;padding:50px;color:var(--accent);font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;}
  .loading.active{display:block;}
  .loading-steps{display:flex;flex-direction:column;gap:6px;margin-top:16px;text-align:left;max-width:300px;margin:16px auto 0;}
  .lstep{font-size:11px;color:var(--muted);display:flex;align-items:center;gap:8px;}
  .lstep.active{color:var(--accent);}
  @keyframes blink{0%,100%{opacity:1;}50%{opacity:0.3;}}
  .live-dot,.loading{animation:blink 1.2s infinite;}
  /* SIGNALS PANEL */
  .sig-card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:14px;margin-bottom:10px;cursor:pointer;transition:all 0.2s;box-shadow:0 1px 4px rgba(0,0,0,0.04);}
  .sig-card:hover{border-color:var(--accent);box-shadow:0 4px 12px rgba(0,85,204,0.1);}
  .sig-top{display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;}
  .sig-sym{font-size:16px;font-weight:800;font-family:'JetBrains Mono',monospace;color:var(--text);}
  .sig-v{font-size:10px;font-weight:700;padding:3px 10px;border-radius:20px;font-family:'JetBrains Mono',monospace;}
  .va{background:var(--green-bg);color:var(--green);border:1px solid var(--green);}
  .vp{background:var(--red-bg);color:var(--red);border:1px solid var(--red);}
  .vw{background:var(--yellow-bg);color:var(--yellow);border:1px solid var(--yellow);}
  .sig-info{font-size:11px;font-family:'JetBrains Mono',monospace;color:var(--muted);}
  /* CONFLUENCE */
  .conf-alert{background:linear-gradient(135deg,#0a1628,#0d2340);border:2px solid var(--accent);border-radius:12px;padding:14px;margin-bottom:12px;}
  .conf-title{font-size:11px;font-weight:700;color:var(--accent);font-family:'JetBrains Mono',monospace;margin-bottom:5px;letter-spacing:1px;}
  .conf-text{font-size:12px;color:#a0b8cc;line-height:1.5;}
  /* FOOTER */
  .footer{background:var(--surface);border-top:1px solid var(--border);padding:20px 28px;text-align:center;font-size:10px;color:var(--muted);font-family:'JetBrains Mono',monospace;line-height:2;}
</style>
</head>
<body>

<div class="ticker-bar">
  <div class="ticker-track" id="tickerTrack">
    <span class="ticker-item"><span class="tsym">APEX Q</span><span class="tprice">Loading market data...</span></span>
  </div>
</div>

<div class="header">
  <div class="logo">
    <div class="logo-mark">&#9889;</div>
    <div class="logo-name">Apex <span>Q</span></div>
  </div>
  <div class="live-badge"><div class="live-dot"></div>LIVE INTEL ACTIVE</div>
</div>

<div class="market-bar">
  <div class="mkt" onclick="go('^GSPC')"><div class="mkt-label">S&amp;P 500</div><div class="mkt-val up" id="m0">Loading...</div></div>
  <div class="mkt" onclick="go('^IXIC')"><div class="mkt-label">NASDAQ</div><div class="mkt-val up" id="m1">Loading...</div></div>
  <div class="mkt" onclick="go('^DJI')"><div class="mkt-label">DOW JONES</div><div class="mkt-val up" id="m2">Loading...</div></div>
  <div class="mkt" onclick="go('^RUT')"><div class="mkt-label">RUSSELL 2000</div><div class="mkt-val up" id="m3">Loading...</div></div>
  <div class="mkt" onclick="go('^VIX')"><div class="mkt-label">VIX FEAR INDEX</div><div class="mkt-val" id="m4">Loading...</div></div>
  <div class="mkt" onclick="go('GC=F')"><div class="mkt-label">GOLD FUTURES</div><div class="mkt-val up" id="m5">Loading...</div></div>
  <div class="mkt" onclick="go('CL=F')"><div class="mkt-label">OIL WTI</div><div class="mkt-val" id="m6">Loading...</div></div>
  <div class="mkt" onclick="go('BTC-USD')"><div class="mkt-label">BITCOIN</div><div class="mkt-val up" id="m7">Loading...</div></div>
</div>

<div class="search-wrap">
  <div class="search-label">&#128269; Search any stock or company name</div>
  <div class="search-row">
    <input class="search-input" id="si" type="text" placeholder="Type a company or ticker... Apple, Tesla, SpaceX, SOFI, NVDA" autocomplete="off"/>
    <div class="ac" id="ac"></div>
    <button class="search-btn" onclick="run()">ANALYZE</button>
  </div>
  <div class="quick-row">
    <div class="qpick" onclick="go('SOFI')">SOFI</div>
    <div class="qpick" onclick="go('SPCX')">SpaceX</div>
    <div class="qpick" onclick="go('NVDA')">NVDA</div>
    <div class="qpick" onclick="go('AAPL')">Apple</div>
    <div class="qpick" onclick="go('AMD')">AMD</div>
    <div class="qpick" onclick="go('TSLA')">Tesla</div>
    <div class="qpick" onclick="go('MSFT')">Microsoft</div>
    <div class="qpick" onclick="go('AMZN')">Amazon</div>
    <div class="qpick" onclick="go('GOOGL')">Google</div>
    <div class="qpick" onclick="go('META')">Meta</div>
    <div class="qpick" onclick="go('JPM')">JPMorgan</div>
    <div class="qpick" onclick="go('SCHD')">SCHD</div>
  </div>
</div>

<div class="main">
  <div>
    <div class="sec-title">Full Intelligence Report</div>
    <div class="loading" id="loadBox">
      Running intelligence agents...
      <div class="loading-steps">
        <div class="lstep">&#128202; Analyst Agent — pulling price and fundamentals</div>
        <div class="lstep">&#127963; Regulatory Agent — checking congressional trades</div>
        <div class="lstep">&#128188; Insider Agent — scanning C-level activity</div>
        <div class="lstep">&#128240; News Agent — fetching Finnhub intelligence</div>
        <div class="lstep">&#9889; Synthesis Engine — calculating verdict</div>
      </div>
    </div>
    <div id="report" class="rcard">
      <div class="stock-hdr">
        <div>
          <div class="sname" id="sym">APEX Q</div>
          <div class="sfull" id="sname">Search a stock above to begin full multi-agent analysis</div>
          <div class="ssect" id="ssect"></div>
        </div>
        <div class="price-blk">
          <div class="sprice" id="sprice">--</div>
          <div class="schg up" id="schg">-- today</div>
        </div>
      </div>

      <div class="verdict-box watch" id="vbox">
        <div class="verdict-top">
          <div class="vbadge watch" id="vbadge">&#9889; READY</div>
          <div class="vscore" id="vscore">Intelligence Score: --</div>
        </div>
        <div class="vconf" id="vconf">Search any stock above. Apex Q runs four independent intelligence agents simultaneously and synthesizes all data into a single data-driven verdict with full plain English reasoning.</div>
        <div class="vreasons" id="vreasons">
          <div class="vreason"><span class="vicon">&#128202;</span><div><span class="vlabel">Analyst Agent</span><span class="vdetail">Price momentum, PE ratio, analyst consensus, and price target analysis</span></div></div>
          <div class="vreason"><span class="vicon">&#127963;</span><div><span class="vlabel">Regulatory Agent</span><span class="vdetail">Congressional trading patterns from Quiver Quantitative</span></div></div>
          <div class="vreason"><span class="vicon">&#128188;</span><div><span class="vlabel">Insider Agent</span><span class="vdetail">C-Level cluster buy and sell detection</span></div></div>
          <div class="vreason"><span class="vicon">&#128240;</span><div><span class="vlabel">News Agent</span><span class="vdetail">Live news intelligence from Finnhub</span></div></div>
        </div>
      </div>

      <div class="metrics">
        <div class="met"><div class="met-lbl">Current Price</div><div class="met-val neu" id="mp">--</div></div>
        <div class="met"><div class="met-lbl">Change Today</div><div class="met-val" id="mc">--</div></div>
        <div class="met"><div class="met-lbl">Intel Score</div><div class="met-val neu" id="ms">--</div></div>
        <div class="met"><div class="met-lbl">PE Ratio</div><div class="met-val" id="mpe">--</div></div>
        <div class="met"><div class="met-lbl">Analyst Target</div><div class="met-val pos" id="mt">--</div></div>
        <div class="met"><div class="met-lbl">Market Cap</div><div class="met-val neu" id="mm">--</div></div>
      </div>

      <div class="sec-title">&#127963; Congressional Trading Intelligence</div>
      <div id="congSec"><div class="icard"><div class="ihead"><div class="ititle">Quiver Quantitative</div><div class="ibadge by">WAITING</div></div><div class="itext">Congressional trading data will appear here after analysis.</div></div></div>

      <div class="sec-title">&#128188; Insider Activity Intelligence</div>
      <div id="insSec"><div class="icard"><div class="ihead"><div class="ititle">C-Level Insider Trades</div><div class="ibadge by">WAITING</div></div><div class="itext">Executive buy and sell activity will appear here after analysis.</div></div></div>

      <div class="sec-title">&#128240; Live News Intelligence</div>
      <div id="newsSec"><div class="icard"><div class="ihead"><div class="ititle">Finnhub News Feed</div><div class="ibadge by">WAITING</div></div><div class="itext">Live news feed will appear here after analysis.</div></div></div>
    </div>
  </div>

  <div>
    <div class="sec-title">Live Signals</div>
    <div id="panel">
      <div class="sig-card" onclick="go('SOFI')"><div class="sig-top"><div class="sig-sym">SOFI</div><div class="sig-v vw">WATCH</div></div><div class="sig-info">Click to run full analysis</div></div>
      <div class="sig-card" onclick="go('NVDA')"><div class="sig-top"><div class="sig-sym">NVDA</div><div class="sig-v vw">WATCH</div></div><div class="sig-info">Click to run full analysis</div></div>
      <div class="sig-card" onclick="go('SPCX')"><div class="sig-top"><div class="sig-sym">SPCX</div><div class="sig-v vw">WATCH</div></div><div class="sig-info">Click to run full analysis</div></div>
      <div class="sig-card" onclick="go('AMD')"><div class="sig-top"><div class="sig-sym">AMD</div><div class="sig-v vw">WATCH</div></div><div class="sig-info">Click to run full analysis</div></div>
      <div class="sig-card" onclick="go('TSLA')"><div class="sig-top"><div class="sig-sym">TSLA</div><div class="sig-v vw">WATCH</div></div><div class="sig-info">Click to run full analysis</div></div>
      <div class="sig-card" onclick="go('AAPL')"><div class="sig-top"><div class="sig-sym">AAPL</div><div class="sig-v vw">WATCH</div></div><div class="sig-info">Click to run full analysis</div></div>
      <div class="sig-card" onclick="go('MSFT')"><div class="sig-top"><div class="sig-sym">MSFT</div><div class="sig-v vw">WATCH</div></div><div class="sig-info">Click to run full analysis</div></div>
    </div>
  </div>
</div>

<div class="footer">
  APEX Q INTELLIGENCE TERMINAL &nbsp;|&nbsp; ANALYST AGENT &bull; REGULATORY AGENT &bull; INSIDER AGENT &bull; NEWS AGENT &bull; SYNTHESIS ENGINE<br>
  Powered by yFinance &bull; Finnhub &bull; Quiver Quantitative &bull; SEC EDGAR<br><br>
  The insights provided are generated by our analytical engine for educational and illustrative purposes only.<br>
  They are not intended as financial, investment, or legal advice. Every market participant is unique.<br>
  We encourage you to perform your own due diligence or consult with a qualified professional before making any financial decisions.
</div>

<script>
const API = window.location.origin;
const TICKERS = ['AAPL','MSFT','NVDA','AMD','TSLA','AMZN','GOOGL','META','SOFI','SPCX','SCHD','JPM','BAC','NFLX','BTC-USD','GC=F'];
const MKT = [
  {sym:'^GSPC',id:'m0'},{sym:'^IXIC',id:'m1'},{sym:'^DJI',id:'m2'},
  {sym:'^RUT',id:'m3'},{sym:'^VIX',id:'m4'},{sym:'GC=F',id:'m5'},
  {sym:'CL=F',id:'m6'},{sym:'BTC-USD',id:'m7'}
];

function fmt(n){
  if(!n||n==='N/A')return 'N/A';
  const x=parseFloat(n);if(isNaN(x))return'N/A';
  if(x>=1e12)return'$'+(x/1e12).toFixed(2)+'T';
  if(x>=1e9)return'$'+(x/1e9).toFixed(2)+'B';
  if(x>=1e6)return'$'+(x/1e6).toFixed(2)+'M';
  return'$'+x.toLocaleString();
}

function renderCongress(trades){
  const s=document.getElementById('congSec');
  if(!trades||!trades.length){
    s.innerHTML='<div class="icard"><div class="ihead"><div class="ititle">Congressional Trading</div><div class="ibadge bg">CLEAN</div></div><div class="itext">No recent congressional trading activity found for this stock in the Quiver Quantitative database.</div></div>';
    return;
  }
  const buys=trades.filter(t=>t.action&&t.action.toLowerCase().includes('purchase'));
  const sells=trades.filter(t=>t.action&&t.action.toLowerCase().includes('sale'));
  const bc=buys.length>sells.length?'bg':sells.length>buys.length?'br':'by';
  const bl=buys.length>sells.length?`BUYING (${buys.length})`:sells.length>buys.length?`SELLING (${sells.length})`:'MIXED';
  s.innerHTML=`<div class="icard"><div class="ihead"><div class="ititle">Congressional Trades — ${trades.length} total</div><div class="ibadge ${bc}">${bl}</div></div><div class="itext">${
    trades.map(t=>`<div class="trade-row"><span class="${t.action&&t.action.toLowerCase().includes('purchase')?'tbuy':'tsell'}">${t.action||'Unknown'}</span><span>${t.politician||'Unknown'}</span><span class="tgray">(${t.party||''})</span><span class="tgray">${t.amount||''}</span><span class="tgray">${t.date||''}</span></div>`).join('')
  }</div></div>`;
}

function renderInsider(trades){
  const s=document.getElementById('insSec');
  if(!trades||!trades.length){
    s.innerHTML='<div class="icard"><div class="ihead"><div class="ititle">Insider Activity</div><div class="ibadge bg">CLEAN</div></div><div class="itext">No recent insider trading filings detected. Clean insider slate — no unusual activity from executives or directors.</div></div>';
    return;
  }
  const buys=trades.filter(t=>t.action==='A');
  const sells=trades.filter(t=>t.action==='D');
  const cbuys=trades.filter(t=>t.is_clevel&&t.action==='A');
  const bc=buys.length>sells.length?'bg':sells.length>buys.length?'br':'by';
  let label=buys.length>sells.length?'BUYING':'SELLING';
  if(cbuys.length>=2)label='CLUSTER BUY &#9889;';
  s.innerHTML=`<div class="icard"><div class="ihead"><div class="ititle">Insider Trades — ${trades.length} filings</div><div class="ibadge ${bc}">${label}</div></div><div class="itext">${
    trades.slice(0,8).map(t=>`<div class="trade-row"><span class="${t.action==='A'?'tbuy':'tsell'}">${t.action==='A'?'BUY':'SELL'}</span><span>${t.name||'Unknown'}</span><span class="tgray">${t.title||''}</span>${t.shares?`<span class="tgray">${parseInt(t.shares).toLocaleString()} shares</span>`:''}<span class="tgray">${t.date||''}</span></div>`).join('')
  }</div></div>`;
}

function renderNews(news){
  const s=document.getElementById('newsSec');
  if(!news||!news.length){
    s.innerHTML='<div class="icard"><div class="ihead"><div class="ititle">Finnhub News Feed</div><div class="ibadge by">NO RESULTS</div></div><div class="itext">No recent news articles found for this stock in the last 30 days. This may indicate low media coverage or a very recently listed company.</div></div>';
    return;
  }
  s.innerHTML=news.map(n=>`<div class="news-item"><div class="nsrc">${n.source||'Market News'}</div><div class="nhd">${n.headline}</div>${n.summary?`<div class="nsum">${n.summary.slice(0,120)}...</div>`:''}</div>`).join('');
}

async function loadTicker(sym){
  try{
    const r=await fetch(`${API}/analyze?symbol=${encodeURIComponent(sym)}`);
    const d=await r.json();
    if(d.price){
      const up=d.change_pct>=0;
      return `<span class="ticker-item" onclick="go('${sym}')"><span class="tsym">${d.symbol}</span><span class="tprice">$${d.price.toLocaleString()}</span><span class="${up?'tup':'tdown'}">${up?'+':''}${d.change_pct}%</span></span>`;
    }
  }catch(e){}
  return'';
}

async function buildTicker(){
  const track=document.getElementById('tickerTrack');
  let html='';
  for(const s of TICKERS)html+=await loadTicker(s);
  if(html)track.innerHTML=html+html;
}

async function loadMarket(){
  for(const m of MKT){
    try{
      const r=await fetch(`${API}/analyze?symbol=${encodeURIComponent(m.sym)}`);
      const d=await r.json();
      if(d.price){
        const el=document.getElementById(m.id);
        if(el){
          el.textContent=d.price.toLocaleString()+' ('+(d.change_pct>=0?'+':'')+d.change_pct+'%)';
          el.className='mkt-val '+(d.change_pct>=0?'up':'down');
        }
      }
    }catch(e){}
  }
}

function go(sym){document.getElementById('si').value=sym;run();}

let acTimer;
document.getElementById('si').addEventListener('input',function(){
  clearTimeout(acTimer);
  const v=this.value.trim();
  if(v.length<2){document.getElementById('ac').style.display='none';return;}
  acTimer=setTimeout(()=>suggest(v),300);
});

async function suggest(q){
  try{
    const r=await fetch(`${API}/search?q=${encodeURIComponent(q)}`);
    const d=await r.json();
    const ac=document.getElementById('ac');
    if(d.results&&d.results.length){
      ac.innerHTML=d.results.map(x=>`<div class="ac-item" onclick="go('${x.symbol}')"><span class="ac-sym">${x.symbol}</span><span class="ac-name">${x.name||''}</span></div>`).join('');
      ac.style.display='block';
    }else ac.style.display='none';
  }catch(e){}
}

document.addEventListener('click',e=>{if(!e.target.closest('.search-row'))document.getElementById('ac').style.display='none';});
document.getElementById('si').addEventListener('keypress',e=>{if(e.key==='Enter')run();});

async function run(){
  const val=document.getElementById('si').value.trim();
  if(!val)return;
  document.getElementById('ac').style.display='none';
  document.getElementById('loadBox').classList.add('active');
  document.getElementById('report').style.opacity='0.35';

  try{
    const r=await fetch(`${API}/analyze?symbol=${encodeURIComponent(val)}`);
    const d=await r.json();

    if(d.error){
      document.getElementById('sym').textContent='NOT FOUND';
      document.getElementById('sname').textContent=d.error;
      document.getElementById('loadBox').classList.remove('active');
      document.getElementById('report').style.opacity='1';
      return;
    }

    document.getElementById('sym').textContent=d.symbol||val;
    document.getElementById('sname').textContent=d.name||val;
    document.getElementById('ssect').textContent=d.sector||'';
    document.getElementById('sprice').textContent='$'+(d.price||0).toLocaleString();
    document.getElementById('mp').textContent='$'+(d.price||0).toLocaleString();

    const chg=d.change_pct||0;
    const chgTxt=(chg>=0?'+':'')+chg+'%';
    document.getElementById('schg').textContent=chgTxt+' today';
    document.getElementById('schg').className='schg '+(chg>=0?'up':'down');
    document.getElementById('mc').textContent=chgTxt;
    document.getElementById('mc').className='met-val '+(chg>=0?'pos':'neg');

    document.getElementById('ms').textContent=(d.score||0)+'/15';
    document.getElementById('mpe').textContent=d.pe_ratio||'N/A';
    document.getElementById('mt').textContent=d.analyst_target?'$'+d.analyst_target:'N/A';
    document.getElementById('mm').textContent=fmt(d.market_cap);

    const v=d.verdict||'WATCH';
    const vbox=document.getElementById('vbox');
    const vbadge=document.getElementById('vbadge');
    vbox.className='verdict-box '+v.toLowerCase();
    vbadge.className='vbadge '+v.toLowerCase();
    const vi={APPROVE:'&#9989;',PASS:'&#10060;',WATCH:'&#9889;'};
    vbadge.innerHTML=vi[v]+' '+v;
    document.getElementById('vscore').textContent='Intelligence Score: '+(d.score||0)+'/15';
    document.getElementById('vconf').textContent=d.confidence||'';

    if(d.reasons&&d.reasons.length){
      document.getElementById('vreasons').innerHTML=d.reasons.map(r=>`<div class="vreason"><span class="vicon">${r.icon}</span><div><span class="vlabel">${r.label}</span><span class="vdetail">${r.detail}</span></div></div>`).join('');
    }

    renderCongress(d.congressional||[]);
    renderInsider(d.insider||[]);
    renderNews(d.news||[]);

    const vc=v==='APPROVE'?'va':v==='PASS'?'vp':'vw';
    const panel=document.getElementById('panel');
    const existing=panel.querySelector(`[data-s="${d.symbol}"]`);
    const card=`<div class="sig-card" data-s="${d.symbol}" onclick="go('${d.symbol}')">
      <div class="sig-top"><div class="sig-sym">${d.symbol}</div><div class="sig-v ${vc}">${v}</div></div>
      <div class="sig-info">$${d.price.toLocaleString()} &nbsp; Score: ${d.score}/15 &nbsp; ${d.name}</div>
    </div>`;
    if(existing)existing.outerHTML=card;
    else panel.insertAdjacentHTML('afterbegin',card);

  }catch(e){
    document.getElementById('sym').textContent='ERROR';
    document.getElementById('sname').textContent='Connection failed. Check your internet and try again.';
  }

  document.getElementById('loadBox').classList.remove('active');
  document.getElementById('report').style.opacity='1';
}

buildTicker();
loadMarket();
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
        return jsonify({"error": "No query"}), 400
    try:
        s = yf.Search(query, max_results=6)
        results = [{"symbol": q.get("symbol"), "name": q.get("longname") or q.get("shortname")} for q in s.quotes if q.get("symbol")]
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/analyze")
def analyze():
    query = request.args.get("symbol", "").strip()
    if not query:
        return jsonify({"error": "No symbol provided"}), 400

    symbol = resolve_ticker(query)
    logger.info(f"ANALYZE REQUEST: query={query} resolved={symbol}")

    market = market_agent.get(symbol)
    if not market:
        return jsonify({"error": f"No market data found for {symbol}. Please check the company name or ticker symbol."}), 404

    news = news_agent.get(symbol)
    congressional = regulatory_agent.get_congressional(symbol)
    insider = insider_agent.get(symbol)

    verdict, confidence, reasons, score, signals = orchestrator.synthesize(symbol, market, congressional, insider, news)

    logger.info(f"FINAL VERDICT: {symbol} = {verdict} score={score} signals={signals}")

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
        "volume": market.get("volume"),
        "beta": market.get("beta"),
        "dividend_yield": market.get("dividend_yield"),
        "news": news,
        "congressional": congressional,
        "insider": insider,
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
