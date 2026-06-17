from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import yfinance as yf
import requests
import os
import time
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

FINNHUB_KEY = os.environ.get("FINNHUB_KEY", "")
QUIVER_KEY = os.environ.get("QUIVER_KEY", "")

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
        quotes = s.quotes
        if quotes:
            return quotes[0].get("symbol", query.upper())
    except:
        pass
    return query.upper()

def fmt_price(val):
    try:
        return round(float(val), 2)
    except:
        return val

# ── AGENT 1: Market Data ──────────────────────────────────
class MarketDataAgent:
    def get(self, symbol):
        cached = get_cache(f"mkt_{symbol}")
        if cached:
            return cached
        try:
            t = yf.Ticker(symbol)
            hist = t.history(period="5d", timeout=10)
            info = t.info
            if hist.empty:
                return None
            cur = fmt_price(hist["Close"].iloc[-1])
            prev = fmt_price(hist["Close"].iloc[-2]) if len(hist) > 1 else cur
            chg = round(((cur - prev) / prev) * 100, 2)
            pe_raw = info.get("trailingPE")
            pe = round(float(pe_raw), 2) if pe_raw else "N/A"
            tgt_raw = info.get("targetMeanPrice")
            tgt = round(float(tgt_raw), 2) if tgt_raw else "N/A"
            res = {
                "price": cur,
                "change_pct": chg,
                "recommendation": info.get("recommendationKey", "hold").upper(),
                "name": info.get("longName", symbol),
                "sector": info.get("sector", ""),
                "pe_ratio": pe,
                "market_cap": info.get("marketCap", "N/A"),
                "analyst_target": tgt,
                "volume": int(hist["Volume"].iloc[-1]),
                "52w_high": fmt_price(info.get("fiftyTwoWeekHigh")),
                "52w_low": fmt_price(info.get("fiftyTwoWeekLow")),
                "beta": fmt_price(info.get("beta")),
                "dividend_yield": info.get("dividendYield", "N/A"),
            }
            set_cache(f"mkt_{symbol}", res)
            return res
        except Exception as e:
            logger.error(f"MarketAgent {symbol}: {e}")
            return None

# ── AGENT 2: News Agent ───────────────────────────────────
class NewsAgent:
    def get(self, symbol):
        cached = get_cache(f"news_{symbol}")
        if cached is not None:
            return cached
        if not FINNHUB_KEY:
            logger.warning("NewsAgent: FINNHUB_KEY missing")
            return []
        results = []
        # Try company-specific news first
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            from_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
            url = f"https://finnhub.io/api/v1/company-news?symbol={symbol}&from={from_date}&to={today}&token={FINNHUB_KEY}"
            r = requests.get(url, timeout=10)
            logger.info(f"NewsAgent company-news status={r.status_code} for {symbol}")
            if r.status_code == 200:
                for n in r.json()[:8]:
                    if n.get("headline"):
                        results.append({
                            "headline": n["headline"],
                            "source": n.get("source", "Market News"),
                            "summary": n.get("summary", "")[:200],
                            "url": n.get("url", ""),
                        })
        except Exception as e:
            logger.error(f"NewsAgent company news error: {e}")

        # If no results, pull general market news
        if not results:
            try:
                url2 = f"https://finnhub.io/api/v1/news?category=general&token={FINNHUB_KEY}"
                r2 = requests.get(url2, timeout=10)
                logger.info(f"NewsAgent general news status={r2.status_code}")
                if r2.status_code == 200:
                    for n in r2.json()[:5]:
                        if n.get("headline"):
                            results.append({
                                "headline": n["headline"],
                                "source": n.get("source", "Market News") + " (General Market)",
                                "summary": n.get("summary", "")[:200],
                                "url": n.get("url", ""),
                            })
            except Exception as e:
                logger.error(f"NewsAgent general news error: {e}")

        set_cache(f"news_{symbol}", results)
        logger.info(f"NewsAgent: {len(results)} articles for {symbol}")
        return results

# ── AGENT 3: Regulatory Agent ─────────────────────────────
class RegulatoryAgent:
    def get_congressional(self, symbol):
        cached = get_cache(f"cong_{symbol}")
        if cached is not None:
            return cached
        if not QUIVER_KEY:
            return []
        try:
            url = f"https://api.quiverquant.com/beta/historical/congresstrading/{symbol}"
            h = {"Authorization": f"Token {QUIVER_KEY}", "Accept": "application/json"}
            r = requests.get(url, headers=h, timeout=10)
            logger.info(f"RegulatoryAgent congress status={r.status_code} for {symbol}")
            if r.status_code == 200:
                res = []
                for t in r.json()[:10]:
                    res.append({
                        "politician": t.get("Representative", "Unknown"),
                        "party": t.get("Party", ""),
                        "action": t.get("Transaction", "Unknown"),
                        "amount": t.get("Range", ""),
                        "date": t.get("TransactionDate", ""),
                    })
                set_cache(f"cong_{symbol}", res)
                return res
        except Exception as e:
            logger.error(f"RegulatoryAgent: {e}")
        return []

# ── AGENT 4: Insider Agent ────────────────────────────────
class InsiderAgent:
    CLEVEL = ["CEO","CFO","COO","PRESIDENT","CHAIRMAN","CTO","CIO","DIRECTOR","FOUNDER","OWNER"]

    def get(self, symbol):
        cached = get_cache(f"ins_{symbol}")
        if cached is not None:
            return cached
        if not QUIVER_KEY:
            return []
        try:
            url = f"https://api.quiverquant.com/beta/historical/insiders/{symbol}"
            h = {"Authorization": f"Token {QUIVER_KEY}", "Accept": "application/json"}
            r = requests.get(url, headers=h, timeout=10)
            logger.info(f"InsiderAgent status={r.status_code} for {symbol}")
            if r.status_code == 200:
                res = []
                for t in r.json()[:15]:
                    title = str(t.get("Title","")).upper()
                    res.append({
                        "name": t.get("Name","Unknown"),
                        "title": t.get("Title",""),
                        "action": t.get("AcquiredDisposed",""),
                        "shares": t.get("Shares",0),
                        "price": fmt_price(t.get("Price",0)),
                        "date": t.get("Date",""),
                        "is_clevel": any(c in title for c in self.CLEVEL),
                    })
                set_cache(f"ins_{symbol}", res)
                return res
        except Exception as e:
            logger.error(f"InsiderAgent: {e}")
        return []

# ── ORCHESTRATOR ──────────────────────────────────────────
class Orchestrator:
    def synthesize(self, symbol, market, congressional, insider, news):
        score = 0
        reasons = []
        signals = []
        chg = market.get("change_pct", 0)
        rec = market.get("recommendation", "HOLD")
        tgt = market.get("analyst_target")
        price = market.get("price", 0)
        pe = market.get("pe_ratio", "N/A")

        # Price momentum
        if chg > 3:
            score += 3; signals.append("STRONG_MOMENTUM")
            reasons.append({"icon":"&#128200;","label":f"Strong Price Momentum: +{chg}%","detail":f"Up {chg}% today. Strong buying pressure. When a stock moves this much in one day buyers are firmly in control. This is a bullish signal for short term momentum traders."})
        elif chg > 1:
            score += 2; signals.append("POSITIVE_MOMENTUM")
            reasons.append({"icon":"&#128202;","label":f"Positive Price Action: +{chg}%","detail":f"Stock is up {chg}% today. Buyers are outnumbering sellers. Positive momentum means demand is exceeding supply at current prices."})
        elif chg > 0:
            score += 1
            reasons.append({"icon":"&#10145;","label":f"Slight Upward Drift: +{chg}%","detail":f"Up {chg}% today but no strong conviction. The market is slightly favoring buyers but not aggressively. Watch for a stronger move to confirm direction."})
        elif chg < -5:
            score -= 3; signals.append("HEAVY_SELLING")
            reasons.append({"icon":"&#128201;","label":f"Heavy Selling Pressure: {chg}%","detail":f"Down {abs(chg)}% today. This is significant single day selling. When a stock drops this sharply it means sellers are panicking or reacting to bad news. High risk entry point."})
        elif chg < -2:
            score -= 2; signals.append("SELLING_PRESSURE")
            reasons.append({"icon":"&#128201;","label":f"Significant Decline: {chg}%","detail":f"Down {abs(chg)}% today. Sellers are in control. This could be a buying opportunity on dip OR the start of a larger move down. Wait for the selling to stabilize before acting."})
        else:
            score -= 1
            reasons.append({"icon":"&#10145;","label":f"Minor Pullback: {chg}%","detail":f"Down {abs(chg)}% today. Minor selling pressure. Could be normal profit taking after a run or the early sign of a trend change. Monitor closely."})

        # Analyst consensus
        if rec in ["BUY","STRONG_BUY"]:
            score += 2; signals.append("ANALYST_BUY")
            reasons.append({"icon":"&#9989;","label":f"Wall Street Rating: {rec.replace('_',' ')}","detail":"Professional analysts who research this company full time rate it a Buy. These are the same analysts used by hedge funds and institutional investors. Their consensus matters."})
        elif rec in ["SELL","STRONG_SELL"]:
            score -= 2; signals.append("ANALYST_SELL")
            reasons.append({"icon":"&#9940;","label":f"Wall Street Rating: {rec.replace('_',' ')}","detail":"Professional analysts are negative on this stock right now. When the people who research a company full time say sell that is a serious signal worth respecting."})
        else:
            reasons.append({"icon":"&#9888;","label":"Wall Street Rating: Hold","detail":"Analysts are neutral. They do not see a compelling reason to buy or sell right now. This often means the stock is fairly valued or they are waiting for a catalyst."})

        # Price target
        if tgt and price and str(tgt) != "N/A":
            try:
                up = round(((float(tgt) - price) / price) * 100, 1)
                if up > 15:
                    score += 2
                    reasons.append({"icon":"&#127919;","label":f"{up}% Upside to Analyst Target: ${tgt}","detail":f"The average analyst price target is ${tgt}. At today's price of ${price} that represents {up}% potential upside. When targets are significantly above current price it signals analysts see undervaluation."})
                elif up > 5:
                    score += 1
                    reasons.append({"icon":"&#127919;","label":f"{up}% Upside to Target: ${tgt}","detail":f"Analyst consensus target ${tgt} is {up}% above current price ${price}. Modest but positive projected upside based on professional analysis."})
                elif up < -5:
                    score -= 1
                    reasons.append({"icon":"&#127919;","label":f"Trading {abs(up)}% Above Target","detail":f"Current price ${price} is ABOVE the analyst target of ${tgt}. This suggests the stock may be overvalued relative to what analysts think it is worth right now."})
                else:
                    reasons.append({"icon":"&#127919;","label":f"Near Analyst Target: ${tgt}","detail":f"Stock is trading close to the analyst consensus target of ${tgt}. Fairly valued at current levels based on professional projections."})
            except:
                pass

        # PE ratio
        if pe and pe != "N/A":
            try:
                pn = float(str(pe))
                if pn < 12:
                    score += 2
                    reasons.append({"icon":"&#128176;","label":f"PE {pn:.1f} — Deeply Undervalued","detail":f"A PE of {pn:.1f} means you are paying only {pn:.1f} dollars for every dollar the company earns. The average stock trades at 20x earnings. This looks cheap. Low PE can signal a hidden gem or a company in trouble. Dig deeper."})
                elif pn < 20:
                    score += 1
                    reasons.append({"icon":"&#128176;","label":f"PE {pn:.1f} — Reasonably Valued","detail":f"PE of {pn:.1f} is below or near the market average of around 20x. You are not overpaying for this stock relative to its earnings power. Solid valuation signal."})
                elif pn < 40:
                    reasons.append({"icon":"&#128203;","label":f"PE {pn:.1f} — Moderate Premium","detail":f"PE of {pn:.1f} is above average. Investors are paying a moderate premium. This is acceptable for a high growth company but requires the growth to actually materialize."})
                elif pn < 80:
                    score -= 1
                    reasons.append({"icon":"&#128184;","label":f"PE {pn:.1f} — High Valuation","detail":f"PE of {pn:.1f} means you are paying a significant premium for future growth. If growth slows the stock could fall sharply. High PE stocks carry more risk when market conditions change."})
                else:
                    score -= 2
                    reasons.append({"icon":"&#128184;","label":f"PE {pn:.1f} — Extreme Valuation","detail":f"PE of {pn:.1f} is very high. The company must deliver exceptional growth to justify this price. One earnings miss could cause a significant drop. Proceed with caution and manage position size carefully."})
            except:
                pass

        # Congressional
        if congressional:
            buys = [t for t in congressional if "purchase" in str(t.get("action","")).lower()]
            sells = [t for t in congressional if "sale" in str(t.get("action","")).lower()]
            if len(buys) >= 3:
                score += 3; signals.append("CONGRESS_CLUSTER_BUY")
                pols = ", ".join([b.get("politician","Unknown") for b in buys[:3]])
                reasons.append({"icon":"&#127963;","label":f"Congressional Cluster Buy: {len(buys)} politicians","detail":f"Multiple members of Congress recently bought this stock including {pols}. Under the STOCK Act politicians must disclose trades within 45 days. When multiple members buy the same stock at once it can signal positive regulatory or policy developments ahead."})
            elif buys:
                score += 2; signals.append("CONGRESS_BUYING")
                reasons.append({"icon":"&#127963;","label":f"Congressional Buying Detected","detail":f"{buys[0].get('politician','A politician')} recently purchased shares. Congressional members often have early access to regulatory and policy information. Their personal investment decisions can be a meaningful signal."})
            elif sells:
                score -= 1; signals.append("CONGRESS_SELLING")
                reasons.append({"icon":"&#127963;","label":f"Congressional Selling: {len(sells)} trades","detail":f"{len(sells)} congressional members recently sold this stock. This could signal concern about upcoming regulatory changes or simply routine portfolio management."})

        # Insider
        if insider:
            cb = [t for t in insider if t.get("is_clevel") and t.get("action")=="A"]
            cs = [t for t in insider if t.get("is_clevel") and t.get("action")=="D"]
            if len(cb) >= 3:
                score += 4; signals.append("INSIDER_CLUSTER_BUY")
                names = ", ".join([f"{t.get('name')} ({t.get('title')})" for t in cb[:3]])
                reasons.append({"icon":"&#128188;","label":"C-Level Cluster Buy — HIGHEST CONVICTION SIGNAL","detail":f"Multiple executives are buying with their own money: {names}. Insiders have only one reason to buy their own stock with personal funds: they believe it is going higher. Three or more executives buying simultaneously is the single strongest signal in Apex Q."})
            elif len(cb) == 2:
                score += 3; signals.append("INSIDER_CLUSTER_BUY")
                reasons.append({"icon":"&#128188;","label":f"Dual Executive Buy","detail":f"{cb[0].get('name')} ({cb[0].get('title')}) and {cb[1].get('name')} ({cb[1].get('title')}) both bought shares. When two or more executives buy simultaneously it shows strong internal confidence in the company's direction."})
            elif len(cb) == 1:
                score += 2; signals.append("INSIDER_BUY")
                reasons.append({"icon":"&#128188;","label":f"Executive Buy: {cb[0].get('title')}","detail":f"{cb[0].get('name')} recently purchased {int(cb[0].get('shares',0)):,} shares at ${cb[0].get('price')}. Executives see the company's financials before anyone else. When they buy with personal money they are putting their own wealth behind their conviction."})
            if len(cs) >= 2:
                score -= 2; signals.append("INSIDER_CLUSTER_SELL")
                reasons.append({"icon":"&#128188;","label":f"Executive Cluster Selling: {len(cs)} officers","detail":f"Multiple executives recently sold shares. Executives sell for many reasons including taxes, diversification, and personal needs. However heavy cluster selling can sometimes signal concern about near term performance."})

        # Confluence
        conf = [s for s in ["CONGRESS_BUYING","CONGRESS_CLUSTER_BUY","INSIDER_BUY","INSIDER_CLUSTER_BUY","ANALYST_BUY","STRONG_MOMENTUM","POSITIVE_MOMENTUM"] if s in signals]
        if len(conf) >= 3:
            score += 3
            reasons.append({"icon":"&#9889;","label":f"CONFLUENCE SIGNAL: {len(conf)} Layers Aligned","detail":f"Rare alignment across {len(conf)} independent intelligence sources: {', '.join(conf)}. Confluence is when multiple unrelated data sources all point in the same direction. This is the highest conviction setup Apex Q can identify."})

        # Verdict
        if score >= 6:
            v = "APPROVE"
            conf_txt = f"HIGH CONVICTION BUY. Score {score}/15. Multiple independent intelligence layers confirm bullish setup. Price momentum, analyst consensus, and smart money activity are all pointing the same direction. This is exactly the type of confluence Apex Q is built to find."
        elif score >= 3:
            v = "APPROVE"
            conf_txt = f"MODERATE BUY SIGNAL. Score {score}/15. The weight of evidence leans bullish. Not a perfect setup but more signals favor upside than downside. Manage position size appropriately and set a clear stop loss."
        elif score <= -5:
            v = "PASS"
            conf_txt = f"HIGH CONVICTION AVOID. Score {score}/15. Multiple signals are clearly negative. The data strongly suggests avoiding this position right now and waiting for conditions to improve."
        elif score <= -2:
            v = "PASS"
            conf_txt = f"CAUTION SIGNAL. Score {score}/15. More signals are negative than positive. The risk/reward does not favor entry at current levels. Monitor for improvement before considering a position."
        else:
            v = "WATCH"
            conf_txt = f"MIXED SIGNALS. Score {score}/15. The data is not conclusive in either direction. Add to your watchlist and wait for a catalyst that creates a clearer signal before acting."

        logger.info(f"Orchestrator: {symbol} verdict={v} score={score} signals={signals}")
        return v, conf_txt, reasons, score, signals

market_agent = MarketDataAgent()
news_agent = NewsAgent()
reg_agent = RegulatoryAgent()
ins_agent = InsiderAgent()
orch = Orchestrator()

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Apex Q Intelligence Terminal</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');
:root{
  --bg:#eef2f7;--surface:#fff;--s2:#e4eaf2;--border:#ccd8e8;
  --accent:#0052cc;--green:#006830;--gbg:#dff2e9;
  --red:#b00000;--rbg:#fde8e8;--yellow:#7a4500;--ybg:#fff4e0;
  --text:#08192e;--muted:#4a6282;--navy:#08192e;
}
*{margin:0;padding:0;box-sizing:border-box;}
body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',sans-serif;}

/* TICKER */
.tkbar{background:var(--navy);height:38px;overflow:hidden;display:flex;align-items:center;}
.tktrack{display:flex;animation:scroll 90s linear infinite;white-space:nowrap;}
.tktrack:hover{animation-play-state:paused;}
.tki{display:inline-flex;align-items:center;gap:9px;padding:0 22px;height:38px;font-family:'JetBrains Mono',monospace;font-size:11.5px;border-right:1px solid #162840;cursor:pointer;flex-shrink:0;transition:background .2s;}
.tki:hover{background:#162840;}
.tsym{color:#fff;font-weight:700;}
.tpx{color:#6a8aaa;}
.tup{color:#00e676;font-weight:600;}
.tdn{color:#ff5252;font-weight:600;}
@keyframes scroll{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}

/* HEADER */
.hdr{background:var(--surface);border-bottom:2px solid var(--border);padding:14px 28px;display:flex;align-items:center;justify-content:space-between;}
.hlogo{display:flex;align-items:center;gap:13px;}
.hmark{width:42px;height:42px;background:linear-gradient(135deg,#0052cc,#003399);border-radius:11px;display:flex;align-items:center;justify-content:center;font-size:22px;box-shadow:0 3px 10px rgba(0,82,204,.25);}
.hname{font-size:25px;font-weight:800;letter-spacing:-.5px;}
.hname span{color:var(--accent);}
.hbadge{display:flex;align-items:center;gap:7px;background:var(--gbg);border:1px solid var(--green);border-radius:20px;padding:6px 14px;font-size:11px;color:var(--green);font-family:'JetBrains Mono',monospace;font-weight:700;}
.hdot{width:7px;height:7px;background:var(--green);border-radius:50%;animation:pulse 1.4s infinite;}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}

/* MARKET BAR */
.mbar{background:var(--surface);border-bottom:1px solid var(--border);display:flex;overflow-x:auto;scrollbar-width:none;}
.mbar::-webkit-scrollbar{display:none;}
.mi{padding:10px 22px;border-right:1px solid var(--border);cursor:pointer;transition:background .2s;min-width:148px;flex-shrink:0;}
.mi:hover{background:var(--s2);}
.ml{font-size:9px;color:var(--muted);font-family:'JetBrains Mono',monospace;text-transform:uppercase;letter-spacing:1.2px;margin-bottom:4px;font-weight:700;}
.mv{font-size:13px;font-weight:700;font-family:'JetBrains Mono',monospace;}
.mv.up{color:var(--green);}
.mv.dn{color:var(--red);}
.mv.ld{color:var(--muted);font-size:11px;}

/* SEARCH */
.swrap{background:var(--surface);border-bottom:1px solid var(--border);padding:18px 28px 14px;}
.slbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:2px;margin-bottom:11px;font-family:'JetBrains Mono',monospace;font-weight:700;}
.srow{display:flex;gap:10px;max-width:740px;position:relative;}
.sinp{flex:1;background:var(--bg);border:2px solid var(--border);border-radius:11px;padding:14px 18px;color:var(--text);font-family:'Space Grotesk',sans-serif;font-size:15px;outline:none;transition:border-color .2s;}
.sinp::placeholder{color:var(--muted);}
.sinp:focus{border-color:var(--accent);background:#fff;}
.sbtn{background:var(--accent);color:#fff;border:none;border-radius:11px;padding:14px 32px;font-family:'Space Grotesk',sans-serif;font-size:14px;font-weight:700;cursor:pointer;box-shadow:0 2px 8px rgba(0,82,204,.3);}
.sbtn:hover{background:#0044aa;}
.ac{position:absolute;top:calc(100% + 5px);left:0;right:100px;background:#fff;border:2px solid var(--border);border-radius:11px;z-index:300;display:none;box-shadow:0 8px 28px rgba(0,0,0,.12);}
.aci{padding:11px 17px;cursor:pointer;font-size:13px;display:flex;gap:14px;align-items:center;border-bottom:1px solid var(--border);}
.aci:last-child{border-bottom:none;}
.aci:hover{background:var(--bg);}
.acs{font-family:'JetBrains Mono',monospace;color:var(--accent);font-weight:700;min-width:66px;}
.acn{color:var(--muted);font-size:12px;}
.qrow{display:flex;gap:8px;margin-top:11px;flex-wrap:wrap;}
.qp{background:var(--bg);border:1px solid var(--border);border-radius:20px;padding:5px 15px;font-size:11px;color:var(--muted);cursor:pointer;transition:all .2s;font-family:'JetBrains Mono',monospace;font-weight:600;}
.qp:hover{border-color:var(--accent);color:var(--accent);background:#e6f0ff;}

/* MAIN */
.main{padding:22px 28px 70px;display:grid;grid-template-columns:1fr 340px;gap:22px;}
@media(max-width:980px){.main{grid-template-columns:1fr;}}
.stitle{font-size:10px;text-transform:uppercase;letter-spacing:2px;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-bottom:13px;display:flex;align-items:center;gap:9px;font-weight:700;}
.stitle::after{content:'';flex:1;height:1px;background:var(--border);}

/* REPORT */
.rc{background:var(--surface);border:1px solid var(--border);border-radius:15px;padding:24px;margin-bottom:18px;box-shadow:0 2px 14px rgba(0,0,0,.05);}
.shdr{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:22px;gap:14px;}
.sn{font-size:32px;font-weight:800;letter-spacing:-.5px;}
.sf{font-size:13px;color:var(--muted);margin-top:4px;}
.ss{font-size:10px;color:var(--accent);margin-top:5px;font-family:'JetBrains Mono',monospace;font-weight:700;text-transform:uppercase;letter-spacing:1px;}
.pb{text-align:right;flex-shrink:0;}
.sp{font-size:32px;font-weight:800;font-family:'JetBrains Mono',monospace;}
.sc{font-size:14px;font-family:'JetBrains Mono',monospace;margin-top:3px;font-weight:700;}
.sc.up{color:var(--green);}
.sc.dn{color:var(--red);}

/* VERDICT */
.vb{border-radius:15px;padding:24px;margin-bottom:22px;transition:all .3s;}
.vb.approve{background:linear-gradient(135deg,#dff2e9,#b8e6cc);border:2px solid var(--green);}
.vb.pass{background:linear-gradient(135deg,#fde8e8,#f5b8b8);border:2px solid var(--red);}
.vb.watch{background:linear-gradient(135deg,#fff4e0,#ffd9a0);border:2px solid var(--yellow);}
.vtop{display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:10px;}
.vbdg{font-size:19px;font-weight:900;font-family:'JetBrains Mono',monospace;letter-spacing:4px;padding:12px 30px;border-radius:11px;box-shadow:0 2px 10px rgba(0,0,0,.15);}
.vbdg.approve{background:var(--green);color:#fff;}
.vbdg.pass{background:var(--red);color:#fff;}
.vbdg.watch{background:var(--yellow);color:#fff;}
.vsco{font-size:12px;font-family:'JetBrains Mono',monospace;color:var(--muted);font-weight:700;background:rgba(255,255,255,.75);padding:7px 13px;border-radius:20px;}
.vconf{font-size:13px;color:var(--text);margin-bottom:18px;line-height:1.75;font-weight:500;background:rgba(255,255,255,.65);padding:14px 18px;border-radius:10px;}
.vrlist{display:flex;flex-direction:column;gap:9px;}
.vr{display:flex;align-items:flex-start;gap:13px;padding:12px 16px;background:rgba(255,255,255,.88);border-radius:11px;transition:transform .2s;}
.vr:hover{transform:translateX(4px);}
.vi{font-size:20px;flex-shrink:0;margin-top:1px;}
.vlbl{font-weight:700;display:block;margin-bottom:3px;color:var(--text);font-size:13px;}
.vdt{color:var(--muted);font-size:12px;line-height:1.55;}

/* METRICS */
.mets{display:grid;grid-template-columns:repeat(3,1fr);gap:11px;margin-bottom:20px;}
.met{background:var(--s2);border-radius:11px;padding:14px;border:1px solid var(--border);}
.ml2{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:1.2px;margin-bottom:5px;font-family:'JetBrains Mono',monospace;font-weight:700;}
.mv2{font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--text);}
.mv2.pos{color:var(--green);}
.mv2.neg{color:var(--red);}
.mv2.neu{color:var(--accent);}

/* INTEL */
.ic{background:var(--s2);border:1px solid var(--border);border-radius:11px;padding:15px;margin-bottom:10px;}
.ih{display:flex;align-items:center;justify-content:space-between;margin-bottom:11px;}
.it{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);font-family:'JetBrains Mono',monospace;}
.ibdg{font-size:10px;font-weight:700;padding:3px 11px;border-radius:20px;font-family:'JetBrains Mono',monospace;}
.bg{background:var(--gbg);color:var(--green);border:1px solid var(--green);}
.br{background:var(--rbg);color:var(--red);border:1px solid var(--red);}
.by{background:var(--ybg);color:var(--yellow);border:1px solid var(--yellow);}
.itxt{font-size:13px;color:var(--text);line-height:1.6;}
.tr{padding:8px 0;border-bottom:1px solid var(--border);font-size:12px;display:flex;flex-wrap:wrap;gap:7px;align-items:center;}
.tr:last-child{border-bottom:none;}
.buy{color:var(--green);font-weight:700;font-family:'JetBrains Mono',monospace;}
.sell{color:var(--red);font-weight:700;font-family:'JetBrains Mono',monospace;}
.gray{color:var(--muted);font-size:11px;}

/* NEWS */
.ni{padding:12px 0;border-bottom:1px solid var(--border);}
.ni:last-child{border-bottom:none;}
.ns{font-size:10px;color:var(--accent);font-family:'JetBrains Mono',monospace;text-transform:uppercase;font-weight:700;margin-bottom:4px;}
.nh{font-size:13px;color:var(--text);line-height:1.55;font-weight:500;}
.nsum{font-size:11px;color:var(--muted);margin-top:3px;line-height:1.4;}

/* LOADING */
.loading{display:none;padding:50px 30px;text-align:center;color:var(--accent);font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:700;}
.loading.on{display:block;animation:pulse 1.2s infinite;}
.lsteps{margin-top:18px;display:flex;flex-direction:column;gap:7px;max-width:320px;margin:18px auto 0;text-align:left;}
.ls{font-size:11px;color:var(--muted);display:flex;align-items:center;gap:9px;}

/* SIGNAL CARDS */
.sg{background:var(--surface);border:1px solid var(--border);border-radius:13px;padding:15px;margin-bottom:10px;cursor:pointer;transition:all .2s;box-shadow:0 1px 5px rgba(0,0,0,.04);}
.sg:hover{border-color:var(--accent);box-shadow:0 4px 14px rgba(0,82,204,.1);}
.sgtop{display:flex;align-items:center;justify-content:space-between;margin-bottom:7px;}
.sgsym{font-size:17px;font-weight:800;font-family:'JetBrains Mono',monospace;}
.sgv{font-size:10px;font-weight:700;padding:3px 11px;border-radius:20px;font-family:'JetBrains Mono',monospace;}
.va{background:var(--gbg);color:var(--green);border:1px solid var(--green);}
.vp{background:var(--rbg);color:var(--red);border:1px solid var(--red);}
.vw{background:var(--ybg);color:var(--yellow);border:1px solid var(--yellow);}
.sgi{font-size:11px;font-family:'JetBrains Mono',monospace;color:var(--muted);}

/* FOOTER */
.foot{background:var(--surface);border-top:1px solid var(--border);padding:22px 28px;text-align:center;font-size:10px;color:var(--muted);font-family:'JetBrains Mono',monospace;line-height:2.2;}
</style>
</head>
<body>

<div class="tkbar"><div class="tktrack" id="tktrack"><span class="tki"><span class="tsym">APEX Q</span><span class="tpx">Loading live data...</span></span></div></div>

<div class="hdr">
  <div class="hlogo"><div class="hmark">&#9889;</div><div class="hname">Apex <span>Q</span></div></div>
  <div class="hbadge"><div class="hdot"></div>LIVE INTEL ACTIVE</div>
</div>

<div class="mbar">
  <div class="mi" onclick="go('^GSPC')"><div class="ml">S&amp;P 500</div><div class="mv ld" id="m0">Loading...</div></div>
  <div class="mi" onclick="go('^IXIC')"><div class="ml">NASDAQ</div><div class="mv ld" id="m1">Loading...</div></div>
  <div class="mi" onclick="go('^DJI')"><div class="ml">DOW JONES</div><div class="mv ld" id="m2">Loading...</div></div>
  <div class="mi" onclick="go('^RUT')"><div class="ml">RUSSELL 2000</div><div class="mv ld" id="m3">Loading...</div></div>
  <div class="mi" onclick="go('^VIX')"><div class="ml">VIX FEAR</div><div class="mv ld" id="m4">Loading...</div></div>
  <div class="mi" onclick="go('GC=F')"><div class="ml">GOLD FUTURES</div><div class="mv ld" id="m5">Loading...</div></div>
  <div class="mi" onclick="go('CL=F')"><div class="ml">OIL WTI</div><div class="mv ld" id="m6">Loading...</div></div>
  <div class="mi" onclick="go('BTC-USD')"><div class="ml">BITCOIN</div><div class="mv ld" id="m7">Loading...</div></div>
</div>

<div class="swrap">
  <div class="slbl">&#128269; Search any stock or company name</div>
  <div class="srow">
    <input class="sinp" id="si" type="text" placeholder="Type a company or ticker... Apple, Tesla, SpaceX, SOFI, NVDA" autocomplete="off"/>
    <div class="ac" id="ac"></div>
    <button class="sbtn" onclick="run()">ANALYZE</button>
  </div>
  <div class="qrow">
    <div class="qp" onclick="go('SOFI')">SOFI</div>
    <div class="qp" onclick="go('SPCX')">SpaceX</div>
    <div class="qp" onclick="go('NVDA')">NVDA</div>
    <div class="qp" onclick="go('AAPL')">Apple</div>
    <div class="qp" onclick="go('AMD')">AMD</div>
    <div class="qp" onclick="go('TSLA')">Tesla</div>
    <div class="qp" onclick="go('MSFT')">Microsoft</div>
    <div class="qp" onclick="go('AMZN')">Amazon</div>
    <div class="qp" onclick="go('GOOGL')">Google</div>
    <div class="qp" onclick="go('META')">Meta</div>
    <div class="qp" onclick="go('JPM')">JPMorgan</div>
    <div class="qp" onclick="go('SCHD')">SCHD</div>
  </div>
</div>

<div class="main">
  <div>
    <div class="stitle">Full Intelligence Report</div>
    <div class="loading" id="lb">
      Running 4 intelligence agents simultaneously...
      <div class="lsteps">
        <div class="ls">&#128202; Analyst Agent — price, fundamentals, valuation</div>
        <div class="ls">&#127963; Regulatory Agent — congressional trades</div>
        <div class="ls">&#128188; Insider Agent — C-level buy/sell activity</div>
        <div class="ls">&#128240; News Agent — Finnhub live intelligence</div>
        <div class="ls">&#9889; Synthesis Engine — calculating verdict</div>
      </div>
    </div>
    <div id="rpt" class="rc">
      <div class="shdr">
        <div>
          <div class="sn" id="sym">APEX Q</div>
          <div class="sf" id="sfull">Search a stock above to begin full multi-agent analysis</div>
          <div class="ss" id="ssect"></div>
        </div>
        <div class="pb">
          <div class="sp" id="spx">--</div>
          <div class="sc up" id="schg">--</div>
        </div>
      </div>

      <div class="vb watch" id="vbox">
        <div class="vtop">
          <div class="vbdg watch" id="vbdg">&#9889; READY</div>
          <div class="vsco" id="vsco">Score: --</div>
        </div>
        <div class="vconf" id="vconf">Search any stock or company name above. Apex Q runs four independent intelligence agents and synthesizes all data into a single clear verdict with plain English reasoning designed for every level of investor.</div>
        <div class="vrlist" id="vrl">
          <div class="vr"><span class="vi">&#128202;</span><div><span class="vlbl">Analyst Agent</span><span class="vdt">Pulls price momentum, PE ratio, analyst consensus rating, and price target upside</span></div></div>
          <div class="vr"><span class="vi">&#127963;</span><div><span class="vlbl">Regulatory Agent</span><span class="vdt">Monitors congressional stock trades via Quiver Quantitative STOCK Act disclosures</span></div></div>
          <div class="vr"><span class="vi">&#128188;</span><div><span class="vlbl">Insider Agent</span><span class="vdt">Tracks C-Level executive buy and sell filings. Cluster buys are the strongest signal</span></div></div>
          <div class="vr"><span class="vi">&#128240;</span><div><span class="vlbl">News Agent</span><span class="vdt">Live company news from Finnhub covering the last 60 days of coverage</span></div></div>
        </div>
      </div>

      <div class="mets">
        <div class="met"><div class="ml2">Current Price</div><div class="mv2 neu" id="mp">--</div></div>
        <div class="met"><div class="ml2">Change Today</div><div class="mv2" id="mc">--</div></div>
        <div class="met"><div class="ml2">Intel Score</div><div class="mv2 neu" id="ms">--</div></div>
        <div class="met"><div class="ml2">PE Ratio</div><div class="mv2" id="mpe">--</div></div>
        <div class="met"><div class="ml2">Analyst Target</div><div class="mv2 pos" id="mt">--</div></div>
        <div class="met"><div class="ml2">Market Cap</div><div class="mv2 neu" id="mm">--</div></div>
      </div>

      <div class="stitle">&#127963; Congressional Trading</div>
      <div id="cong"><div class="ic"><div class="ih"><div class="it">Quiver Quantitative</div><div class="ibdg by">WAITING</div></div><div class="itxt">Congressional trading data loads after analysis.</div></div></div>

      <div class="stitle">&#128188; Insider Activity</div>
      <div id="ins"><div class="ic"><div class="ih"><div class="it">C-Level Insider Trades</div><div class="ibdg by">WAITING</div></div><div class="itxt">Executive buy and sell activity loads after analysis.</div></div></div>

      <div class="stitle">&#128240; Live News Intelligence</div>
      <div id="news"><div class="ic"><div class="ih"><div class="it">Finnhub News Feed</div><div class="ibdg by">WAITING</div></div><div class="itxt">Live news feed loads after analysis.</div></div></div>
    </div>
  </div>

  <div>
    <div class="stitle">Live Signals</div>
    <div id="panel">
      <div class="sg" onclick="go('SOFI')"><div class="sgtop"><div class="sgsym">SOFI</div><div class="sgv vw">WATCH</div></div><div class="sgi">Click to run full analysis</div></div>
      <div class="sg" onclick="go('NVDA')"><div class="sgtop"><div class="sgsym">NVDA</div><div class="sgv vw">WATCH</div></div><div class="sgi">Click to run full analysis</div></div>
      <div class="sg" onclick="go('SPCX')"><div class="sgtop"><div class="sgsym">SPCX</div><div class="sgv vw">WATCH</div></div><div class="sgi">Click to run full analysis</div></div>
      <div class="sg" onclick="go('AMD')"><div class="sgtop"><div class="sgsym">AMD</div><div class="sgv vw">WATCH</div></div><div class="sgi">Click to run full analysis</div></div>
      <div class="sg" onclick="go('TSLA')"><div class="sgtop"><div class="sgsym">TSLA</div><div class="sgv vw">WATCH</div></div><div class="sgi">Click to run full analysis</div></div>
      <div class="sg" onclick="go('AAPL')"><div class="sgtop"><div class="sgsym">AAPL</div><div class="sgv vw">WATCH</div></div><div class="sgi">Click to run full analysis</div></div>
      <div class="sg" onclick="go('MSFT')"><div class="sgtop"><div class="sgsym">MSFT</div><div class="sgv vw">WATCH</div></div><div class="sgi">Click to run full analysis</div></div>
    </div>
  </div>
</div>

<div class="foot">
  APEX Q INTELLIGENCE TERMINAL &nbsp;|&nbsp; ANALYST AGENT &bull; REGULATORY AGENT &bull; INSIDER AGENT &bull; NEWS AGENT &bull; SYNTHESIS ENGINE<br>
  Powered by yFinance &bull; Finnhub &bull; Quiver Quantitative &bull; SEC EDGAR<br><br>
  The insights provided are generated by our analytical engine for educational and illustrative purposes only.<br>
  They are not intended as financial, investment, or legal advice. Every market participant is unique.<br>
  We encourage you to perform your own due diligence or consult with a qualified professional before making any financial decisions.
</div>

<script>
const A=window.location.origin;
const TKS=['AAPL','MSFT','NVDA','AMD','TSLA','AMZN','GOOGL','META','SOFI','SPCX','SCHD','JPM','BAC','NFLX','BTC-USD'];
const MKT=[{s:'^GSPC',id:'m0'},{s:'^IXIC',id:'m1'},{s:'^DJI',id:'m2'},{s:'^RUT',id:'m3'},{s:'^VIX',id:'m4'},{s:'GC=F',id:'m5'},{s:'CL=F',id:'m6'},{s:'BTC-USD',id:'m7'}];

function fmt(n){
  if(!n||n==='N/A')return'N/A';
  const x=parseFloat(n);if(isNaN(x))return'N/A';
  if(x>=1e12)return'$'+(x/1e12).toFixed(2)+'T';
  if(x>=1e9)return'$'+(x/1e9).toFixed(2)+'B';
  if(x>=1e6)return'$'+(x/1e6).toFixed(2)+'M';
  return'$'+x.toLocaleString();
}

function renderCong(data){
  const s=document.getElementById('cong');
  if(!data||!data.length){
    s.innerHTML='<div class="ic"><div class="ih"><div class="it">Congressional Trading</div><div class="ibdg bg">CLEAN</div></div><div class="itxt">No recent congressional trading activity found for this stock. This means no politicians have publicly disclosed trades in this company recently.</div></div>';
    return;
  }
  const buys=data.filter(t=>t.action&&t.action.toLowerCase().includes('purchase'));
  const sells=data.filter(t=>t.action&&t.action.toLowerCase().includes('sale'));
  const bc=buys.length>sells.length?'bg':sells.length>buys.length?'br':'by';
  const bl=buys.length>sells.length?`BUYING (${buys.length})`:sells.length>buys.length?`SELLING (${sells.length})`:'MIXED';
  s.innerHTML=`<div class="ic"><div class="ih"><div class="it">Congressional Trades — ${data.length} total</div><div class="ibdg ${bc}">${bl}</div></div><div class="itxt">${data.map(t=>`<div class="tr"><span class="${t.action&&t.action.toLowerCase().includes('purchase')?'buy':'sell'}">${t.action||'Unknown'}</span><span>${t.politician||'Unknown'}</span><span class="gray">(${t.party||''})</span><span class="gray">${t.amount||''}</span><span class="gray">${t.date||''}</span></div>`).join('')}</div></div>`;
}

function renderIns(data){
  const s=document.getElementById('ins');
  if(!data||!data.length){
    s.innerHTML='<div class="ic"><div class="ih"><div class="it">Insider Activity</div><div class="ibdg bg">CLEAN</div></div><div class="itxt">No recent insider trading filings detected. A clean insider slate means executives and directors are not making unusual moves with their personal holdings right now.</div></div>';
    return;
  }
  const buys=data.filter(t=>t.action==='A');
  const sells=data.filter(t=>t.action==='D');
  const cb=data.filter(t=>t.is_clevel&&t.action==='A');
  const bc=buys.length>sells.length?'bg':sells.length>buys.length?'br':'by';
  let lbl=buys.length>sells.length?'BUYING':'SELLING';
  if(cb.length>=2)lbl='CLUSTER BUY &#9889;';
  s.innerHTML=`<div class="ic"><div class="ih"><div class="it">Insider Trades — ${data.length} filings</div><div class="ibdg ${bc}">${lbl}</div></div><div class="itxt">${data.slice(0,8).map(t=>`<div class="tr"><span class="${t.action==='A'?'buy':'sell'}">${t.action==='A'?'BUY':'SELL'}</span><span>${t.name||'Unknown'}</span><span class="gray">${t.title||''}</span>${t.shares?`<span class="gray">${parseInt(t.shares).toLocaleString()} shares</span>`:''}<span class="gray">${t.date||''}</span></div>`).join('')}</div></div>`;
}

function renderNews(data){
  const s=document.getElementById('news');
  if(!data||!data.length){
    s.innerHTML='<div class="ic"><div class="ih"><div class="it">Finnhub News Feed</div><div class="ibdg by">NO RESULTS</div></div><div class="itxt">No news articles found in the last 60 days for this stock. This may mean low media coverage, a very new listing, or the company is between major announcements.</div></div>';
    return;
  }
  const isGeneral=data.some(n=>n.source&&n.source.includes('General Market'));
  s.innerHTML=(isGeneral?'<div class="ic" style="margin-bottom:10px"><div class="ih"><div class="it">Market News</div><div class="ibdg by">GENERAL</div></div><div class="itxt" style="font-size:12px">No company-specific news found. Showing general market news instead.</div></div>':'')+data.map(n=>`<div class="ni"><div class="ns">${n.source||'Market News'}</div><div class="nh">${n.headline}</div>${n.summary?`<div class="nsum">${n.summary}</div>`:''}</div>`).join('');
}

async function loadTicker(sym){
  try{
    const r=await fetch(`${A}/analyze?symbol=${encodeURIComponent(sym)}`);
    const d=await r.json();
    if(d.price){
      const up=d.change_pct>=0;
      return `<span class="tki" onclick="go('${sym}')"><span class="tsym">${d.symbol}</span><span class="tpx">$${d.price.toLocaleString()}</span><span class="${up?'tup':'tdn'}">${up?'+':''}${d.change_pct}%</span></span>`;
    }
  }catch(e){}
  return'';
}

async function buildTicker(){
  const tk=document.getElementById('tktrack');
  let h='';
  for(const s of TKS)h+=await loadTicker(s);
  if(h)tk.innerHTML=h+h;
}

async function loadMarket(){
  for(const m of MKT){
    try{
      const r=await fetch(`${A}/analyze?symbol=${encodeURIComponent(m.s)}`);
      const d=await r.json();
      const el=document.getElementById(m.id);
      if(d.price&&el){
        el.textContent=d.price.toLocaleString()+' ('+(d.change_pct>=0?'+':'')+d.change_pct+'%)';
        el.className='mv '+(d.change_pct>=0?'up':'dn');
      }else if(el){
        el.textContent='N/A';
        el.className='mv ld';
      }
    }catch(e){
      const el=document.getElementById(m.id);
      if(el){el.textContent='N/A';el.className='mv ld';}
    }
  }
}

function go(sym){document.getElementById('si').value=sym;run();}

let acT;
document.getElementById('si').addEventListener('input',function(){
  clearTimeout(acT);
  const v=this.value.trim();
  if(v.length<2){document.getElementById('ac').style.display='none';return;}
  acT=setTimeout(()=>suggest(v),300);
});

async function suggest(q){
  try{
    const r=await fetch(`${A}/search?q=${encodeURIComponent(q)}`);
    const d=await r.json();
    const ac=document.getElementById('ac');
    if(d.results&&d.results.length){
      ac.innerHTML=d.results.map(x=>`<div class="aci" onclick="go('${x.symbol}')"><span class="acs">${x.symbol}</span><span class="acn">${x.name||''}</span></div>`).join('');
      ac.style.display='block';
    }else ac.style.display='none';
  }catch(e){}
}

document.addEventListener('click',e=>{if(!e.target.closest('.srow'))document.getElementById('ac').style.display='none';});
document.getElementById('si').addEventListener('keypress',e=>{if(e.key==='Enter')run();});

async function run(){
  const val=document.getElementById('si').value.trim();
  if(!val)return;
  document.getElementById('ac').style.display='none';
  document.getElementById('lb').classList.add('on');
  document.getElementById('rpt').style.opacity='.35';

  try{
    const r=await fetch(`${A}/analyze?symbol=${encodeURIComponent(val)}`);
    const d=await r.json();

    if(d.error){
      document.getElementById('sym').textContent='NOT FOUND';
      document.getElementById('sfull').textContent=d.error;
      document.getElementById('lb').classList.remove('on');
      document.getElementById('rpt').style.opacity='1';
      return;
    }

    document.getElementById('sym').textContent=d.symbol||val;
    document.getElementById('sfull').textContent=d.name||val;
    document.getElementById('ssect').textContent=d.sector||'';
    document.getElementById('spx').textContent='$'+(d.price||0).toLocaleString();
    document.getElementById('mp').textContent='$'+(d.price||0).toLocaleString();

    const chg=d.change_pct||0;
    const ct=(chg>=0?'+':'')+chg+'% today';
    document.getElementById('schg').textContent=ct;
    document.getElementById('schg').className='sc '+(chg>=0?'up':'dn');
    document.getElementById('mc').textContent=(chg>=0?'+':'')+chg+'%';
    document.getElementById('mc').className='mv2 '+(chg>=0?'pos':'neg');

    document.getElementById('ms').textContent=(d.score||0)+'/15';
    document.getElementById('mpe').textContent=d.pe_ratio||'N/A';
    document.getElementById('mt').textContent=d.analyst_target&&d.analyst_target!=='N/A'?'$'+d.analyst_target:'N/A';
    document.getElementById('mm').textContent=fmt(d.market_cap);

    const v=d.verdict||'WATCH';
    document.getElementById('vbox').className='vb '+v.toLowerCase();
    document.getElementById('vbdg').className='vbdg '+v.toLowerCase();
    const vi={APPROVE:'&#9989;',PASS:'&#10060;',WATCH:'&#9889;'};
    document.getElementById('vbdg').innerHTML=vi[v]+' '+v;
    document.getElementById('vsco').textContent='Intelligence Score: '+(d.score||0)+'/15';
    document.getElementById('vconf').textContent=d.confidence||'';

    if(d.reasons&&d.reasons.length){
      document.getElementById('vrl').innerHTML=d.reasons.map(r=>`<div class="vr"><span class="vi">${r.icon}</span><div><span class="vlbl">${r.label}</span><span class="vdt">${r.detail}</span></div></div>`).join('');
    }

    renderCong(d.congressional||[]);
    renderIns(d.insider||[]);
    renderNews(d.news||[]);

    const vc=v==='APPROVE'?'va':v==='PASS'?'vp':'vw';
    const panel=document.getElementById('panel');
    const ex=panel.querySelector(`[data-s="${d.symbol}"]`);
    const card=`<div class="sg" data-s="${d.symbol}" onclick="go('${d.symbol}')"><div class="sgtop"><div class="sgsym">${d.symbol}</div><div class="sgv ${vc}">${v}</div></div><div class="sgi">$${(d.price||0).toLocaleString()} &nbsp;|&nbsp; Score: ${d.score}/15 &nbsp;|&nbsp; ${d.name}</div></div>`;
    if(ex)ex.outerHTML=card;
    else panel.insertAdjacentHTML('afterbegin',card);

  }catch(e){
    document.getElementById('sym').textContent='ERROR';
    document.getElementById('sfull').textContent='Connection failed. Check your internet and try again.';
  }

  document.getElementById('lb').classList.remove('on');
  document.getElementById('rpt').style.opacity='1';
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
    q = request.args.get("q","").strip()
    if not q:
        return jsonify({"error":"No query"}),400
    try:
        s=yf.Search(q,max_results=6)
        return jsonify({"results":[{"symbol":x.get("symbol"),"name":x.get("longname") or x.get("shortname")} for x in s.quotes if x.get("symbol")]})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route("/analyze")
def analyze():
    query = request.args.get("symbol","").strip()
    if not query:
        return jsonify({"error":"No symbol provided"}),400

    symbol = resolve_ticker(query)
    logger.info(f"ANALYZE: query={query} symbol={symbol}")

    market = market_agent.get(symbol)
    if not market:
        return jsonify({"error":f"No data found for {symbol}. Please check the name or ticker symbol."}),404

    news = news_agent.get(symbol)
    congressional = reg_agent.get_congressional(symbol)
    insider = ins_agent.get(symbol)
    verdict, confidence, reasons, score, signals = orch.synthesize(symbol, market, congressional, insider, news)

    return jsonify({
        "symbol": symbol,
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
        "news": news,
        "congressional": congressional,
        "insider": insider,
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0", port=port, debug=False)
