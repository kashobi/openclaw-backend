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
    return Response(HTML, mimetype='text/html')

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
        conviction = "Low"

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
                today = datetime.now().strftime('%Y-%m-%d')
                from_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)


HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Apex Q Intelligence Terminal</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap');
:root{
  --bg:#e4ecf5;--surface:#fff;--s2:#d6e2ef;--border:#aac0d8;
  --accent:#003eaa;--green:#004d22;--gbg:#b8f0cc;
  --red:#8b0000;--rbg:#ffc0c0;--yellow:#5c2d00;--ybg:#ffd888;
  --text:#040e1c;--muted:#2a4060;--navy:#040e1c;
}
*{margin:0;padding:0;box-sizing:border-box;}
body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',sans-serif;}
.tkbar{background:var(--navy);height:42px;overflow:hidden;display:flex;align-items:center;}
.tkwrap{width:100%;overflow:hidden;}
.tktrack{display:inline-flex;animation:tkscroll 70s linear infinite;white-space:nowrap;}
.tktrack:hover{animation-play-state:paused;}
.tki{display:inline-flex;align-items:center;gap:10px;padding:0 22px;height:42px;font-family:'JetBrains Mono',monospace;font-size:12px;border-right:1px solid #1a2d50;cursor:pointer;flex-shrink:0;}
.tki:hover{background:#1a2d50;}
.tsym{color:#fff;font-weight:800;}
.tpx{color:#7aabcc;font-size:11px;}
.tup{color:#00ff99;font-weight:800;}
.tdn{color:#ff5555;font-weight:800;}
@keyframes tkscroll{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}
.hdr{background:var(--surface);border-bottom:2px solid var(--border);padding:14px 24px;display:flex;align-items:center;justify-content:space-between;}
.logo{display:flex;align-items:center;gap:12px;}
.lmark{width:42px;height:42px;background:linear-gradient(135deg,#003eaa,#001e77);border-radius:11px;display:flex;align-items:center;justify-content:center;font-size:22px;}
.lname{font-size:25px;font-weight:800;color:var(--text);}
.lname span{color:var(--accent);}
.badge{display:flex;align-items:center;gap:7px;background:var(--gbg);border:2px solid var(--green);border-radius:20px;padding:6px 14px;font-size:11px;color:var(--green);font-family:'JetBrains Mono',monospace;font-weight:800;}
.dot{width:8px;height:8px;background:var(--green);border-radius:50%;animation:pulse 1.4s infinite;}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.2}}
.mbar{background:var(--surface);border-bottom:2px solid var(--border);overflow-x:auto;scrollbar-width:none;-webkit-overflow-scrolling:touch;}
.mbar::-webkit-scrollbar{display:none;}
.mbari{display:flex;min-width:max-content;}
.mi{padding:11px 22px;border-right:1px solid var(--border);cursor:pointer;transition:background .2s;min-width:150px;}
.mi:hover{background:var(--s2);}
.ml{font-size:9px;color:var(--muted);font-family:'JetBrains Mono',monospace;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:4px;font-weight:800;}
.mv{font-size:14px;font-weight:800;font-family:'JetBrains Mono',monospace;}
.mv.up{color:var(--green);}
.mv.dn{color:var(--red);}
.mv.ld{color:var(--muted);font-size:12px;}
.swrap{background:var(--surface);border-bottom:2px solid var(--border);padding:16px 24px 14px;}
.slbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:2px;margin-bottom:10px;font-family:'JetBrains Mono',monospace;font-weight:800;}
.srow{display:flex;gap:10px;max-width:720px;position:relative;}
.sinp{flex:1;background:var(--bg);border:2px solid var(--border);border-radius:11px;padding:13px 18px;color:var(--text);font-family:'Space Grotesk',sans-serif;font-size:15px;font-weight:600;outline:none;transition:all .2s;}
.sinp:focus{border-color:var(--accent);background:#fff;}
.sinp::placeholder{color:var(--muted);}
.sbtn{background:var(--accent);color:#fff;border:none;border-radius:11px;padding:13px 30px;font-family:'Space Grotesk',sans-serif;font-size:14px;font-weight:800;cursor:pointer;}
.sbtn:hover{background:#002d88;}
.ac{position:absolute;top:calc(100%+5px);left:0;right:100px;background:#fff;border:2px solid var(--border);border-radius:11px;z-index:300;display:none;box-shadow:0 8px 28px rgba(0,0,0,.12);}
.aci{padding:11px 16px;cursor:pointer;font-size:13px;display:flex;gap:12px;align-items:center;border-bottom:1px solid var(--border);}
.aci:last-child{border-bottom:none;}
.aci:hover{background:var(--bg);}
.acs{font-family:'JetBrains Mono',monospace;color:var(--accent);font-weight:800;min-width:64px;}
.acn{color:var(--muted);font-size:12px;}
.qrow{display:flex;gap:8px;margin-top:10px;flex-wrap:wrap;}
.qp{background:var(--bg);border:2px solid var(--border);border-radius:20px;padding:5px 14px;font-size:11px;color:var(--text);cursor:pointer;font-family:'JetBrains Mono',monospace;font-weight:700;}
.qp:hover{border-color:var(--accent);color:var(--accent);}
.main{padding:20px 24px 60px;display:grid;grid-template-columns:1fr 350px;gap:20px;}
@media(max-width:960px){.main{grid-template-columns:1fr;padding:14px 14px 50px;}}
.stitle{font-size:10px;text-transform:uppercase;letter-spacing:2px;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-bottom:12px;display:flex;align-items:center;gap:8px;font-weight:800;}
.stitle::after{content:'';flex:1;height:1px;background:var(--border);}
.rc{background:var(--surface);border:2px solid var(--border);border-radius:14px;padding:22px;margin-bottom:16px;box-shadow:0 2px 12px rgba(0,0,0,.05);}
.shdr{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:20px;gap:14px;}
.sn{font-size:32px;font-weight:800;color:var(--text);}
.sf{font-size:13px;color:var(--muted);margin-top:3px;font-weight:500;}
.ss{font-size:10px;color:var(--accent);margin-top:5px;font-family:'JetBrains Mono',monospace;font-weight:800;text-transform:uppercase;}
.pb{text-align:right;flex-shrink:0;}
.sp{font-size:32px;font-weight:800;font-family:'JetBrains Mono',monospace;color:var(--text);}
.sc{font-size:14px;font-family:'JetBrains Mono',monospace;margin-top:3px;font-weight:800;}
.sc.up{color:var(--green);}
.sc.dn{color:var(--red);}
.vb{border-radius:14px;padding:22px;margin-bottom:20px;}
.vb.approve{background:linear-gradient(135deg,#b8f0cc,#88e0aa);border:2px solid var(--green);}
.vb.pass{background:linear-gradient(135deg,#ffc0c0,#ff9090);border:2px solid var(--red);}
.vb.watch{background:linear-gradient(135deg,#ffd888,#ffbb44);border:2px solid var(--yellow);}
.vtop{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;flex-wrap:wrap;gap:10px;}
.vbdg{font-size:20px;font-weight:900;font-family:'JetBrains Mono',monospace;letter-spacing:4px;padding:12px 28px;border-radius:10px;box-shadow:0 2px 10px rgba(0,0,0,.2);}
.vbdg.approve{background:var(--green);color:#fff;}
.vbdg.pass{background:var(--red);color:#fff;}
.vbdg.watch{background:var(--yellow);color:#fff;}
.vsco{font-size:13px;font-family:'JetBrains Mono',monospace;color:var(--text);font-weight:800;background:rgba(255,255,255,.85);padding:8px 16px;border-radius:20px;}
.vconf{font-size:13px;color:var(--text);margin-bottom:14px;line-height:1.7;font-weight:500;background:rgba(255,255,255,.7);padding:14px 16px;border-radius:10px;}
.wguide{margin-bottom:14px;}
.wg{background:rgba(255,255,255,.75);border-radius:10px;padding:13px 16px;margin-bottom:8px;}
.wg.yellow{border-left:4px solid var(--yellow);}
.wg.blue{border-left:4px solid var(--accent);}
.wg.red{border-left:4px solid var(--red);}
.wgt{font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:1.5px;font-family:'JetBrains Mono',monospace;margin-bottom:5px;}
.wg.yellow .wgt{color:var(--yellow);}
.wg.blue .wgt{color:var(--accent);}
.wg.red .wgt{color:var(--red);}
.wgtxt{font-size:13px;color:var(--text);line-height:1.6;font-weight:500;}
.vrlist{display:flex;flex-direction:column;gap:8px;}
.vr{background:rgba(255,255,255,.9);border-radius:11px;overflow:hidden;border:1px solid rgba(0,0,0,.06);}
.vr-hdr{display:flex;align-items:center;gap:12px;padding:13px 16px;cursor:pointer;user-select:none;}
.vr-hdr:hover{background:rgba(0,62,170,.04);}
.vi{font-size:22px;flex-shrink:0;}
.vrm{flex:1;}
.vlbl{font-weight:800;display:block;color:var(--text);font-size:13px;}
.vshort{color:var(--muted);font-size:12px;margin-top:2px;display:block;font-weight:500;}
.vbtn{font-size:11px;color:var(--accent);font-family:'JetBrains Mono',monospace;font-weight:800;background:rgba(0,62,170,.1);padding:5px 12px;border-radius:20px;flex-shrink:0;border:1px solid rgba(0,62,170,.2);white-space:nowrap;}
.vr-body{display:none;padding:0 16px 16px 50px;border-top:1px solid rgba(0,0,0,.07);}
.vr-body.open{display:block;}
.vrs{margin-top:12px;}
.vrst{font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:1.5px;color:var(--accent);font-family:'JetBrains Mono',monospace;margin-bottom:5px;}
.vrstxt{font-size:13px;color:var(--text);line-height:1.7;font-weight:500;}
.vlesson{background:var(--s2);border-radius:9px;padding:11px 14px;margin-top:10px;border-left:3px solid var(--accent);}
.vlessont{font-size:9px;font-weight:800;text-transform:uppercase;letter-spacing:1.5px;color:var(--accent);font-family:'JetBrains Mono',monospace;margin-bottom:4px;}
.vlessontxt{font-size:13px;color:var(--text);line-height:1.6;font-style:italic;font-weight:500;}
.mets{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:18px;}
@media(max-width:600px){.mets{grid-template-columns:repeat(2,1fr);}}
.met{background:var(--s2);border-radius:10px;padding:13px;border:2px solid var(--border);}
.ml2{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:1.5px;margin-bottom:5px;font-family:'JetBrains Mono',monospace;font-weight:800;}
.mv2{font-size:17px;font-weight:800;font-family:'JetBrains Mono',monospace;color:var(--text);}
.mv2.pos{color:var(--green);}
.mv2.neg{color:var(--red);}
.mv2.neu{color:var(--accent);}
.ic{background:var(--s2);border:2px solid var(--border);border-radius:10px;padding:14px;margin-bottom:10px;}
.ih{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;}
.it{font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);font-family:'JetBrains Mono',monospace;}
.ibdg{font-size:10px;font-weight:800;padding:3px 11px;border-radius:20px;font-family:'JetBrains Mono',monospace;}
.bg{background:var(--gbg);color:var(--green);border:2px solid var(--green);}
.br{background:var(--rbg);color:var(--red);border:2px solid var(--red);}
.by{background:var(--ybg);color:var(--yellow);border:2px solid var(--yellow);}
.itxt{font-size:13px;color:var(--text);line-height:1.6;font-weight:500;}
.tr{padding:8px 0;border-bottom:1px solid var(--border);font-size:12px;display:flex;flex-wrap:wrap;gap:7px;align-items:center;}
.tr:last-child{border-bottom:none;}
.buy{color:var(--green);font-weight:800;font-family:'JetBrains Mono',monospace;}
.sell{color:var(--red);font-weight:800;font-family:'JetBrains Mono',monospace;}
.gray{color:var(--muted);font-size:11px;font-weight:500;}
.ni{padding:11px 0;border-bottom:1px solid var(--border);}
.ni:last-child{border-bottom:none;}
.ns{font-size:10px;color:var(--accent);font-family:'JetBrains Mono',monospace;text-transform:uppercase;font-weight:800;margin-bottom:4px;}
.nh{font-size:13px;color:var(--text);line-height:1.5;font-weight:600;}
.nsum{font-size:11px;color:var(--muted);margin-top:3px;line-height:1.4;}
.loading{display:none;padding:40px 20px;text-align:center;color:var(--accent);font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:800;}
.loading.on{display:block;animation:pulse 1.2s infinite;}
.sg{background:var(--surface);border:2px solid var(--border);border-radius:12px;padding:14px;margin-bottom:10px;cursor:pointer;transition:all .2s;}
.sg:hover{border-color:var(--accent);}
.sgtop{display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;}
.sgsym{font-size:17px;font-weight:800;font-family:'JetBrains Mono',monospace;color:var(--text);}
.sgv{font-size:11px;font-weight:800;padding:3px 11px;border-radius:20px;font-family:'JetBrains Mono',monospace;}
.va{background:var(--gbg);color:var(--green);border:2px solid var(--green);}
.vp{background:var(--rbg);color:var(--red);border:2px solid var(--red);}
.vw{background:var(--ybg);color:var(--yellow);border:2px solid var(--yellow);}
.sgi{font-size:11px;font-family:'JetBrains Mono',monospace;color:var(--muted);font-weight:600;}
.foot{background:var(--navy);padding:22px 24px;text-align:center;font-size:11px;color:#5a80aa;font-family:'JetBrains Mono',monospace;line-height:2;}
</style>
</head>
<body>
<div class="tkbar"><div class="tkwrap"><div class="tktrack" id="tktrack"><span class="tki"><span class="tsym">APEX Q</span><span class="tpx">Loading...</span></span></div></div></div>
<div class="hdr">
  <div class="logo"><div class="lmark">&#9889;</div><div class="lname">Apex <span>Q</span></div></div>
  <div class="badge"><div class="dot"></div>LIVE INTEL ACTIVE</div>
</div>
<div class="mbar"><div class="mbari">
  <div class="mi" onclick="go('^GSPC')"><div class="ml">S&amp;P 500</div><div class="mv ld" id="m0">--</div></div>
  <div class="mi" onclick="go('^IXIC')"><div class="ml">NASDAQ</div><div class="mv ld" id="m1">--</div></div>
  <div class="mi" onclick="go('^DJI')"><div class="ml">DOW JONES</div><div class="mv ld" id="m2">--</div></div>
  <div class="mi" onclick="go('^RUT')"><div class="ml">RUSSELL 2000</div><div class="mv ld" id="m3">--</div></div>
  <div class="mi" onclick="go('^VIX')"><div class="ml">VIX FEAR</div><div class="mv ld" id="m4">--</div></div>
  <div class="mi" onclick="go('GC=F')"><div class="ml">GOLD FUTURES</div><div class="mv ld" id="m5">--</div></div>
  <div class="mi" onclick="go('CL=F')"><div class="ml">OIL WTI</div><div class="mv ld" id="m6">--</div></div>
  <div class="mi" onclick="go('BTC-USD')"><div class="ml">BITCOIN</div><div class="mv ld" id="m7">--</div></div>
</div></div>
<div class="swrap">
  <div class="slbl">&#128269; Search any stock or company name</div>
  <div class="srow">
    <input class="sinp" id="si" type="text" placeholder="Type a company or ticker... Apple, Tesla, SpaceX, SOFI, AMD" autocomplete="off"/>
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
    <div class="loading" id="lb">Running intelligence agents...</div>
    <div id="rpt" class="rc">
      <div class="shdr">
        <div><div class="sn" id="sym">APEX Q</div><div class="sf" id="sfull">Search a stock above to begin</div><div class="ss" id="ssect"></div></div>
        <div class="pb"><div class="sp" id="spx">--</div><div class="sc up" id="schg">--</div></div>
      </div>
      <div class="vb watch" id="vbox">
        <div class="vtop">
          <div class="vbdg watch" id="vbdg">&#9889; READY</div>
          <div class="vsco" id="vsco">Market Intelligence Rating: --</div>
        </div>
        <div class="vconf" id="vconf">Search any stock or company name above. Tap any signal card to expand the full WHY, What To Watch For, and The Lesson.</div>
        <div class="wguide" id="wguide"></div>
        <div class="vrlist" id="vrl"></div>
      </div>
      <div class="mets">
        <div class="met"><div class="ml2">Current Price</div><div class="mv2 neu" id="mp">--</div></div>
        <div class="met"><div class="ml2">Change Today</div><div class="mv2" id="mc">--</div></div>
        <div class="met"><div class="ml2">Conviction</div><div class="mv2 neu" id="ms">--</div></div>
        <div class="met"><div class="ml2">PE Ratio</div><div class="mv2" id="mpe">--</div></div>
        <div class="met"><div class="ml2">Analyst Target</div><div class="mv2 pos" id="mt">--</div></div>
        <div class="met"><div class="ml2">Market Cap</div><div class="mv2 neu" id="mm">--</div></div>
      </div>
      <div class="stitle">&#127963; Congressional Trading</div>
      <div id="cong"><div class="ic"><div class="ih"><div class="it">Quiver Quantitative</div><div class="ibdg by">WAITING</div></div><div class="itxt">Loads after analysis.</div></div></div>
      <div class="stitle">&#128188; Insider Activity</div>
      <div id="ins"><div class="ic"><div class="ih"><div class="it">Insider Trades</div><div class="ibdg by">WAITING</div></div><div class="itxt">Loads after analysis.</div></div></div>
      <div class="stitle">&#128240; Live News</div>
      <div id="news"><div class="ic"><div class="ih"><div class="it">Finnhub News</div><div class="ibdg by">WAITING</div></div><div class="itxt">Loads after analysis.</div></div></div>
    </div>
  </div>
  <div>
    <div class="stitle">Live Signals</div>
    <div id="panel">
      <div class="sg" onclick="go('SOFI')"><div class="sgtop"><div class="sgsym">SOFI</div><div class="sgv vw">WATCH</div></div><div class="sgi">Click to analyze</div></div>
      <div class="sg" onclick="go('NVDA')"><div class="sgtop"><div class="sgsym">NVDA</div><div class="sgv vw">WATCH</div></div><div class="sgi">Click to analyze</div></div>
      <div class="sg" onclick="go('SPCX')"><div class="sgtop"><div class="sgsym">SPCX</div><div class="sgv vw">WATCH</div></div><div class="sgi">Click to analyze</div></div>
      <div class="sg" onclick="go('AMD')"><div class="sgtop"><div class="sgsym">AMD</div><div class="sgv vw">WATCH</div></div><div class="sgi">Click to analyze</div></div>
      <div class="sg" onclick="go('TSLA')"><div class="sgtop"><div class="sgsym">TSLA</div><div class="sgv vw">WATCH</div></div><div class="sgi">Click to analyze</div></div>
      <div class="sg" onclick="go('AAPL')"><div class="sgtop"><div class="sgsym">AAPL</div><div class="sgv vw">WATCH</div></div><div class="sgi">Click to analyze</div></div>
      <div class="sg" onclick="go('MSFT')"><div class="sgtop"><div class="sgsym">MSFT</div><div class="sgv vw">WATCH</div></div><div class="sgi">Click to analyze</div></div>
    </div>
  </div>
</div>
<div class="foot">
  APEX Q INTELLIGENCE TERMINAL &nbsp;|&nbsp; ANALYST AGENT &bull; REGULATORY AGENT &bull; INSIDER AGENT &bull; NEWS AGENT &bull; SYNTHESIS ENGINE<br><br>
  The insights provided are generated by our analytical engine for educational and illustrative purposes only.<br>
  They are not intended as financial, investment, or legal advice. Every market participant is unique.<br>
  We encourage you to perform your own due diligence or consult with a qualified professional before making any financial decisions.
</div>
<script>
const API = 'https://web-production-9897d5.up.railway.app';
const TKS=['AAPL','MSFT','NVDA','AMD','TSLA','AMZN','GOOGL','META','SOFI','SPCX','SCHD','JPM','BAC','NFLX','BTC-USD'];
const MKT=[{s:'^GSPC',id:'m0'},{s:'^IXIC',id:'m1'},{s:'^DJI',id:'m2'},{s:'^RUT',id:'m3'},{s:'^VIX',id:'m4'},{s:'GC=F',id:'m5'},{s:'CL=F',id:'m6'},{s:'BTC-USD',id:'m7'}];

function fmt(n){if(!n||n==='N/A')return 'N/A';const x=parseFloat(n);if(isNaN(x))return 'N/A';if(x>=1e12)return '$'+(x/1e12).toFixed(2)+'T';if(x>=1e9)return '$'+(x/1e9).toFixed(2)+'B';if(x>=1e6)return '$'+(x/1e6).toFixed(2)+'M';return '$'+x.toLocaleString();}

function buildCard(r,idx){
  const bid='vb'+idx;
  let body='';
  if(r.why)body+='<div class="vrs"><div class="vrst">&#10067; Why This Matters</div><div class="vrstxt">'+r.why+'</div></div>';
  if(r.what_to_watch)body+='<div class="vrs"><div class="vrst">&#128064; What To Watch For</div><div class="vrstxt">'+r.what_to_watch+'</div></div>';
  if(r.lesson)body+='<div class="vlesson"><div class="vlessont">&#127891; The Lesson</div><div class="vlessontxt">'+r.lesson+'</div></div>';
  return '<div class="vr"><div class="vr-hdr" onclick="toggle(\''+bid+'\',this)"><span class="vi">'+r.icon+'</span><div class="vrm"><span class="vlbl">'+r.label+'</span><span class="vshort">'+(r.short||'')+'</span></div><span class="vbtn" id="b'+bid+'">&#9660; LEARN WHY</span></div><div class="vr-body" id="'+bid+'">'+body+'</div></div>';
}

function toggle(bid,hdr){const body=document.getElementById(bid);const btn=document.getElementById('b'+bid);if(!body||!btn)return;const open=body.classList.toggle('open');btn.innerHTML=open?'&#9650; CLOSE':'&#9660; LEARN WHY';hdr.style.background=open?'rgba(0,62,170,0.05)':'';}

function buildReasons(d){
  const reasons=[];const chg=d.change_pct||0;const rec=d.recommendation||'HOLD';const tgt=d.analyst_target;const price=d.price||0;const pe=d.pe_ratio;const name=d.name||d.symbol;
  if(chg>3){reasons.push({icon:'📈',label:'Strong Price Momentum: +'+chg+'%',short:'Up '+chg+'% today. Strong buying pressure.',why:name+' moved up '+chg+'% in a single trading day. Buyers are strongly outnumbering sellers. Institutional investors are actively accumulating shares at this price.',what_to_watch:'Check if today\'s trading volume is higher than average. High price movement on high volume is more meaningful than a move on low volume.',lesson:'Strong price momentum on high volume is one of the clearest early signals that something meaningful is happening with a stock.'});}
  else if(chg>1){reasons.push({icon:'📊',label:'Positive Price Action: +'+chg+'%',short:'Up '+chg+'% today. Buyers in control.',why:name+' is up '+chg+'% today. Steady positive movement shows buyers consistently outnumbering sellers. This is a healthier signal than one explosive spike.',what_to_watch:'Look for this positive trend to continue over 2 to 3 consecutive days. Consistent upward movement is more reliable than a single large jump.',lesson:'Steady accumulation over multiple days shows sustained institutional interest rather than a one-day speculative move.'});}
  else if(chg>0){reasons.push({icon:'➡️',label:'Slight Upward Drift: +'+chg+'%',short:'Up '+chg+'% today. Minimal movement.',why:name+' barely moved today — only '+chg+'%. Buyers and sellers are nearly balanced. The market has no strong opinion right now.',what_to_watch:'Wait for a clear catalyst — earnings, analyst upgrade, or insider buying — to push this in a clear direction before acting.',lesson:'When a stock barely moves it means the market is undecided. Small news events can swing it either way.'});}
  else if(chg<-5){reasons.push({icon:'📉',label:'Heavy Selling Pressure: '+chg+'%',short:'Down '+Math.abs(chg)+'% today. Significant selling.',why:name+' dropped '+Math.abs(chg)+'% in a single day. Something spooked the market. This could be bad earnings, negative news, or large investors exiting.',what_to_watch:'Find out WHY it dropped before making any decision. Check the news section below. A drop on bad earnings is different from general market fear.',lesson:'Big single-day drops can be buying opportunities OR the start of a longer decline. Understanding the reason is always the most important first step.'});}
  else if(chg<-2){reasons.push({icon:'📉',label:'Significant Decline: '+chg+'%',short:'Down '+Math.abs(chg)+'% today. Sellers in control.',why:name+' is down '+Math.abs(chg)+'% today. Sellers are outnumbering buyers. This could be a temporary pullback or the start of a real trend change.',what_to_watch:'Check if the stock is holding above recent support levels. If it breaks below recent lows the selling could accelerate.',lesson:'Not every pullback is a crisis. The question is always whether the underlying business is still strong.'});}
  else{reasons.push({icon:'➡️',label:'Minor Pullback: '+chg+'%',short:'Down '+Math.abs(chg)+'% today. Small decline.',why:name+' dipped '+Math.abs(chg)+'% today. Small normal move that could simply be routine profit taking.',what_to_watch:'If the stock has been trending up before this small dip it may just be a healthy breather.',lesson:'Small daily declines are completely normal. What matters is the overall trend over days and weeks not a single day.'});}
  if(rec==='BUY'||rec==='STRONG_BUY'){reasons.push({icon:'✅',label:'Wall Street Rating: '+rec.replace('_',' '),short:'Professional analysts rate this a Buy.',why:'Analysts at major banks spend months studying '+name+'. When they say Buy they are putting their professional reputation on the line. This represents hundreds of hours of research.',what_to_watch:'Check the analyst price target. A large gap between current price and target means analysts see significant upside still available.',lesson:'A consensus Buy rating from multiple independent research teams is a meaningful signal worth respecting.'});}
  else if(rec==='SELL'||rec==='STRONG_SELL'){reasons.push({icon:'⛔',label:'Wall Street Rating: '+rec.replace('_',' '),short:'Professional analysts are negative on this stock.',why:'When professionals who research '+name+' full time say Sell it is a serious warning. This is the consensus of multiple independent research teams.',what_to_watch:'Read the news section to understand what concerns are driving the negative rating.',lesson:'Never fight the research. The people who know a company best saying sell deserves serious respect.'});}
  else{reasons.push({icon:'⚠️',label:'Wall Street Rating: Hold',short:'Analysts are in wait-and-see mode.',why:'A Hold means analysts do not see a compelling reason to buy OR sell '+name+' right now. The stock may be fairly valued or analysts are waiting for the next earnings report.',what_to_watch:'A Hold can flip to Buy quickly after a strong earnings report or positive announcement.',lesson:'Hold often means the professional community is waiting for more information. This is frequently the calm before a significant move.'});}
  if(tgt&&price&&tgt!=='N/A'){try{const up=Math.round(((parseFloat(tgt)-price)/price)*100);if(up>15){reasons.push({icon:'🎯',label:up+'% Upside to Analyst Target: $'+tgt,short:'Analysts see '+up+'% more room to grow.',why:'The analyst consensus target for '+name+' is $'+tgt+'. At $'+price+' today that is '+up+'% potential upside based on professional models of future earnings.',what_to_watch:'Price targets move after earnings. If the company beats expectations targets get raised.',lesson:'A large gap between current price and analyst target suggests the market has not fully valued what analysts see.'});}else if(up>0){reasons.push({icon:'🎯',label:up+'% Upside to Target: $'+tgt,short:'Modest '+up+'% upside to analyst target.',why:'Analysts see $'+tgt+' as fair value versus today\'s price of $'+price+'. Positive gap but not dramatic.',what_to_watch:'The closer a stock gets to its target the less upside remains.',lesson:'Price targets narrow as stocks rise. The best entries are far below the consensus target.'});}else if(up<-5){reasons.push({icon:'🎯',label:'Trading '+Math.abs(up)+'% Above Target',short:'Stock is above what analysts think it is worth.',why:name+' at $'+price+' is trading '+Math.abs(up)+'% ABOVE the analyst target of $'+tgt+'. The stock may have run ahead of fundamentals.',what_to_watch:'Stocks above analyst targets need consistently strong earnings just to maintain their premium.',lesson:'Trading above analyst targets means pricing in perfection. Any miss can cause a sharp pullback.'});}}catch(e){}}
  if(pe&&pe!=='N/A'){try{const pn=parseFloat(pe);if(pn<12){reasons.push({icon:'💰',label:'PE '+pn.toFixed(1)+' — Potentially Undervalued',short:'Paying only '+pn.toFixed(1)+'x earnings. Market average is 20x.',why:'A PE of '+pn.toFixed(1)+' means you pay $'+pn.toFixed(1)+' for every dollar '+name+' earns. The market average is 20x. This company is significantly cheaper than average.',what_to_watch:'Low PE is only positive if earnings are stable or growing. Check whether profits have been trending up.',lesson:'PE ratio measures how much you pay per dollar of company profit. Cheap PE plus growing earnings is one of the most powerful investing combinations.'});}else if(pn<20){reasons.push({icon:'💰',label:'PE '+pn.toFixed(1)+' — Reasonably Valued',short:'PE at or below market average. Not overpriced.',why:'A PE of '+pn.toFixed(1)+' means you are paying a fair price for '+name+'\'s earnings. At or below the market average of 20x means you are not overpaying.',what_to_watch:'Reasonable PE plus growing earnings is the ideal setup. Check year over year earnings growth.',lesson:'The best long term investments combine reasonable valuation with growing earnings.'});}else if(pn>60){reasons.push({icon:'💸',label:'PE '+pn.toFixed(1)+' — High Valuation',short:'Paying a significant premium. Requires strong growth.',why:'A PE of '+pn.toFixed(1)+' requires exceptional future growth to justify. If growth disappoints the stock could drop sharply.',what_to_watch:'Is the high PE driven by genuine growth potential or by excitement and momentum? Separate the story from the math.',lesson:'High PE stocks need consistent high growth just to maintain their valuation. Manage position size carefully.'});}else{reasons.push({icon:'📋',label:'PE '+pn.toFixed(1)+' — Growth Premium',short:'Above average valuation. Growth must justify it.',why:'A PE of '+pn.toFixed(1)+' means paying above the 20x market average for '+name+'. Acceptable for a high growth company if growth materializes.',what_to_watch:'A PE of 35 with 30% earnings growth is reasonable. A PE of 35 with only 5% growth means overpaying.',lesson:'High PE is not automatically bad if growth justifies it.'});}}catch(e){}}
  return reasons;
}

function renderCong(data){const s=document.getElementById('cong');if(!data||!data.length){s.innerHTML='<div class="ic"><div class="ih"><div class="it">Congressional Trading</div><div class="ibdg bg">CLEAN</div></div><div class="itxt">No recent congressional trading disclosures found. No politicians have publicly reported buying or selling this company recently.</div></div>';return;}const buys=data.filter(t=>t.action&&t.action.toLowerCase().includes('purchase'));const sells=data.filter(t=>t.action&&t.action.toLowerCase().includes('sale'));const bc=buys.length>sells.length?'bg':sells.length>buys.length?'br':'by';const bl=buys.length>sells.length?'BUYING ('+buys.length+')':sells.length>buys.length?'SELLING ('+sells.length+')':'MIXED';s.innerHTML='<div class="ic"><div class="ih"><div class="it">Congressional Trades — '+data.length+' total</div><div class="ibdg '+bc+'">'+bl+'</div></div><div class="itxt">'+data.map(t=>'<div class="tr"><span class="'+(t.action&&t.action.toLowerCase().includes('purchase')?'buy':'sell')+'">'+t.action+'</span><span>'+t.politician+'</span><span class="gray">('+t.party+')</span><span class="gray">'+t.amount+'</span><span class="gray">'+t.date+'</span></div>').join('')+'</div></div>';}

function renderIns(data){const s=document.getElementById('ins');if(!data||!data.length){s.innerHTML='<div class="ic"><div class="ih"><div class="it">Insider Activity</div><div class="ibdg bg">CLEAN</div></div><div class="itxt">No recent insider trading filings found. Executives and directors have not reported unusual personal trading recently.</div></div>';return;}const buys=data.filter(t=>t.action==='A');const sells=data.filter(t=>t.action==='D');const cb=data.filter(t=>t.is_clevel&&t.action==='A');const bc=buys.length>sells.length?'bg':sells.length>buys.length?'br':'by';let lbl=buys.length>sells.length?'BUYING':'SELLING';if(cb.length>=2)lbl='CLUSTER BUY &#9889;';s.innerHTML='<div class="ic"><div class="ih"><div class="it">Insider Trades — '+data.length+' filings</div><div class="ibdg '+bc+'">'+lbl+'</div></div><div class="itxt">'+data.slice(0,8).map(t=>'<div class="tr"><span class="'+(t.action==='A'?'buy':'sell')+'">'+(t.action==='A'?'BUY':'SELL')+'</span><span>'+t.name+'</span><span class="gray">'+t.title+'</span>'+(t.shares?'<span class="gray">'+parseInt(t.shares).toLocaleString()+' shares</span>':'')+'<span class="gray">'+t.date+'</span></div>').join('')+'</div></div>';}

function renderNews(data){const s=document.getElementById('news');if(!data||!data.length){s.innerHTML='<div class="ic"><div class="ih"><div class="it">Finnhub News</div><div class="ibdg by">NO RESULTS</div></div><div class="itxt">No news articles found in the last 60 days. Low media coverage is not automatically negative.</div></div>';return;}const isGen=data.some(n=>n.source&&n.source.includes('General'));const pre=isGen?'<div class="ic" style="margin-bottom:8px"><div class="ih"><div class="it">General Market News</div><div class="ibdg by">NO COMPANY NEWS</div></div><div class="itxt" style="font-size:12px">No company-specific news found. Showing general market news.</div></div>':'';s.innerHTML=pre+data.map(n=>'<div class="ni"><div class="ns">'+(n.source||'News')+'</div><div class="nh">'+n.headline+'</div>'+(n.summary?'<div class="nsum">'+n.summary+'</div>':'')+'</div>').join('');}

async function loadTicker(sym){try{const r=await fetch(API+'/analyze?symbol='+encodeURIComponent(sym));const d=await r.json();if(d.price){const up=d.change_pct>=0;return '<span class="tki" onclick="go(\''+sym+'\')"><span class="tsym">'+d.symbol+'</span><span class="tpx">$'+d.price.toLocaleString()+'</span><span class="'+(up?'tup':'tdn')+'">'+(up?'+':'')+d.change_pct+'%</span></span>';}}catch(e){}return '';}

async function buildTicker(){const tk=document.getElementById('tktrack');let h='';for(const s of TKS)h+=await loadTicker(s);if(h)tk.innerHTML=h+h;}

async function loadMarket(){for(const m of MKT){const el=document.getElementById(m.id);try{const r=await fetch(API+'/analyze?symbol='+encodeURIComponent(m.s));const d=await r.json();if(d.price&&el){el.textContent=d.price.toLocaleString()+' ('+(d.change_pct>=0?'+':'')+d.change_pct+'%)';el.className='mv '+(d.change_pct>=0?'up':'dn');}else if(el){el.textContent='N/A';el.className='mv ld';}}catch(e){if(el){el.textContent='N/A';el.className='mv ld';}}}}

function go(sym){document.getElementById('si').value=sym;run();}

let acT;
document.getElementById('si').addEventListener('input',function(){clearTimeout(acT);const v=this.value.trim();if(v.length<2){document.getElementById('ac').style.display='none';return;}acT=setTimeout(function(){suggest(v);},300);});

async function suggest(q){try{const r=await fetch(API+'/search?q='+encodeURIComponent(q));const d=await r.json();const ac=document.getElementById('ac');if(d.results&&d.results.length){ac.innerHTML=d.results.map(x=>'<div class="aci" onclick="go(\''+x.symbol+'\')"><span class="acs">'+x.symbol+'</span><span class="acn">'+(x.name||'')+'</span></div>').join('');ac.style.display='block';}else ac.style.display='none';}catch(e){}}

document.addEventListener('click',function(e){if(!e.target.closest('.srow'))document.getElementById('ac').style.display='none';});
document.getElementById('si').addEventListener('keypress',function(e){if(e.key==='Enter')run();});

async function run(){
  const val=document.getElementById('si').value.trim();
  if(!val)return;
  document.getElementById('ac').style.display='none';
  document.getElementById('lb').classList.add('on');
  document.getElementById('rpt').style.opacity='.3';
  try{
    const r=await fetch(API+'/analyze?symbol='+encodeURIComponent(val));
    const d=await r.json();
    if(d.error){document.getElementById('sym').textContent='NOT FOUND';document.getElementById('sfull').textContent=d.error;document.getElementById('lb').classList.remove('on');document.getElementById('rpt').style.opacity='1';return;}
    document.getElementById('sym').textContent=d.symbol||val;
    document.getElementById('sfull').textContent=d.name||val;
    document.getElementById('ssect').textContent=d.sector||'';
    document.getElementById('spx').textContent='$'+(d.price||0).toLocaleString();
    document.getElementById('mp').textContent='$'+(d.price||0).toLocaleString();
    const chg=d.change_pct||0;
    document.getElementById('schg').textContent=(chg>=0?'+':'')+chg+'% today';
    document.getElementById('schg').className='sc '+(chg>=0?'up':'dn');
    document.getElementById('mc').textContent=(chg>=0?'+':'')+chg+'%';
    document.getElementById('mc').className='mv2 '+(chg>=0?'pos':'neg');
    document.getElementById('ms').textContent=d.conviction||'--';
    document.getElementById('mpe').textContent=d.pe_ratio||'N/A';
    document.getElementById('mt').textContent=(d.analyst_target&&d.analyst_target!=='N/A')?'$'+d.analyst_target:'N/A';
    document.getElementById('mm').textContent=fmt(d.market_cap);
    const v=d.verdict||'WATCH';
    document.getElementById('vbox').className='vb '+v.toLowerCase();
    document.getElementById('vbdg').className='vbdg '+v.toLowerCase();
    const vi={APPROVE:'&#9989;',PASS:'&#10060;',WATCH:'&#9889;'};
    document.getElementById('vbdg').innerHTML=vi[v]+' '+v;
    document.getElementById('vsco').textContent='Market Intelligence Rating: '+(d.conviction||'--');
    const conf={APPROVE:'Strong bullish signals across multiple intelligence layers. Price momentum and analyst consensus align favorably.',PASS:'Multiple signals are pointing negative. The data suggests avoiding this position until conditions improve.',WATCH:'Mixed signals across intelligence layers. Monitor for a clearer directional move before acting.'};
    document.getElementById('vconf').textContent=conf[v]||'';
    const wg=document.getElementById('wguide');
    if(v==='WATCH'){wg.innerHTML='<div class="wg yellow"><div class="wgt">&#128064; What To Watch For</div><div class="wgtxt">Strong price momentum above +2% on high volume. An analyst upgrade to Buy. Insider buying by a C-level executive.</div></div><div class="wg blue"><div class="wgt">&#9889; What Changes The Verdict</div><div class="wgtxt">For APPROVE: Need analyst Buy rating AND positive price momentum together. For PASS: Need analyst Sell AND price decline below -3%.</div></div>';}
    else if(v==='PASS'){wg.innerHTML='<div class="wg red"><div class="wgt">&#128064; Watch For Recovery Signs</div><div class="wgtxt">Price stabilization above recent lows. Analyst upgrade. Insider buying by executives.</div></div><div class="wg blue"><div class="wgt">&#9889; What Changes The Verdict</div><div class="wgtxt">For WATCH: Price needs to stabilize and one negative signal needs to flip positive. For APPROVE: Multiple signals need to align bullishly.</div></div>';}
    else{wg.innerHTML='';}
    const reasons=buildReasons(d);
    document.getElementById('vrl').innerHTML=reasons.map(function(r,i){return buildCard(r,i);}).join('');
    renderCong(d.congressional||[]);
    renderIns(d.insider||[]);
    renderNews(d.news||[]);
    const vc=v==='APPROVE'?'va':v==='PASS'?'vp':'vw';
    const panel=document.getElementById('panel');
    const ex=panel.querySelector('[data-s="'+d.symbol+'"]');
    const card='<div class="sg" data-s="'+d.symbol+'" onclick="go(\''+d.symbol+'\')"><div class="sgtop"><div class="sgsym">'+d.symbol+'</div><div class="sgv '+vc+'">'+v+'</div></div><div class="sgi">$'+(d.price||0).toLocaleString()+' | '+(d.conviction||'')+' | '+d.name+'</div></div>';
    if(ex)ex.outerHTML=card;
    else panel.insertAdjacentHTML('afterbegin',card);
  }catch(e){document.getElementById('sym').textContent='ERROR';document.getElementById('sfull').textContent='Connection failed. Try again.';}
  document.getElementById('lb').classList.remove('on');
  document.getElementById('rpt').style.opacity='1';
}

buildTicker();
loadMarket();
</script>
</body>
</html>"""
