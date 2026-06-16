from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import yfinance as yf
import os

app = Flask(__name__)
CORS(app)

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
  .market-item{display:flex;flex-direction:column;padding:10px 20px;border-right:1px solid var(--border);cursor:pointer;transition:background 0.2s;min-width:110px;}
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
  .verdict-display{border-radius:12px;padding:20px;margin-bottom:20px;text-align:center;transition:all 0.3s;}
  .verdict-display.approve{background:linear-gradient(135deg,#e6f4ed,#c8ebd8);border:2px solid var(--green);}
  .verdict-display.pass{background:linear-gradient(135deg,#fce8e8,#f5c6c6);border:2px solid var(--red);}
  .verdict-display.watch{background:linear-gradient(135deg,#fff3e0,#ffe0b2);border:2px solid var(--yellow);}
  .verdict-badge{font-size:30px;font-weight:900;font-family:'JetBrains Mono',monospace;letter-spacing:4px;margin-bottom:8px;}
  .verdict-badge.approve{color:var(--green);}
  .verdict-badge.pass{color:var(--red);}
  .verdict-badge.watch{color:var(--yellow);}
  .verdict-confidence{font-size:13px;color:var(--muted);margin-bottom:16px;line-height:1.5;}
  .verdict-reasons{text-align:left;display:flex;flex-direction:column;gap:8px;}
  .verdict-reason{display:flex;align-items:flex-start;gap:10px;padding:10px 14px;background:rgba(255,255,255,0.8);border-radius:8px;font-size:13px;line-height:1.5;}
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
  .footer{text-align:center;padding:16px;border-top:1px solid var(--border);font-size:10px;color:var(--muted);font-family:'JetBrains Mono',monospace;background:var(--surface);}
</style>
</head>
<body>
<div class="ticker-bar"><div class="ticker-track" id="tickerTrack"><span class="ticker-item"><span class="ticker-sym">Loading market data...</span></span></div></div>
<div class="header">
  <div class="logo"><div class="logo-icon">&#9889;</div><div class="logo-text">Apex <span>Q</span></div></div>
  <div class="status-dot"><div class="dot"></div>LIVE INTEL ACTIVE</div>
</div>
<div class="market-bar">
  <div class="market-item" onclick="tickerClick('SPY')"><div class="market-label">S&amp;P 500</div><div class="market-value up" id="m-SPY">Loading...</div></div>
  <div class="market-item" onclick="tickerClick('QQQ')"><div class="market-label">NASDAQ</div><div class="market-value up" id="m-QQQ">Loading...</div></div>
  <div class="market-item" onclick="tickerClick('DIA')"><div class="market-label">DOW</div><div class="market-value up" id="m-DIA">Loading...</div></div>
  <div class="market-item" onclick="tickerClick('IWM')"><div class="market-label">RUSSELL</div><div class="market-value up" id="m-IWM">Loading...</div></div>
  <div class="market-item" onclick="tickerClick('GLD')"><div class="market-label">GOLD</div><div class="market-value up" id="m-GLD">Loading...</div></div>
  <div class="market-item" onclick="tickerClick('USO')"><div class="market-label">OIL</div><div class="market-value" id="m-USO">Loading...</div></div>
  <div class="market-item" onclick="tickerClick('BTC-USD')"><div class="market-label">BITCOIN</div><div class="market-value up" id="m-BTC">Loading...</div></div>
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
    <div id="loading" class="loading">Pulling live intelligence... stand by</div>
    <div class="report-card" id="report">
      <div class="stock-header">
        <div>
          <div class="stock-name" id="stockSymbol">APEX Q</div>
          <div class="stock-full" id="stockName">Search a stock to begin</div>
          <div class="stock-sector" id="stockSector"></div>
        </div>
        <div class="price-block">
          <div class="price" id="stockPrice">--</div>
          <div class="price-change up" id="stockChange">-- today</div>
        </div>
      </div>
      <div class="verdict-display watch" id="verdictDisplay">
        <div class="verdict-badge watch" id="verdictBadge">&#9889; READY</div>
        <div class="verdict-confidence" id="verdictConfidence">Search any stock or company above to see a full signal breakdown with plain English reasoning.</div>
        <div class="verdict-reasons" id="verdictReasons">
          <div class="verdict-reason"><span class="reason-icon">&#128202;</span><div class="reason-text"><span class="reason-label">Price Momentum</span><span class="reason-detail">How the stock is moving today compared to yesterday</span></div></div>
          <div class="verdict-reason"><span class="reason-icon">&#10003;</span><div class="reason-text"><span class="reason-label">Analyst Consensus</span><span class="reason-detail">What Wall Street professionals think about this stock</span></div></div>
          <div class="verdict-reason"><span class="reason-icon">&#127919;</span><div class="reason-text"><span class="reason-label">Price Target</span><span class="reason-detail">How far the stock could go based on analyst projections</span></div></div>
          <div class="verdict-reason"><span class="reason-icon">&#128176;</span><div class="reason-text"><span class="reason-label">Valuation</span><span class="reason-detail">Whether the stock is cheap, fair, or expensive right now</span></div></div>
        </div>
      </div>
      <div class="metrics">
        <div class="metric"><div class="metric-label">Price</div><div class="metric-value neutral" id="metricPrice">--</div></div>
        <div class="metric"><div class="metric-label">Change Today</div><div class="metric-value" id="metricChange">--</div></div>
        <div class="metric"><div class="metric-label">Verdict</div><div class="metric-value neutral" id="metricVerdict">--</div></div>
        <div class="metric"><div class="metric-label">PE Ratio</div><div class="metric-value" id="metricPE">--</div></div>
        <div class="metric"><div class="metric-label">Analyst Target</div><div class="metric-value" id="metricTarget">--</div></div>
        <div class="metric"><div class="metric-label">Analyst Call</div><div class="metric-value neutral" id="metricRec">--</div></div>
      </div>
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
    </div>
  </div>
</div>
<div class="footer">APEX Q INTELLIGENCE TERMINAL &nbsp;|&nbsp; POWERED BY YFINANCE + FINNHUB + SEC EDGAR &nbsp;|&nbsp; FOR EDUCATIONAL PURPOSES ONLY. NOT FINANCIAL ADVICE.</div>
<script>
const API = window.location.origin;
const TICKERS = ['SPY','QQQ','DIA','IWM','AAPL','MSFT','NVDA','AMD','TSLA','AMZN','GOOGL','META','SOFI','SPCX','SCHD','JPM','BAC','GLD','BTC-USD','NFLX'];

function buildReasons(data) {
  const change = data.change_pct || 0;
  const rec = data.recommendation || 'N/A';
  const pe = data.pe_ratio;
  const target = data.analyst_target;
  const price = data.price;
  const reasons = [];
  if (change > 2) reasons.push({icon:'&#128200;',label:'Strong Price Momentum',detail:`Up ${change}% today. Buyers are in control and demand is strong.`});
  else if (change > 0) reasons.push({icon:'&#128202;',label:'Positive Price Action',detail:`Up ${change}% today. Modest positive momentum in the right direction.`});
  else if (change < -3) reasons.push({icon:'&#128201;',label:'Significant Price Decline',detail:`Down ${Math.abs(change)}% today. Sellers are dominating. Risk is elevated.`});
  else reasons.push({icon:'&#10145;',label:'Neutral Price Action',detail:`${change}% today. No strong directional momentum at this time.`});
  if (rec==='BUY'||rec==='STRONG_BUY') reasons.push({icon:'&#10003;',label:`Analyst Consensus: ${rec.replace('_',' ')}`,detail:'Wall Street professionals rate this a Buy. Professional money managers see upside ahead.'});
  else if (rec==='SELL'||rec==='STRONG_SELL') reasons.push({icon:'&#9940;',label:`Analyst Consensus: ${rec.replace('_',' ')}`,detail:'Analysts are cautious or negative on this stock right now. Professional consensus suggests caution.'});
  else reasons.push({icon:'&#9888;',label:'Analyst Consensus: Hold',detail:'Analysts are not strongly bullish or bearish. Neutral professional outlook at this time.'});
  if (target && price) {
    const upside = Math.round(((target-price)/price)*100);
    if (upside>10) reasons.push({icon:'&#127919;',label:`${upside}% Upside to Analyst Target`,detail:`Analyst price target is $${target}. At $${price} today there is significant room to grow.`});
    else if (upside>0) reasons.push({icon:'&#127919;',label:`${upside}% Upside to Analyst Target`,detail:`Analyst price target is $${target}. Modest upside from current price of $${price}.`});
    else reasons.push({icon:'&#127919;',label:'Trading Above Analyst Target',detail:`Current price $${price} is above the analyst target of $${target}. May be overvalued.`});
  }
  if (pe && pe!=='N/A') {
    const peNum = parseFloat(pe);
    if (peNum<15) reasons.push({icon:'&#128176;',label:`PE ${peNum.toFixed(1)} — Potentially Undervalued`,detail:'Low PE ratio suggests the stock may be cheap relative to its earnings power.'});
    else if (peNum>50) reasons.push({icon:'&#128184;',label:`PE ${peNum.toFixed(1)} — Premium Valuation`,detail:'High PE means investors are paying a premium. High growth expectations must be met.'});
    else reasons.push({icon:'&#128203;',label:`PE ${peNum.toFixed(1)} — Fair Valuation`,detail:'PE ratio is in a reasonable range relative to broader market averages.'});
  }
  return reasons;
}

function updateVerdictDisplay(data) {
  const verdict = data.verdict || 'WATCH';
  const display = document.getElementById('verdictDisplay');
  const badge = document.getElementById('verdictBadge');
  const confidence = document.getElementById('verdictConfidence');
  const reasonsEl = document.getElementById('verdictReasons');
  display.className = 'verdict-display ' + verdict.toLowerCase();
  badge.className = 'verdict-badge ' + verdict.toLowerCase();
  const icons = {APPROVE:'&#9989;',PASS:'&#10060;',WATCH:'&#9889;'};
  badge.innerHTML = icons[verdict] + ' ' + verdict;
  if (verdict==='APPROVE') confidence.textContent = data.symbol + ' shows bullish signals across multiple indicators. Positive price momentum combined with analyst buy consensus makes this a higher conviction opportunity.';
  else if (verdict==='PASS') confidence.textContent = data.symbol + ' is showing weakness. Declining price action or negative analyst sentiment suggests sitting this one out for now.';
  else confidence.textContent = data.symbol + ' has mixed signals. Not strong enough to approve and not weak enough to pass. Monitor for a clearer entry point before acting.';
  const reasons = buildReasons(data);
  reasonsEl.innerHTML = reasons.map(r=>`<div class="verdict-reason"><span class="reason-icon">${r.icon}</span><div class="reason-text"><span class="reason-label">${r.label}</span><span class="reason-detail">${r.detail}</span></div></div>`).join('');
}

async function loadTicker(sym) {
  try {
    const res = await fetch(`${API}/analyze?symbol=${sym}`);
    const d = await res.json();
    if (d.price) {
      const cc = d.change_pct>=0?'up':'down';
      const cs = (d.change_pct>=0?'+':'')+d.change_pct+'%';
      return `<span class="ticker-item" onclick="tickerClick('${sym}')"><span class="ticker-sym">${d.symbol}</span><span class="ticker-price">$${d.price}</span><span class="ticker-change ${cc}">${cs}</span></span>`;
    }
  } catch(e){}
  return '';
}

async function buildTicker() {
  const track = document.getElementById('tickerTrack');
  let html = '';
  for (const sym of TICKERS) { html += await loadTicker(sym); }
  track.innerHTML = html + html;
}

async function loadMarketBar() {
  const markets = [{sym:'SPY',id:'m-SPY'},{sym:'QQQ',id:'m-QQQ'},{sym:'DIA',id:'m-DIA'},{sym:'IWM',id:'m-IWM'},{sym:'GLD',id:'m-GLD'},{sym:'USO',id:'m-USO'},{sym:'BTC-USD',id:'m-BTC'}];
  for (const m of markets) {
    try {
      const res = await fetch(`${API}/analyze?symbol=${m.sym}`);
      const d = await res.json();
      if (d.price) {
        const el = document.getElementById(m.id);
        if (el) { el.textContent='$'+d.price+' ('+(d.change_pct>=0?'+':'')+d.change_pct+'%)'; el.className='market-value '+(d.change_pct>=0?'up':'down'); }
      }
    } catch(e){}
  }
}

function tickerClick(sym) { document.getElementById('searchInput').value=sym; analyzeStock(); }

let searchTimeout;
document.getElementById('searchInput').addEventListener('input',function(){
  clearTimeout(searchTimeout);
  const val=this.value.trim();
  if(val.length<2){document.getElementById('autocomplete').style.display='none';return;}
  searchTimeout=setTimeout(()=>fetchSuggestions(val),300);
});

async function fetchSuggestions(query) {
  try {
    const res = await fetch(`${API}/search?q=${encodeURIComponent(query)}`);
    const data = await res.json();
    const ac = document.getElementById('autocomplete');
    if (data.results&&data.results.length>0) {
      ac.innerHTML=data.results.map(r=>`<div class="autocomplete-item" onclick="tickerClick('${r.symbol}')"><span class="autocomplete-sym">${r.symbol}</span><span class="autocomplete-name">${r.name||''}</span></div>`).join('');
      ac.style.display='block';
    } else { ac.style.display='none'; }
  } catch(e){}
}

document.addEventListener('click',function(e){if(!e.target.closest('.search-box'))document.getElementById('autocomplete').style.display='none';});

async function analyzeStock() {
  const val=document.getElementById('searchInput').value.trim();
  if(!val)return;
  document.getElementById('autocomplete').style.display='none';
  document.getElementById('loading').classList.add('active');
  document.getElementById('report').style.opacity='0.4';
  try {
    const res=await fetch(`${API}/analyze?symbol=${encodeURIComponent(val)}`);
    const data=await res.json();
    if(data.error){document.getElementById('stockSymbol').textContent='NOT FOUND';document.getElementById('stockName').textContent=data.error;document.getElementById('loading').classList.remove('active');document.getElementById('report').style.opacity='1';return;}
    document.getElementById('stockSymbol').textContent=data.symbol||val;
    document.getElementById('stockName').textContent=data.name||val;
    document.getElementById('stockSector').textContent=data.sector||'';
    document.getElementById('stockPrice').textContent='$'+(data.price||0);
    document.getElementById('metricPrice').textContent='$'+(data.price||0);
    document.getElementById('metricChange').textContent=(data.change_pct>=0?'+':'')+data.change_pct+'%';
    document.getElementById('metricVerdict').textContent=data.verdict||'WATCH';
    document.getElementById('metricPE').textContent=data.pe_ratio||'N/A';
    document.getElementById('metricTarget').textContent=data.analyst_target?'$'+data.analyst_target:'N/A';
    document.getElementById('metricRec').textContent=data.recommendation||'N/A';
    const change=data.change_pct||0;
    const changeEl=document.getElementById('stockChange');
    changeEl.textContent=(change>=0?'+':'')+change+'% today';
    changeEl.className='price-change '+(change>=0?'up':'down');
    updateVerdictDisplay(data);
    const verdict=data.verdict||'WATCH';
    const vc=verdict==='APPROVE'?'verdict-approve-badge':verdict==='PASS'?'verdict-pass-badge':'verdict-watch-badge';
    const panel=document.getElementById('signalsPanel');
    const existing=panel.querySelector(`[data-sym="${data.symbol}"]`);
    const card=`<div class="signal-card" data-sym="${data.symbol}" onclick="tickerClick('${data.symbol}')"><div class="signal-top"><div class="signal-sym">${data.symbol}</div><div class="signal-verdict ${vc}">${verdict}</div></div><div class="signal-price">$${data.price} &nbsp; ${data.name}</div></div>`;
    if(existing){existing.outerHTML=card;}else{panel.insertAdjacentHTML('afterbegin',card);}
  } catch(e){document.getElementById('stockSymbol').textContent='ERROR';document.getElementById('stockName').textContent='Could not connect. Try again.';}
  document.getElementById('loading').classList.remove('active');
  document.getElementById('report').style.opacity='1';
}

document.getElementById('searchInput').addEventListener('keypress',function(e){if(e.key==='Enter')analyzeStock();});
buildTicker();
loadMarketBar();
</script>
</body>
</html>"""

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
        return jsonify({"symbol": symbol, "query": query, "price": current, "change_pct": change_pct, "recommendation": rec, "verdict": verdict, "name": info.get("longName", symbol), "sector": info.get("sector", "N/A"), "pe_ratio": info.get("trailingPE", "N/A"), "market_cap": info.get("marketCap", "N/A"), "analyst_target": info.get("targetMeanPrice", "N/A")})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
