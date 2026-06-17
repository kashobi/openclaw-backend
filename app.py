from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import yfinance as yf
import requests
import os
import time
import logging
import json
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
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

class NewsAgent:
    def get(self, symbol):
        cached = get_cache(f"news_{symbol}")
        if cached is not None:
            return cached
        if not FINNHUB_KEY:
            return []
        results = []
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            from_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
            url = f"https://finnhub.io/api/v1/company-news?symbol={symbol}&from={from_date}&to={today}&token={FINNHUB_KEY}"
            r = requests.get(url, timeout=10)
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
            logger.error(f"NewsAgent company: {e}")
        if not results:
            try:
                url2 = f"https://finnhub.io/api/v1/news?category=general&token={FINNHUB_KEY}"
                r2 = requests.get(url2, timeout=10)
                if r2.status_code == 200:
                    for n in r2.json()[:5]:
                        if n.get("headline"):
                            results.append({
                                "headline": n["headline"],
                                "source": n.get("source", "Market News") + " (General)",
                                "summary": n.get("summary", "")[:200],
                            })
            except Exception as e:
                logger.error(f"NewsAgent general: {e}")
        set_cache(f"news_{symbol}", results)
        return results

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
            if r.status_code == 200:
                res = [{"politician": t.get("Representative","Unknown"),"party": t.get("Party",""),"action": t.get("Transaction","Unknown"),"amount": t.get("Range",""),"date": t.get("TransactionDate","")} for t in r.json()[:10]]
                set_cache(f"cong_{symbol}", res)
                return res
        except Exception as e:
            logger.error(f"RegulatoryAgent: {e}")
        return []

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
            if r.status_code == 200:
                res = []
                for t in r.json()[:15]:
                    title = str(t.get("Title","")).upper()
                    res.append({"name": t.get("Name","Unknown"),"title": t.get("Title",""),"action": t.get("AcquiredDisposed",""),"shares": t.get("Shares",0),"price": fmt_price(t.get("Price",0)),"date": t.get("Date",""),"is_clevel": any(c in title for c in self.CLEVEL)})
                set_cache(f"ins_{symbol}", res)
                return res
        except Exception as e:
            logger.error(f"InsiderAgent: {e}")
        return []

class GeminiAgent:
    def get_live_context(self, symbol, company_name, signals):
        if not GEMINI_KEY:
            return None
        cached = get_cache(f"gemini_{symbol}")
        if cached is not None:
            return cached
        try:
            prompt = f"""You are a financial intelligence assistant for Apex Q, an educational stock market platform built for everyday people including those who have never invested before.

A user is analyzing {symbol} ({company_name}). Current signals detected: {', '.join(signals) if signals else 'mixed'}.

Provide current intelligence about {symbol} in this exact JSON format only. No markdown. No extra text. Just JSON:

{{
  "current_context": "2-3 sentences about what is happening with {symbol} right now in the market. Include any recent earnings results, major news, or significant market developments.",
  "why_it_matters": "2 sentences explaining why an everyday person with no financial background should care about what is happening with {symbol} right now.",
  "watch_for": "One specific thing to watch for in the next 30 days that could change the price direction.",
  "simple_lesson": "One sentence teaching a basic investing concept that applies directly to {symbol} right now. Write it like you are explaining to a smart teenager."
}}"""

            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_KEY}"
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.2, "maxOutputTokens": 600}
            }
            r = requests.post(url, json=payload, timeout=15)
            logger.info(f"GeminiAgent status={r.status_code} for {symbol}")
            if r.status_code == 200:
                data = r.json()
                text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
                if "```" in text:
                    parts = text.split("```")
                    for p in parts:
                        p = p.strip()
                        if p.startswith("json"):
                            p = p[4:].strip()
                        if p.startswith("{"):
                            text = p
                            break
                result = json.loads(text)
                set_cache(f"gemini_{symbol}", result)
                logger.info(f"GeminiAgent success for {symbol}: {list(result.keys())}")
                return result
        except Exception as e:
            logger.error(f"GeminiAgent error: {e}")
        return None

class Orchestrator:
    def synthesize(self, symbol, market, congressional, insider, news, gemini_ctx):
        score = 0
        reasons = []
        signals = []
        chg = market.get("change_pct", 0)
        rec = market.get("recommendation", "HOLD")
        tgt = market.get("analyst_target")
        price = market.get("price", 0)
        pe = market.get("pe_ratio", "N/A")
        beta = market.get("beta")
        name = market.get("name", symbol)

        lctx = gemini_ctx.get("current_context","") if gemini_ctx else ""
        lwhy = gemini_ctx.get("why_it_matters","") if gemini_ctx else ""
        lwatch = gemini_ctx.get("watch_for","") if gemini_ctx else ""
        llesson = gemini_ctx.get("simple_lesson","") if gemini_ctx else ""

        def enrich(base_why, base_watch, base_lesson):
            w = base_why + (" " + lwhy if lwhy else "")
            wf = base_watch + (" " + lwatch if lwatch else "")
            l = base_lesson + (" " + llesson if llesson else "")
            return w.strip(), wf.strip(), l.strip()

        # Price momentum
        if chg > 3:
            score += 3; signals.append("STRONG_MOMENTUM")
            w,wf,l = enrich(
                f"{name} moved up {chg}% in a single trading day. When a stock moves this sharply upward buyers are strongly outnumbering sellers. Institutional investors and smart money are actively accumulating shares. Think of it like a store that suddenly has a line around the block — demand is real and urgent.",
                "Check the trading volume. If today's volume is significantly higher than the 30-day average the move is more credible and more likely to continue.",
                "Strong price momentum on high volume is one of the clearest early signals that something meaningful is happening with a stock."
            )
            reasons.append({"icon":"📈","label":f"Strong Price Momentum: +{chg}%","short":f"Up {chg}% today. Strong buying pressure.","why":w,"what_to_watch":wf,"lesson":l})
        elif chg > 1:
            score += 2; signals.append("POSITIVE_MOMENTUM")
            w,wf,l = enrich(
                f"{name} is up {chg}% today. Steady positive movement shows buyers are consistently outnumbering sellers without panic buying. This is actually a healthier signal than one explosive spike.",
                "Look for this positive trend to continue over 2 to 3 consecutive days. Consistent upward movement is more reliable than a single large jump.",
                "Steady accumulation over multiple days shows sustained institutional interest rather than a one-day speculative move."
            )
            reasons.append({"icon":"📊","label":f"Positive Price Action: +{chg}%","short":f"Up {chg}% today. Buyers in control.","why":w,"what_to_watch":wf,"lesson":l})
        elif chg > 0:
            score += 1
            w,wf,l = enrich(
                f"{name} barely moved today — only {chg}%. Buyers and sellers are nearly balanced. The market has no strong opinion right now. Not a bad sign but not a green light either.",
                "Wait for a clear catalyst such as earnings news, an analyst upgrade, or insider buying activity to push this in a clear direction before acting.",
                "When a stock barely moves it means the market is undecided. Undecided markets are risky because any small piece of news can swing it either way."
            )
            reasons.append({"icon":"➡️","label":f"Slight Upward Drift: +{chg}%","short":f"Up {chg}% today. Minimal movement.","why":w,"what_to_watch":wf,"lesson":l})
        elif chg < -5:
            score -= 3; signals.append("HEAVY_SELLING")
            w,wf,l = enrich(
                f"{name} dropped {abs(chg)}% in a single day. Something spooked the market. This could be bad earnings, negative news, or large investors exiting. A drop this size means elevated risk of further decline.",
                "Find out WHY it dropped before making any decision. Check the news section below. A drop on bad earnings is fundamentally different from a drop caused by general market fear.",
                "Big single-day drops can be buying opportunities OR the start of a longer decline. Understanding the reason behind the move is always the most important first step."
            )
            reasons.append({"icon":"📉","label":f"Heavy Selling Pressure: {chg}%","short":f"Down {abs(chg)}% today. Significant selling.","why":w,"what_to_watch":wf,"lesson":l})
        elif chg < -2:
            score -= 2; signals.append("SELLING_PRESSURE")
            w,wf,l = enrich(
                f"{name} is down {abs(chg)}% today. Sellers are outnumbering buyers. This could be a temporary pullback or the start of a real trend change. Without more context this is a cautious signal.",
                "Check if the stock is holding above its recent support levels. If it breaks below recent lows the selling could accelerate significantly.",
                "Not every pullback is a crisis. Healthy stocks pull back regularly. The key question is always whether the underlying business is still strong."
            )
            reasons.append({"icon":"📉","label":f"Significant Decline: {chg}%","short":f"Down {abs(chg)}% today. Sellers in control.","why":w,"what_to_watch":wf,"lesson":l})
        else:
            score -= 1
            w,wf,l = enrich(
                f"{name} dipped {abs(chg)}% today. Small normal move that could simply be routine profit taking. Not alarming on its own but adds slightly to caution.",
                "If the stock has been trending up before this small dip it may just be a healthy breather. If it has been trending down this could confirm that direction.",
                "Small daily declines are completely normal. Every stock goes up and down every day. What matters is the overall trend over days and weeks not a single day's move."
            )
            reasons.append({"icon":"➡️","label":f"Minor Pullback: {chg}%","short":f"Down {abs(chg)}% today. Small decline.","why":w,"what_to_watch":wf,"lesson":l})

        # Analyst consensus
        if rec in ["BUY","STRONG_BUY"]:
            score += 2; signals.append("ANALYST_BUY")
            w,wf,l = enrich(
                f"Analysts at major banks and research firms spend months studying {name} — reading every financial report, talking to management, comparing it to every competitor. When they say Buy they are putting their professional reputation on the line.",
                "Check the analyst price target in the metrics section. A large gap between current price and target means analysts see significant upside still ahead.",
                "A consensus Buy rating from multiple independent research teams representing hundreds of hours of professional analysis is a signal worth respecting."
            )
            reasons.append({"icon":"✅","label":f"Wall Street Rating: {rec.replace('_',' ')}","short":"Professional analysts rate this a Buy.","why":w,"what_to_watch":wf,"lesson":l})
        elif rec in ["SELL","STRONG_SELL"]:
            score -= 2; signals.append("ANALYST_SELL")
            w,wf,l = enrich(
                f"When professionals who research {name} full time say Sell it is a serious warning. Their analysis shows the stock is likely to decline. This is not one person's opinion — it represents the consensus of multiple independent research teams.",
                "Read the news section to understand what specific concerns are driving the negative rating. Slowing growth, rising competition, heavy debt, or management issues are common reasons.",
                "Never fight the research. The people who know a company best saying sell is information that deserves serious respect even if you personally like the company's products."
            )
            reasons.append({"icon":"⛔","label":f"Wall Street Rating: {rec.replace('_',' ')}","short":"Professional analysts are negative on this stock.","why":w,"what_to_watch":wf,"lesson":l})
        else:
            w,wf,l = enrich(
                f"A Hold rating means analysts do not see a compelling reason to buy OR sell {name} right now. The stock may be fairly valued or analysts are waiting for the next earnings report before making a stronger call.",
                "A Hold can flip to Buy quickly after a strong earnings report or positive company announcement. Watch for the upcoming earnings date as a potential catalyst.",
                "Hold does not mean nothing is happening. It often means the professional community is waiting for more information before committing. This is frequently the calm before a significant move in either direction."
            )
            reasons.append({"icon":"⚠️","label":"Wall Street Rating: Hold","short":"Analysts are in wait-and-see mode.","why":w,"what_to_watch":wf,"lesson":l})

        # Price target
        if tgt and price and str(tgt) != "N/A":
            try:
                up = round(((float(tgt) - price) / price) * 100, 1)
                if up > 15:
                    score += 2
                    w,wf,l = enrich(
                        f"The average analyst price target for {name} is ${tgt}. The stock trades at ${price} today. That {up}% gap is how much additional upside analysts believe is still available based on their models of the company's future earnings and growth.",
                        "Price targets move after earnings. If the company beats expectations targets often get raised giving even more upside. If they miss targets drop.",
                        "A large gap between current price and analyst target suggests the market has not yet fully valued what analysts see in the company. That gap represents opportunity."
                    )
                    reasons.append({"icon":"🎯","label":f"{up}% Upside to Analyst Target: ${tgt}","short":f"Analysts see {up}% more room to grow from here.","why":w,"what_to_watch":wf,"lesson":l})
                elif up > 5:
                    score += 1
                    w,wf,l = enrich(
                        f"Analysts see ${tgt} as fair value for {name} versus today's price of ${price}. That is {up}% potential upside. A positive gap but not a wide one.",
                        "The closer a stock gets to its analyst target the less upside remains. At that point you need analysts to raise their targets to maintain momentum.",
                        "Price targets narrow as stocks rise. The best entry points are when a stock trades far below its consensus target with strong fundamental support."
                    )
                    reasons.append({"icon":"🎯","label":f"{up}% Upside to Target: ${tgt}","short":f"Modest {up}% upside to analyst target.","why":w,"what_to_watch":wf,"lesson":l})
                elif up < -5:
                    score -= 1
                    w,wf,l = enrich(
                        f"The analyst consensus target for {name} is ${tgt} but the stock is already at ${price} — trading {abs(up)}% ABOVE where analysts think fair value is. The stock has potentially run ahead of its fundamentals.",
                        "Stocks trading above analyst targets need consistently strong earnings just to maintain their premium. Any disappointment tends to be punished hard and fast.",
                        "When a stock trades significantly above analyst targets it is pricing in perfection. Any miss on those high expectations tends to cause sharp sudden pullbacks."
                    )
                    reasons.append({"icon":"🎯","label":f"Trading {abs(up)}% Above Analyst Target","short":"Stock is already above what analysts think it is worth.","why":w,"what_to_watch":wf,"lesson":l})
                else:
                    w,wf,l = enrich(
                        f"{name} at ${price} is right near the analyst consensus target of ${tgt}. This means analysts believe the stock is fairly priced right now. Not cheap and not expensive.",
                        "At fair value the next move depends entirely on whether the company can grow earnings faster than expected. The next earnings report becomes the critical test.",
                        "Fair value is a starting point not a ceiling. Companies that consistently beat expectations see their fair value estimate rise over time creating new upside."
                    )
                    reasons.append({"icon":"🎯","label":f"Near Analyst Target: ${tgt}","short":"Stock trading near fair value.","why":w,"what_to_watch":wf,"lesson":l})
            except:
                pass

        # PE ratio
        if pe and pe != "N/A":
            try:
                pn = float(str(pe))
                if pn < 12:
                    score += 2
                    w,wf,l = enrich(
                        f"The PE ratio of {pn:.1f} means for every dollar {name} earns you are paying ${pn:.1f}. The average stock in the market trades at about 20x earnings. This company is significantly cheaper than average — potentially a hidden gem or there may be a reason the market prices it this low.",
                        "Low PE is only positive if earnings are stable or growing. Check whether revenue and profits have been trending up or down over the last few quarters.",
                        "PE ratio simply measures how much you pay per dollar of company profit. Cheap PE plus growing earnings is one of the most powerful combinations in long term investing."
                    )
                    reasons.append({"icon":"💰","label":f"PE {pn:.1f} — Potentially Undervalued","short":f"Paying only {pn:.1f}x earnings. Market average is 20x.","why":w,"what_to_watch":wf,"lesson":l})
                elif pn < 20:
                    score += 1
                    w,wf,l = enrich(
                        f"A PE of {pn:.1f} means you are paying a fair price for {name}'s earnings. At or below the market average of 20x means you are not overpaying. Fair valuation reduces the risk that the multiple alone drags the stock down.",
                        "A reasonable PE combined with growing earnings is the ideal setup. Check whether the company has been consistently growing its earnings year over year.",
                        "The best long term investments combine reasonable valuation with growing earnings. You buy fair today and receive more value tomorrow as earnings expand."
                    )
                    reasons.append({"icon":"💰","label":f"PE {pn:.1f} — Reasonably Valued","short":"PE at or below market average. Not overpriced.","why":w,"what_to_watch":wf,"lesson":l})
                elif pn < 40:
                    w,wf,l = enrich(
                        f"A PE of {pn:.1f} means you are paying above the market average of 20x for {name}. This is acceptable for a high growth company but the growth must actually materialize to justify the premium price being paid today.",
                        "Look at the earnings growth rate. A PE of 35 with 30% earnings growth is actually reasonable. A PE of 35 with only 5% growth means you are significantly overpaying.",
                        "High PE is not automatically bad if growth justifies it. Dividing the PE by the annual earnings growth rate gives you a better picture of true value."
                    )
                    reasons.append({"icon":"📋","label":f"PE {pn:.1f} — Growth Premium","short":"Above average valuation. Growth must justify it.","why":w,"what_to_watch":wf,"lesson":l})
                elif pn < 80:
                    score -= 1
                    w,wf,l = enrich(
                        f"A PE of {pn:.1f} means you are paying nearly {int(pn)}x what {name} earns annually. This requires exceptional future growth. If growth disappoints even slightly the stock could drop sharply as investors question whether the premium is still justified.",
                        "Separate the story from the math. Is the high PE due to genuine high growth potential or is it driven primarily by excitement and momentum?",
                        "High PE stocks need consistent high growth just to maintain their valuation. When growth slows high PE stocks fall fast and hard. Position size management is critical."
                    )
                    reasons.append({"icon":"💸","label":f"PE {pn:.1f} — High Valuation","short":"Significant premium. Requires strong growth delivery.","why":w,"what_to_watch":wf,"lesson":l})
                else:
                    score -= 2
                    w,wf,l = enrich(
                        f"A PE of {pn:.1f} is extremely high. The market is pricing in years of flawless growth for {name}. Even one disappointing earnings quarter could trigger a large drop as investors reconsider whether this valuation is still justified.",
                        "At this extreme PE level the company needs to beat expectations consistently every single quarter just to stay flat. Any miss is punished severely.",
                        "Extreme valuations require extreme execution. The higher the PE the less margin for error exists. Always keep your position size smaller in extremely high PE stocks."
                    )
                    reasons.append({"icon":"💸","label":f"PE {pn:.1f} — Extreme Valuation","short":"Very expensive. Requires perfect execution.","why":w,"what_to_watch":wf,"lesson":l})
            except:
                pass

        # Beta
        if beta and beta != "N/A":
            try:
                b = float(str(beta))
                if b > 1.5:
                    w,wf,l = enrich(
                        f"Beta measures how much {name} moves relative to the overall market. A beta of {b:.2f} means when the market goes up 1% this stock tends to go up {b:.2f}%. It also drops harder when the market falls. Higher potential reward but significantly higher risk.",
                        "High beta stocks need tighter stop losses. A 10% market correction can hit a high beta stock 15 to 20% or more. Plan for that worst case scenario before entering.",
                        "Beta is your risk multiplier. High beta means bigger swings in both directions. The solution is smaller position sizes in high beta stocks to keep total portfolio risk manageable."
                    )
                    reasons.append({"icon":"🌊","label":f"Beta {b:.2f} — High Volatility Stock","short":f"Moves {b:.1f}x faster than the overall market.","why":w,"what_to_watch":wf,"lesson":l})
                elif b < 0.5:
                    w,wf,l = enrich(
                        f"A beta of {b:.2f} means {name} is significantly more stable than the overall market. It does not swing as hard in either direction. This is typical of defensive companies like utilities and dividend stocks. Lower volatility means more predictable but less exciting returns.",
                        "Low beta stocks hold up better during market downturns but tend to lag during strong bull markets. They are ideal for capital preservation not aggressive growth.",
                        "Low beta stocks are the shock absorbers of a portfolio. They reduce overall volatility and are ideal for investors who cannot stomach or cannot afford large swings in their account value."
                    )
                    reasons.append({"icon":"🛡️","label":f"Beta {b:.2f} — Low Volatility Stock","short":"Moves much less than the overall market. Stable.","why":w,"what_to_watch":wf,"lesson":l})
            except:
                pass

        # Congressional
        if congressional:
            buys = [t for t in congressional if "purchase" in str(t.get("action","")).lower()]
            sells = [t for t in congressional if "sale" in str(t.get("action","")).lower()]
            if len(buys) >= 3:
                score += 3; signals.append("CONGRESS_CLUSTER_BUY")
                pols = ", ".join([b.get("politician","Unknown") for b in buys[:3]])
                w,wf,l = enrich(
                    f"Under the STOCK Act law, every member of Congress must publicly disclose personal stock trades within 45 days. {len(buys)} politicians including {pols} recently used their own personal money to buy {name}. When multiple politicians buy the same stock simultaneously it often signals positive expectations about upcoming policy or regulatory decisions.",
                    "Check which committees these politicians sit on. A senator on the Technology or Finance committee buying a stock in that sector carries far more weight than a random member buying it.",
                    "Congressional trades are public information most people ignore. Politicians sometimes have early visibility into regulatory and policy changes that directly affect certain companies."
                )
                reasons.append({"icon":"🏛️","label":f"Congressional Cluster Buy: {len(buys)} politicians","short":"Multiple members of Congress bought this stock personally.","why":w,"what_to_watch":wf,"lesson":l})
            elif buys:
                score += 2; signals.append("CONGRESS_BUYING")
                w,wf,l = enrich(
                    f"{buys[0].get('politician','A politician')} recently purchased shares of {name} with personal money. This is a legally required public disclosure under the STOCK Act. Politicians who buy stocks in industries they regulate can signal positive policy expectations.",
                    "One politician buying is interesting. Multiple politicians buying the same stock in a short window is dramatically more significant. Watch to see if additional disclosures follow.",
                    "The STOCK Act created transparency around congressional trading. Tracking these disclosures gives everyday investors the same visibility previously only available to institutional researchers."
                )
                reasons.append({"icon":"🏛️","label":"Congressional Buying Detected","short":"A member of Congress personally bought this stock.","why":w,"what_to_watch":wf,"lesson":l})
            elif sells:
                score -= 1; signals.append("CONGRESS_SELLING")
                w,wf,l = enrich(
                    f"{len(sells)} members of Congress recently sold shares of {name}. Politicians sell for many reasons including tax planning and diversification. However when multiple politicians exit the same stock in a short period it can sometimes signal concern about upcoming regulatory or policy headwinds.",
                    "Check whether these politicians sit on committees that directly regulate this company's industry. Relevant committee members selling is much more meaningful than unrelated members selling.",
                    "Congressional selling is a weaker signal than buying but worth monitoring when multiple members exit the same position in a compressed time period."
                )
                reasons.append({"icon":"🏛️","label":f"Congressional Selling: {len(sells)} trades","short":"Politicians selling from personal portfolios.","why":w,"what_to_watch":wf,"lesson":l})

        # Insider
        if insider:
            cb = [t for t in insider if t.get("is_clevel") and t.get("action")=="A"]
            cs = [t for t in insider if t.get("is_clevel") and t.get("action")=="D"]
            if len(cb) >= 3:
                score += 4; signals.append("INSIDER_CLUSTER_BUY")
                names = ", ".join([f"{t.get('name')} ({t.get('title')})" for t in cb[:3]])
                w,wf,l = enrich(
                    f"Multiple executives including {names} recently purchased shares of {name} with their own personal money. These people see every financial report and internal projection before the public. The only reason executives buy their own stock with personal funds is because they believe the price is going higher. When three or more buy simultaneously this cluster pattern is one of the most powerful signals in all of investing.",
                    "Look at the dollar amounts purchased. A CEO spending $1 million of personal money is a dramatically stronger signal than a director spending $10,000. The size of the purchase reveals the depth of conviction.",
                    "Insider buying is the ultimate vote of confidence because you cannot fake it. Real personal money. Real conviction. When the people who know the most buy the most that deserves serious attention."
                )
                reasons.append({"icon":"💼","label":"C-Level Cluster Buy — HIGHEST CONVICTION SIGNAL","short":f"{len(cb)} executives buying their own stock simultaneously.","why":w,"what_to_watch":wf,"lesson":l})
            elif len(cb) == 2:
                score += 3; signals.append("INSIDER_CLUSTER_BUY")
                w,wf,l = enrich(
                    f"{cb[0].get('name')} ({cb[0].get('title')}) and {cb[1].get('name')} ({cb[1].get('title')}) both independently purchased shares of {name}. When two senior executives buy at the same time it shows strong internal alignment. Both independently drew the same conclusion from the same internal data.",
                    "Check the dates of both purchases. Transactions within a few days of each other are more significant than purchases weeks apart. Proximity in timing amplifies the signal.",
                    "Two executives independently buying at similar times is not coincidence. They are both drawing the same conclusion from internal data the public has not yet seen."
                )
                reasons.append({"icon":"💼","label":"Dual Executive Buy Signal","short":"Two executives bought with personal money.","why":w,"what_to_watch":wf,"lesson":l})
            elif len(cb) == 1:
                score += 2; signals.append("INSIDER_BUY")
                w,wf,l = enrich(
                    f"{cb[0].get('name')}, the {cb[0].get('title')} of {name}, recently purchased {int(cb[0].get('shares',0)):,} shares at ${cb[0].get('price')} per share with personal money. Executives receive financial data and forward projections the public does not see. When they spend personal funds on company stock it signals they see value the market has not yet recognized.",
                    "Is this a large purchase relative to their compensation level? A CEO spending the equivalent of their annual salary on stock is dramatically more meaningful than a token purchase.",
                    "Executive open market purchases are completely different from stock option grants which are compensation. Open market purchases mean they paid real money just like any other investor. That distinction matters enormously."
                )
                reasons.append({"icon":"💼","label":f"Executive Buy: {cb[0].get('title')}","short":f"{cb[0].get('name')} bought {int(cb[0].get('shares',0)):,} shares.","why":w,"what_to_watch":wf,"lesson":l})
            if len(cs) >= 2:
                score -= 2; signals.append("INSIDER_CLUSTER_SELL")
                w,wf,l = enrich(
                    f"{len(cs)} executives recently sold shares of {name}. While executives sell for legitimate reasons like diversification and taxes, heavy cluster selling from multiple officers sometimes signals positioning ahead of disappointing results. It reduces overall conviction.",
                    "Are these pre-scheduled 10b5-1 plan sales or new discretionary decisions to sell? Pre-scheduled sales are routine. Sudden new decisions to sell by multiple executives simultaneously carry much more weight.",
                    "Context is everything with insider selling. Pre-planned diversification is routine and expected. Sudden large discretionary sales by multiple executives simultaneously is the version that carries real warning weight."
                )
                reasons.append({"icon":"💼","label":f"Executive Cluster Selling: {len(cs)} officers","short":"Multiple executives selling personal holdings.","why":w,"what_to_watch":wf,"lesson":l})

        # News
        if news:
            w,wf,l = enrich(
                f"Apex Q found {len(news)} recent news articles about {name}. News drives short term stock moves more than almost anything else. A single announcement can change a stock's direction in minutes. " + (lctx if lctx else "Review the news section below to understand what is being reported about this company right now."),
                "Look for patterns across multiple articles. Is coverage consistently positive or negative? Are there recurring themes about growth, competition, regulatory risk, or management changes?",
                "News drives short term price moves. Business fundamentals drive long term value. Use news to understand the current environment and fundamentals to decide whether the price is actually fair."
            )
            reasons.append({"icon":"📰","label":f"News Intelligence: {len(news)} articles","short":"Recent news coverage detected.","why":w,"what_to_watch":wf,"lesson":l})

        # Confluence
        conf_list = [s for s in ["CONGRESS_BUYING","CONGRESS_CLUSTER_BUY","INSIDER_BUY","INSIDER_CLUSTER_BUY","ANALYST_BUY","STRONG_MOMENTUM","POSITIVE_MOMENTUM"] if s in signals]
        if len(conf_list) >= 3:
            score += 3
            w,wf,l = enrich(
                f"Rare alignment detected across {len(conf_list)} completely independent data sources: {', '.join(conf_list)}. Confluence means multiple unrelated signals are agreeing without knowing about each other. The market data, the professional analysts, and the smart money are all independently pointing the same direction simultaneously.",
                "Confluence signals are the highest conviction setups in Apex Q. Still manage your position size and set a clear stop loss. No signal is perfect and markets can always surprise.",
                "The best investment setups occur when multiple independent data sources agree at the same time. When price momentum, insider buying, and analyst upgrades all align simultaneously the probability of a positive outcome increases substantially."
            )
            reasons.append({"icon":"⚡","label":f"CONFLUENCE: {len(conf_list)} Signals Aligned","short":"Multiple independent signals all pointing the same way.","why":w,"what_to_watch":wf,"lesson":l})

        conviction = score_to_conviction(score)

        if score >= 6:
            v = "APPROVE"
            conf_txt = f"HIGH CONVICTION BUY. Market Intelligence Rating: {conviction}. Multiple independent intelligence layers confirm a strong bullish setup across price action, analyst consensus, and smart money activity."
            watch_for = ""
            what_changes = ""
        elif score >= 4:
            v = "APPROVE"
            conf_txt = f"MODERATE BUY SIGNAL. Market Intelligence Rating: {conviction}. The weight of evidence leans bullish. More signals favor the upside than the downside. Manage your position size and set a clear stop loss before entering."
            watch_for = ""
            what_changes = ""
        elif score <= -5:
            v = "PASS"
            conf_txt = f"HIGH CONVICTION AVOID. Market Intelligence Rating: {conviction}. Multiple independent signals are clearly negative. The data strongly suggests avoiding this position until conditions improve significantly."
            watch_for = "Watch for price stabilization and a clear reversal in momentum. An analyst upgrade. Or insider buying activity that signals the situation is changing for the better."
            what_changes = "For this to become WATCH: Price needs to stabilize and one or two negative signals need to flip positive. For this to become APPROVE: Multiple signals including price action and analyst consensus need to align bullishly."
        elif score <= -1:
            v = "PASS"
            conf_txt = f"CAUTION SIGNAL. Market Intelligence Rating: {conviction}. More signals are negative than positive. The risk/reward does not favor entry at current levels. Monitor for improvement before considering a position."
            watch_for = "Watch for a clear price reversal above recent highs. Positive earnings guidance from management. Or analyst upgrades signaling the professional view is shifting."
            what_changes = f"For this to become WATCH: The score needs to reach 0. Currently at {score}. An analyst upgrade adds +2. Insider buying adds +2. Those two together would flip this to WATCH immediately. For APPROVE you need a score of 4 or higher."
        else:
            v = "WATCH"
            pos_sig = [s for s in signals if "BUY" in s or "MOMENTUM" in s]
            neg_sig = [s for s in signals if "SELL" in s or "SELLING" in s or "HEAVY" in s]
            conf_txt = f"MIXED SIGNALS. Market Intelligence Rating: {conviction}. The four intelligence agents are not in full agreement on {name} right now. "
            if pos_sig and neg_sig:
                conf_txt += f"Positive signals detected: {', '.join(pos_sig)}. Negative signals pulling the score down: {', '.join(neg_sig)}. When signals conflict the market itself is undecided. Patience is the right move here."
            elif pos_sig:
                conf_txt += f"Some positive signals exist ({', '.join(pos_sig)}) but not enough combined conviction to justify an Approve signal yet. Needs more confirmation before a strong position is justified."
            elif neg_sig:
                conf_txt += f"Some negative signals present ({', '.join(neg_sig)}) but not severe enough to generate a Pass signal. Sitting on the sidelines and watching is the correct move right now."
            else:
                conf_txt += "No strong signals in either direction. The stock is in a neutral zone. Wait for a catalyst to create a clear directional move before taking action."
            watch_for = "Watch for: Strong price momentum above +2% on above-average volume. An analyst upgrade to Buy. Insider buying by a C-level executive. A congressional purchase disclosure."
            what_changes = f"For this to become APPROVE: The score needs to reach 4 or higher. Currently at {score}. You need {max(0, 4-score)} more positive signal points. An analyst upgrade adds +2. An executive buying shares adds +2. Strong price momentum adds +2 to +3. Any combination reaching the threshold flips the verdict."

        logger.info(f"Orchestrator: {symbol} verdict={v} score={score} conviction={conviction}")
        return v, conf_txt, reasons, score, signals, watch_for, what_changes, conviction

market_agent = MarketDataAgent()
news_agent = NewsAgent()
reg_agent = RegulatoryAgent()
ins_agent = InsiderAgent()
gemini_agent = GeminiAgent()
orch = Orchestrator()

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Apex Q Intelligence Terminal</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap');
:root{
  --bg:#e4ecf5;
  --surface:#ffffff;
  --s2:#d6e2ef;
  --border:#aac0d8;
  --accent:#003eaa;
  --green:#004d22;
  --gbg:#b8f0cc;
  --red:#8b0000;
  --rbg:#ffc0c0;
  --yellow:#5c2d00;
  --ybg:#ffd888;
  --text:#040e1c;
  --muted:#2a4060;
  --navy:#040e1c;
}
*{margin:0;padding:0;box-sizing:border-box;}
body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',sans-serif;}

.tkbar{background:var(--navy);height:42px;overflow:hidden;display:flex;align-items:center;}
.tkwrap{width:100%;overflow:hidden;position:relative;}
.tktrack{display:inline-flex;animation:tkscroll 70s linear infinite;white-space:nowrap;will-change:transform;}
.tktrack:hover{animation-play-state:paused;}
.tki{display:inline-flex;align-items:center;gap:10px;padding:0 24px;height:42px;font-family:'JetBrains Mono',monospace;font-size:12.5px;border-right:1px solid #182840;cursor:pointer;flex-shrink:0;transition:background .2s;}
.tki:hover{background:#182840;}
.tsym{color:#ffffff;font-weight:800;}
.tpx{color:#7aabcc;font-size:11.5px;}
.tup{color:#00ff99;font-weight:800;font-size:12px;}
.tdn{color:#ff5555;font-weight:800;font-size:12px;}
@keyframes tkscroll{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}

.hdr{background:var(--surface);border-bottom:2.5px solid var(--border);padding:14px 28px;display:flex;align-items:center;justify-content:space-between;}
.hlogo{display:flex;align-items:center;gap:14px;}
.hmark{width:46px;height:46px;background:linear-gradient(135deg,#003eaa,#001e77);border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:26px;box-shadow:0 4px 16px rgba(0,62,170,.35);}
.hname{font-size:27px;font-weight:800;letter-spacing:-.5px;color:var(--text);}
.hname span{color:var(--accent);}
.hbadge{display:flex;align-items:center;gap:8px;background:var(--gbg);border:2px solid var(--green);border-radius:20px;padding:7px 16px;font-size:11.5px;color:var(--green);font-family:'JetBrains Mono',monospace;font-weight:800;}
.hdot{width:9px;height:9px;background:var(--green);border-radius:50%;animation:pulse 1.4s infinite;}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.2}}

.mbar{background:var(--surface);border-bottom:2px solid var(--border);overflow-x:auto;scrollbar-width:none;-webkit-overflow-scrolling:touch;}
.mbar::-webkit-scrollbar{display:none;}
.mbari{display:flex;min-width:max-content;}
.mi{padding:12px 26px;border-right:1.5px solid var(--border);cursor:pointer;transition:background .2s;min-width:160px;}
.mi:hover{background:var(--s2);}
.ml{font-size:10px;color:var(--muted);font-family:'JetBrains Mono',monospace;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:5px;font-weight:800;}
.mv{font-size:15px;font-weight:800;font-family:'JetBrains Mono',monospace;}
.mv.up{color:var(--green);}
.mv.dn{color:var(--red);}
.mv.ld{color:var(--muted);font-size:12px;font-weight:500;}

.swrap{background:var(--surface);border-bottom:2px solid var(--border);padding:18px 28px 16px;}
.slbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:2px;margin-bottom:12px;font-family:'JetBrains Mono',monospace;font-weight:800;}
.srow{display:flex;gap:10px;max-width:780px;position:relative;}
.sinp{flex:1;background:var(--bg);border:2.5px solid var(--border);border-radius:12px;padding:15px 20px;color:var(--text);font-family:'Space Grotesk',sans-serif;font-size:15px;font-weight:600;outline:none;transition:all .2s;}
.sinp::placeholder{color:var(--muted);}
.sinp:focus{border-color:var(--accent);background:#fff;box-shadow:0 0 0 4px rgba(0,62,170,.1);}
.sbtn{background:var(--accent);color:#fff;border:none;border-radius:12px;padding:15px 36px;font-family:'Space Grotesk',sans-serif;font-size:14px;font-weight:800;cursor:pointer;box-shadow:0 3px 12px rgba(0,62,170,.35);}
.sbtn:hover{background:#002d88;transform:translateY(-1px);}
.ac{position:absolute;top:calc(100%+6px);left:0;right:100px;background:#fff;border:2px solid var(--border);border-radius:12px;z-index:300;display:none;box-shadow:0 10px 34px rgba(0,0,0,.14);}
.aci{padding:12px 18px;cursor:pointer;font-size:13px;font-weight:600;display:flex;gap:14px;align-items:center;border-bottom:1px solid var(--border);}
.aci:last-child{border-bottom:none;}
.aci:hover{background:var(--bg);}
.acs{font-family:'JetBrains Mono',monospace;color:var(--accent);font-weight:800;min-width:70px;}
.acn{color:var(--muted);font-size:12px;}
.qrow{display:flex;gap:8px;margin-top:12px;flex-wrap:wrap;}
.qp{background:var(--bg);border:2px solid var(--border);border-radius:20px;padding:6px 16px;font-size:11.5px;color:var(--text);cursor:pointer;transition:all .2s;font-family:'JetBrains Mono',monospace;font-weight:700;}
.qp:hover{border-color:var(--accent);color:var(--accent);background:#ddeeff;}

.main{padding:22px 28px 80px;display:grid;grid-template-columns:1fr 370px;gap:24px;}
@media(max-width:1020px){.main{grid-template-columns:1fr;padding:16px 16px 60px;}}
.stitle{font-size:10px;text-transform:uppercase;letter-spacing:2.5px;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-bottom:14px;display:flex;align-items:center;gap:10px;font-weight:800;}
.stitle::after{content:'';flex:1;height:1.5px;background:var(--border);}

.rc{background:var(--surface);border:2px solid var(--border);border-radius:16px;padding:24px;margin-bottom:20px;box-shadow:0 3px 18px rgba(0,0,0,.06);}
.shdr{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:22px;gap:16px;}
.sn{font-size:36px;font-weight:800;letter-spacing:-.5px;color:var(--text);}
.sf{font-size:14px;color:var(--muted);margin-top:4px;font-weight:600;}
.ss{font-size:10px;color:var(--accent);margin-top:6px;font-family:'JetBrains Mono',monospace;font-weight:800;text-transform:uppercase;letter-spacing:1.5px;}
.pb{text-align:right;flex-shrink:0;}
.sp{font-size:36px;font-weight:800;font-family:'JetBrains Mono',monospace;color:var(--text);}
.sc{font-size:15px;font-family:'JetBrains Mono',monospace;margin-top:4px;font-weight:800;}
.sc.up{color:var(--green);}
.sc.dn{color:var(--red);}

.vb{border-radius:16px;padding:24px;margin-bottom:22px;transition:all .3s;}
.vb.approve{background:linear-gradient(135deg,#b8f0cc,#88e0aa);border:2.5px solid var(--green);}
.vb.pass{background:linear-gradient(135deg,#ffc0c0,#ff9090);border:2.5px solid var(--red);}
.vb.watch{background:linear-gradient(135deg,#ffd888,#ffbb44);border:2.5px solid var(--yellow);}
.vtop{display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:12px;}
.vbdg{font-size:21px;font-weight:900;font-family:'JetBrains Mono',monospace;letter-spacing:4px;padding:14px 34px;border-radius:12px;box-shadow:0 3px 14px rgba(0,0,0,.2);}
.vbdg.approve{background:var(--green);color:#fff;}
.vbdg.pass{background:var(--red);color:#fff;}
.vbdg.watch{background:var(--yellow);color:#fff;}
.vsco{font-size:14px;font-family:'JetBrains Mono',monospace;color:var(--text);font-weight:800;background:rgba(255,255,255,.85);padding:9px 18px;border-radius:20px;}
.vconf{font-size:14px;color:var(--text);margin-bottom:16px;line-height:1.75;font-weight:600;background:rgba(255,255,255,.75);padding:16px 20px;border-radius:12px;}
.wguide{margin-bottom:16px;}
.wg{background:rgba(255,255,255,.8);border-radius:12px;padding:15px 18px;margin-bottom:10px;}
.wg.yellow{border-left:5px solid var(--yellow);}
.wg.blue{border-left:5px solid var(--accent);}
.wg.red{border-left:5px solid var(--red);}
.wgt{font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:1.5px;font-family:'JetBrains Mono',monospace;margin-bottom:7px;}
.wg.yellow .wgt{color:var(--yellow);}
.wg.blue .wgt{color:var(--accent);}
.wg.red .wgt{color:var(--red);}
.wgtxt{font-size:13.5px;color:var(--text);line-height:1.65;font-weight:500;}

.vrlist{display:flex;flex-direction:column;gap:10px;}
.vr{background:rgba(255,255,255,.92);border-radius:13px;overflow:hidden;border:1.5px solid rgba(0,0,0,.07);}
.vr-hdr{display:flex;align-items:center;gap:14px;padding:15px 18px;cursor:pointer;user-select:none;transition:background .2s;}
.vr-hdr:hover{background:rgba(0,62,170,.05);}
.vi{font-size:24px;flex-shrink:0;}
.vrm{flex:1;}
.vlbl{font-weight:800;display:block;color:var(--text);font-size:14px;}
.vshort{color:var(--muted);font-size:12.5px;margin-top:3px;display:block;font-weight:600;}
.vbtn{font-size:11.5px;color:var(--accent);font-family:'JetBrains Mono',monospace;font-weight:800;background:rgba(0,62,170,.12);padding:6px 14px;border-radius:20px;flex-shrink:0;border:1.5px solid rgba(0,62,170,.25);white-space:nowrap;}
.vr-body{display:none;padding:0 20px 20px 58px;border-top:1.5px solid rgba(0,0,0,.08);}
.vr-body.open{display:block;}
.vrs{margin-top:15px;}
.vrst{font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:1.5px;color:var(--accent);font-family:'JetBrains Mono',monospace;margin-bottom:7px;}
.vrstxt{font-size:13.5px;color:var(--text);line-height:1.72;font-weight:500;}
.vlesson{background:var(--s2);border-radius:11px;padding:13px 17px;margin-top:13px;border-left:4px solid var(--accent);}
.vlessont{font-size:9.5px;font-weight:800;text-transform:uppercase;letter-spacing:1.5px;color:var(--accent);font-family:'JetBrains Mono',monospace;margin-bottom:6px;}
.vlessontxt{font-size:13.5px;color:var(--text);line-height:1.65;font-style:italic;font-weight:500;}

.mets{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:22px;}
@media(max-width:600px){.mets{grid-template-columns:repeat(2,1fr);}}
.met{background:var(--s2);border-radius:12px;padding:14px;border:2px solid var(--border);}
.ml2{font-size:9.5px;color:var(--muted);text-transform:uppercase;letter-spacing:1.5px;margin-bottom:6px;font-family:'JetBrains Mono',monospace;font-weight:800;}
.mv2{font-size:18px;font-weight:800;font-family:'JetBrains Mono',monospace;color:var(--text);}
.mv2.pos{color:var(--green);}
.mv2.neg{color:var(--red);}
.mv2.neu{color:var(--accent);}

.ic{background:var(--s2);border:2px solid var(--border);border-radius:12px;padding:16px;margin-bottom:12px;}
.ih{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;}
.it{font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);font-family:'JetBrains Mono',monospace;}
.ibdg{font-size:10.5px;font-weight:800;padding:4px 13px;border-radius:20px;font-family:'JetBrains Mono',monospace;}
.bg{background:var(--gbg);color:var(--green);border:2px solid var(--green);}
.br{background:var(--rbg);color:var(--red);border:2px solid var(--red);}
.by{background:var(--ybg);color:var(--yellow);border:2px solid var(--yellow);}
.itxt{font-size:13.5px;color:var(--text);line-height:1.65;font-weight:500;}
.tr{padding:9px 0;border-bottom:1.5px solid var(--border);font-size:12.5px;display:flex;flex-wrap:wrap;gap:8px;align-items:center;}
.tr:last-child{border-bottom:none;}
.buy{color:var(--green);font-weight:800;font-family:'JetBrains Mono',monospace;}
.sell{color:var(--red);font-weight:800;font-family:'JetBrains Mono',monospace;}
.gray{color:var(--muted);font-size:11.5px;font-weight:500;}

.ni{padding:13px 0;border-bottom:1.5px solid var(--border);}
.ni:last-child{border-bottom:none;}
.ns{font-size:10px;color:var(--accent);font-family:'JetBrains Mono',monospace;text-transform:uppercase;font-weight:800;margin-bottom:5px;letter-spacing:1px;}
.nh{font-size:14px;color:var(--text);line-height:1.55;font-weight:700;}
.nsum{font-size:12px;color:var(--muted);margin-top:4px;line-height:1.5;font-weight:500;}

.loading{display:none;padding:50px 30px;text-align:center;color:var(--accent);font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:800;}
.loading.on{display:block;animation:pulse 1.2s infinite;}
.lsteps{margin:18px auto 0;display:flex;flex-direction:column;gap:9px;max-width:360px;text-align:left;}
.ls{font-size:12.5px;color:var(--muted);display:flex;align-items:center;gap:10px;font-weight:600;}

.sg{background:var(--surface);border:2px solid var(--border);border-radius:14px;padding:16px;margin-bottom:12px;cursor:pointer;transition:all .2s;box-shadow:0 2px 10px rgba(0,0,0,.05);}
.sg:hover{border-color:var(--accent);box-shadow:0 6px 22px rgba(0,62,170,.14);transform:translateY(-1px);}
.sgtop{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;}
.sgsym{font-size:19px;font-weight:800;font-family:'JetBrains Mono',monospace;color:var(--text);}
.sgv{font-size:11.5px;font-weight:800;padding:4px 13px;border-radius:20px;font-family:'JetBrains Mono',monospace;}
.va{background:var(--gbg);color:var(--green);border:2px solid var(--green);}
.vp{background:var(--rbg);color:var(--red);border:2px solid var(--red);}
.vw{background:var(--ybg);color:var(--yellow);border:2px solid var(--yellow);}
.sgi{font-size:12px;font-family:'JetBrains Mono',monospace;color:var(--muted);font-weight:600;}

.foot{background:var(--navy);padding:26px 28px;text-align:center;font-size:11.5px;color:#5a80aa;font-family:'JetBrains Mono',monospace;line-height:2.2;}
</style>
</head>
<body>

<div class="tkbar">
  <div class="tkwrap">
    <div class="tktrack" id="tktrack">
      <span class="tki"><span class="tsym">APEX Q</span><span class="tpx">Loading live market data...</span></span>
    </div>
  </div>
</div>

<div class="hdr">
  <div class="hlogo">
    <div class="hmark">&#9889;</div>
    <div class="hname">Apex <span>Q</span></div>
  </div>
  <div class="hbadge"><div class="hdot"></div>LIVE INTEL ACTIVE</div>
</div>

<div class="mbar">
  <div class="mbari">
    <div class="mi" onclick="go('^GSPC')"><div class="ml">S&amp;P 500</div><div class="mv ld" id="m0">--</div></div>
    <div class="mi" onclick="go('^IXIC')"><div class="ml">NASDAQ</div><div class="mv ld" id="m1">--</div></div>
    <div class="mi" onclick="go('^DJI')"><div class="ml">DOW JONES</div><div class="mv ld" id="m2">--</div></div>
    <div class="mi" onclick="go('^RUT')"><div class="ml">RUSSELL 2000</div><div class="mv ld" id="m3">--</div></div>
    <div class="mi" onclick="go('^VIX')"><div class="ml">VIX FEAR</div><div class="mv ld" id="m4">--</div></div>
    <div class="mi" onclick="go('GC=F')"><div class="ml">GOLD FUTURES</div><div class="mv ld" id="m5">--</div></div>
    <div class="mi" onclick="go('CL=F')"><div class="ml">OIL WTI</div><div class="mv ld" id="m6">--</div></div>
    <div class="mi" onclick="go('BTC-USD')"><div class="ml">BITCOIN</div><div class="mv ld" id="m7">--</div></div>
  </div>
</div>

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
    <div class="loading" id="lb">
      Running intelligence agents + Gemini live context...
      <div class="lsteps">
        <div class="ls">&#128202; Analyst Agent — price, fundamentals, valuation</div>
        <div class="ls">&#127963; Regulatory Agent — congressional trades</div>
        <div class="ls">&#128188; Insider Agent — C-level buy and sell activity</div>
        <div class="ls">&#128240; News Agent — Finnhub live intelligence</div>
        <div class="ls">&#129302; Gemini — pulling live market context</div>
        <div class="ls">&#9889; Synthesis Engine — building your verdict</div>
      </div>
    </div>
    <div id="rpt" class="rc">
      <div class="shdr">
        <div>
          <div class="sn" id="sym">APEX Q</div>
          <div class="sf" id="sfull">Search a stock above to begin</div>
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
          <div class="vsco" id="vsco">Market Intelligence Rating: --</div>
        </div>
        <div class="vconf" id="vconf">Search any stock or company name above. Four intelligence agents run simultaneously. Tap any signal card below to expand the full WHY, What To Watch For, and The Lesson.</div>
        <div class="wguide" id="wguide"></div>
        <div class="vrlist" id="vrl">
          <div class="vr"><div class="vr-hdr"><span class="vi">&#128202;</span><div class="vrm"><span class="vlbl">Analyst Agent</span><span class="vshort">Price momentum, valuation, analyst consensus, price target</span></div><span class="vbtn">&#9660; LEARN WHY</span></div></div>
          <div class="vr"><div class="vr-hdr"><span class="vi">&#127963;</span><div class="vrm"><span class="vlbl">Regulatory Agent</span><span class="vshort">Congressional stock trades via Quiver Quantitative</span></div><span class="vbtn">&#9660; LEARN WHY</span></div></div>
          <div class="vr"><div class="vr-hdr"><span class="vi">&#128188;</span><div class="vrm"><span class="vlbl">Insider Agent</span><span class="vshort">C-level executive buy and sell detection</span></div><span class="vbtn">&#9660; LEARN WHY</span></div></div>
          <div class="vr"><div class="vr-hdr"><span class="vi">&#128240;</span><div class="vrm"><span class="vlbl">News Agent</span><span class="vshort">Finnhub live news intelligence</span></div><span class="vbtn">&#9660; LEARN WHY</span></div></div>
        </div>
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
  APEX Q INTELLIGENCE TERMINAL &nbsp;|&nbsp; ANALYST AGENT &bull; REGULATORY AGENT &bull; INSIDER AGENT &bull; NEWS AGENT &bull; GEMINI LIVE CONTEXT &bull; SYNTHESIS ENGINE<br><br>
  The insights provided are generated by our analytical engine for educational and illustrative purposes only.<br>
  They are not intended as financial, investment, or legal advice. Every market participant is unique.<br>
  We encourage you to perform your own due diligence or consult with a qualified professional before making any financial decisions.
</div>

<script>
const A = window.location.origin;
const TKS = ['AAPL','MSFT','NVDA','AMD','TSLA','AMZN','GOOGL','META','SOFI','SPCX','SCHD','JPM','BAC','NFLX','BTC-USD'];
const MKT = [
  {s:'^GSPC',id:'m0'},{s:'^IXIC',id:'m1'},{s:'^DJI',id:'m2'},{s:'^RUT',id:'m3'},
  {s:'^VIX',id:'m4'},{s:'GC=F',id:'m5'},{s:'CL=F',id:'m6'},{s:'BTC-USD',id:'m7'}
];

function fmt(n){
  if(!n||n==='N/A')return 'N/A';
  const x=parseFloat(n);
  if(isNaN(x))return 'N/A';
  if(x>=1e12)return '$'+(x/1e12).toFixed(2)+'T';
  if(x>=1e9)return '$'+(x/1e9).toFixed(2)+'B';
  if(x>=1e6)return '$'+(x/1e6).toFixed(2)+'M';
  return '$'+x.toLocaleString();
}

function buildCard(r, idx){
  const bid = 'vb'+idx;
  let body = '';
  if(r.why) body += '<div class="vrs"><div class="vrst">&#10067; Why This Matters</div><div class="vrstxt">'+r.why+'</div></div>';
  if(r.what_to_watch) body += '<div class="vrs"><div class="vrst">&#128064; What To Watch For</div><div class="vrstxt">'+r.what_to_watch+'</div></div>';
  if(r.lesson) body += '<div class="vlesson"><div class="vlessont">&#127891; The Lesson</div><div class="vlessontxt">'+r.lesson+'</div></div>';
  return '<div class="vr"><div class="vr-hdr" onclick="toggle(\''+bid+'\',this)"><span class="vi">'+r.icon+'</span><div class="vrm"><span class="vlbl">'+r.label+'</span><span class="vshort">'+(r.short||'')+'</span></div><span class="vbtn" id="b'+bid+'">&#9660; LEARN WHY</span></div><div class="vr-body" id="'+bid+'">'+body+'</div></div>';
}

function toggle(bid, hdr){
  const body = document.getElementById(bid);
  const btn = document.getElementById('b'+bid);
  if(!body || !btn) return;
  const open = body.classList.toggle('open');
  btn.innerHTML = open ? '&#9650; CLOSE' : '&#9660; LEARN WHY';
  hdr.style.background = open ? 'rgba(0,62,170,0.06)' : '';
}

function renderCong(data){
  const s = document.getElementById('cong');
  if(!data||!data.length){
    s.innerHTML='<div class="ic"><div class="ih"><div class="it">Congressional Trading</div><div class="ibdg bg">CLEAN</div></div><div class="itxt">No recent congressional trading disclosures found for this stock. No politicians have publicly reported buying or selling this company recently.</div></div>';
    return;
  }
  const buys=data.filter(t=>t.action&&t.action.toLowerCase().includes('purchase'));
  const sells=data.filter(t=>t.action&&t.action.toLowerCase().includes('sale'));
  const bc=buys.length>sells.length?'bg':sells.length>buys.length?'br':'by';
  const bl=buys.length>sells.length?'BUYING ('+buys.length+')':sells.length>buys.length?'SELLING ('+sells.length+')':'MIXED';
  s.innerHTML='<div class="ic"><div class="ih"><div class="it">Congressional Trades — '+data.length+' total</div><div class="ibdg '+bc+'">'+bl+'</div></div><div class="itxt">'+data.map(t=>'<div class="tr"><span class="'+(t.action&&t.action.toLowerCase().includes('purchase')?'buy':'sell')+'">'+( t.action||'?')+'</span><span>'+( t.politician||'Unknown')+'</span><span class="gray">('+( t.party||'')+')</span><span class="gray">'+( t.amount||'')+'</span><span class="gray">'+( t.date||'')+'</span></div>').join('')+'</div></div>';
}

function renderIns(data){
  const s = document.getElementById('ins');
  if(!data||!data.length){
    s.innerHTML='<div class="ic"><div class="ih"><div class="it">Insider Activity</div><div class="ibdg bg">CLEAN</div></div><div class="itxt">No recent insider trading filings found. Executives and directors have not reported unusual personal trading activity in this company recently.</div></div>';
    return;
  }
  const buys=data.filter(t=>t.action==='A');
  const sells=data.filter(t=>t.action==='D');
  const cb=data.filter(t=>t.is_clevel&&t.action==='A');
  const bc=buys.length>sells.length?'bg':sells.length>buys.length?'br':'by';
  let lbl=buys.length>sells.length?'BUYING':'SELLING';
  if(cb.length>=2) lbl='CLUSTER BUY &#9889;';
  s.innerHTML='<div class="ic"><div class="ih"><div class="it">Insider Trades — '+data.length+' filings</div><div class="ibdg '+bc+'">'+lbl+'</div></div><div class="itxt">'+data.slice(0,8).map(t=>'<div class="tr"><span class="'+(t.action==='A'?'buy':'sell')+'">'+(t.action==='A'?'BUY':'SELL')+'</span><span>'+(t.name||'Unknown')+'</span><span class="gray">'+(t.title||'')+'</span>'+(t.shares?'<span class="gray">'+parseInt(t.shares).toLocaleString()+' shares</span>':'')+'<span class="gray">'+(t.date||'')+'</span></div>').join('')+'</div></div>';
}

function renderNews(data){
  const s = document.getElementById('news');
  if(!data||!data.length){
    s.innerHTML='<div class="ic"><div class="ih"><div class="it">Finnhub News Feed</div><div class="ibdg by">NO RESULTS</div></div><div class="itxt">No news articles found for this stock in the last 60 days. Low media coverage is not automatically negative. Some strong opportunities exist in overlooked companies not yet in the headlines.</div></div>';
    return;
  }
  const isGen=data.some(n=>n.source&&n.source.includes('General'));
  const prefix = isGen ? '<div class="ic" style="margin-bottom:10px"><div class="ih"><div class="it">General Market News</div><div class="ibdg by">NO COMPANY NEWS</div></div><div class="itxt" style="font-size:12px">No company-specific news in last 60 days. Showing general market news.</div></div>' : '';
  s.innerHTML=prefix+data.map(n=>'<div class="ni"><div class="ns">'+(n.source||'News')+'</div><div class="nh">'+n.headline+'</div>'+(n.summary?'<div class="nsum">'+n.summary+'</div>':'')+'</div>').join('');
}

async function loadTicker(sym){
  try{
    const r=await fetch(A+'/analyze?symbol='+encodeURIComponent(sym));
    const d=await r.json();
    if(d.price){
      const up=d.change_pct>=0;
      return '<span class="tki" onclick="go(\''+sym+'\')"><span class="tsym">'+d.symbol+'</span><span class="tpx">$'+d.price.toLocaleString()+'</span><span class="'+(up?'tup':'tdn')+'">'+(up?'+':'')+d.change_pct+'%</span></span>';
    }
  }catch(e){}
  return '';
}

async function buildTicker(){
  const tk=document.getElementById('tktrack');
  let h='';
  for(const s of TKS) h+=await loadTicker(s);
  if(h) tk.innerHTML=h+h;
}

async function loadMarket(){
  for(const m of MKT){
    const el=document.getElementById(m.id);
    try{
      const r=await fetch(A+'/analyze?symbol='+encodeURIComponent(m.s));
      const d=await r.json();
      if(d.price&&el){
        el.textContent=d.price.toLocaleString()+' ('+(d.change_pct>=0?'+':'')+d.change_pct+'%)';
        el.className='mv '+(d.change_pct>=0?'up':'dn');
      }else if(el){el.textContent='N/A';el.className='mv ld';}
    }catch(e){if(el){el.textContent='N/A';el.className='mv ld';}}
  }
}

function go(sym){document.getElementById('si').value=sym;run();}

let acT;
document.getElementById('si').addEventListener('input',function(){
  clearTimeout(acT);
  const v=this.value.trim();
  if(v.length<2){document.getElementById('ac').style.display='none';return;}
  acT=setTimeout(function(){suggest(v);},300);
});

async function suggest(q){
  try{
    const r=await fetch(A+'/search?q='+encodeURIComponent(q));
    const d=await r.json();
    const ac=document.getElementById('ac');
    if(d.results&&d.results.length){
      ac.innerHTML=d.results.map(x=>'<div class="aci" onclick="go(\''+x.symbol+'\')"><span class="acs">'+x.symbol+'</span><span class="acn">'+(x.name||'')+'</span></div>').join('');
      ac.style.display='block';
    }else ac.style.display='none';
  }catch(e){}
}

document.addEventListener('click',function(e){if(!e.target.closest('.srow'))document.getElementById('ac').style.display='none';});
document.getElementById('si').addEventListener('keypress',function(e){if(e.key==='Enter')run();});

async function run(){
  const val=document.getElementById('si').value.trim();
  if(!val)return;
  document.getElementById('ac').style.display='none';
  document.getElementById('lb').classList.add('on');
  document.getElementById('rpt').style.opacity='.3';
  try{
    const r=await fetch(A+'/analyze?symbol='+encodeURIComponent(val));
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
    document.getElementById('vconf').textContent=d.confidence||'';
    const wg=document.getElementById('wguide');
    if(v==='WATCH'&&d.watch_for){
      wg.innerHTML='<div class="wg yellow"><div class="wgt">&#128064; What To Watch For</div><div class="wgtxt">'+d.watch_for+'</div></div><div class="wg blue"><div class="wgt">&#9889; What Changes The Verdict</div><div class="wgtxt">'+(d.what_changes||'')+'</div></div>';
    }else if(v==='PASS'&&d.watch_for){
      wg.innerHTML='<div class="wg red"><div class="wgt">&#128064; Watch For Recovery Signs</div><div class="wgtxt">'+d.watch_for+'</div></div><div class="wg blue"><div class="wgt">&#9889; What Changes The Verdict</div><div class="wgtxt">'+(d.what_changes||'')+'</div></div>';
    }else{
      wg.innerHTML='';
    }
    if(d.reasons&&d.reasons.length){
      document.getElementById('vrl').innerHTML=d.reasons.map(function(r,i){return buildCard(r,i);}).join('');
    }
    renderCong(d.congressional||[]);
    renderIns(d.insider||[]);
    renderNews(d.news||[]);
    const vc=v==='APPROVE'?'va':v==='PASS'?'vp':'vw';
    const panel=document.getElementById('panel');
    const ex=panel.querySelector('[data-s="'+d.symbol+'"]');
    const card='<div class="sg" data-s="'+d.symbol+'" onclick="go(\''+d.symbol+'\')"><div class="sgtop"><div class="sgsym">'+d.symbol+'</div><div class="sgv '+vc+'">'+v+'</div></div><div class="sgi">$'+(d.price||0).toLocaleString()+' &nbsp;|&nbsp; '+(d.conviction||'')+' &nbsp;|&nbsp; '+d.name+'</div></div>';
    if(ex) ex.outerHTML=card;
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
        s = yf.Search(q, max_results=6)
        return jsonify({"results":[{"symbol":x.get("symbol"),"name":x.get("longname") or x.get("shortname")} for x in s.quotes if x.get("symbol")]})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route("/analyze")
def analyze():
    query = request.args.get("symbol","").strip()
    if not query:
        return jsonify({"error":"No symbol provided"}),400
    symbol = resolve_ticker(query)
    logger.info(f"ANALYZE: {query} -> {symbol}")
    market = market_agent.get(symbol)
    if not market:
        return jsonify({"error":f"No data found for {symbol}."}),404
    news = news_agent.get(symbol)
    congressional = reg_agent.get_congressional(symbol)
    insider = ins_agent.get(symbol)
    sig_preview = []
    if market.get("recommendation") in ["BUY","STRONG_BUY"]:
        sig_preview.append("ANALYST_BUY")
    if market.get("change_pct",0) > 2:
        sig_preview.append("STRONG_MOMENTUM")
    if any(t.get("is_clevel") and t.get("action")=="A" for t in insider):
        sig_preview.append("INSIDER_BUY")
    gemini_ctx = gemini_agent.get_live_context(symbol, market.get("name",symbol), sig_preview)
    verdict, confidence, reasons, score, signals, watch_for, what_changes, conviction = orch.synthesize(symbol, market, congressional, insider, news, gemini_ctx)
    return jsonify({
        "symbol": symbol,
        "price": market["price"],
        "change_pct": market["change_pct"],
        "recommendation": market["recommendation"],
        "verdict": verdict,
        "confidence": confidence,
        "score": score,
        "conviction": conviction,
        "signals": signals,
        "reasons": reasons,
        "watch_for": watch_for,
        "what_changes": what_changes,
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
