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
            logger.error(f"NewsAgent: {e}")
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
                                "url": n.get("url", ""),
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
        vol = market.get("volume", 0)
        high52 = market.get("52w_high")
        low52 = market.get("52w_low")
        beta = market.get("beta")

        # Price momentum with full WHY
        if chg > 3:
            score += 3; signals.append("STRONG_MOMENTUM")
            reasons.append({
                "icon":"&#128200;","label":f"Strong Price Momentum: +{chg}%",
                "short":f"Up {chg}% today. Strong buying pressure detected.",
                "why":f"The stock moved up {chg}% in a single day. This tells us that buyers are strongly outnumbering sellers right now. When demand is this high it usually means institutional investors or smart money is accumulating shares. Think of it like a store that suddenly has a long line outside — something good is happening inside.",
                "what_to_watch":"Watch to see if the volume is above average. High price movement on high volume is more meaningful than a move on low volume.",
                "lesson":"Price momentum is the speed and direction a stock is moving. Strong upward momentum on high volume is one of the clearest signals that buyers are confident."
            })
        elif chg > 1:
            score += 2; signals.append("POSITIVE_MOMENTUM")
            reasons.append({
                "icon":"&#128202;","label":f"Positive Price Action: +{chg}%",
                "short":f"Up {chg}% today. Buyers are in control.",
                "why":f"The stock is up {chg}% today. This is a moderate positive move. Buyers are outnumbering sellers but not dramatically. It shows steady demand without panic buying.",
                "what_to_watch":"Look for this trend to continue over 2 to 3 days. A stock that moves up steadily is healthier than one that spikes and crashes.",
                "lesson":"Consistent positive price action over multiple days is more reliable than a single big jump. Steady accumulation by buyers is a healthier signal."
            })
        elif chg > 0:
            score += 1
            reasons.append({
                "icon":"&#10145;","label":f"Slight Upward Drift: +{chg}%",
                "short":f"Up {chg}% today. Minimal movement.",
                "why":f"The stock barely moved today — only {chg}%. This means the market has no strong opinion right now. Buyers and sellers are roughly balanced. It is not a bad sign but it is not a green light either.",
                "what_to_watch":"Wait for a catalyst like earnings news, an analyst upgrade, or insider buying to push this stock in a clear direction before acting.",
                "lesson":"When a stock barely moves it means the market is undecided. Undecided markets are risky to trade because a small piece of news can swing it either way."
            })
        elif chg < -5:
            score -= 3; signals.append("HEAVY_SELLING")
            reasons.append({
                "icon":"&#128201;","label":f"Heavy Selling Pressure: {chg}%",
                "short":f"Down {abs(chg)}% today. Sellers are panicking.",
                "why":f"The stock dropped {abs(chg)}% in a single day. This is a significant move down. Something spooked the market — it could be bad earnings, negative news, or large investors selling their position. A drop this size means the risk of further decline is elevated.",
                "what_to_watch":"Find out WHY it dropped before making any decision. Check the news section below. A drop on bad earnings is different from a drop because of general market fear.",
                "lesson":"Big single-day drops can be buying opportunities OR the start of a longer decline. The key is understanding the reason behind the drop before acting."
            })
        elif chg < -2:
            score -= 2; signals.append("SELLING_PRESSURE")
            reasons.append({
                "icon":"&#128201;","label":f"Significant Decline: {chg}%",
                "short":f"Down {abs(chg)}% today. Sellers are in control.",
                "why":f"The stock is down {abs(chg)}% today. Sellers are outnumbering buyers. This could be a temporary pullback after a run up or the start of a real trend change. Without more information this is a cautious signal.",
                "what_to_watch":"Check if the stock is still above its 50-day moving average. If it is the pullback may be temporary. If it breaks below that level the selling could continue.",
                "lesson":"Not every pullback is a crisis. Healthy stocks pull back and recover all the time. The question is whether the underlying business is still strong."
            })
        else:
            score -= 1
            reasons.append({
                "icon":"&#10145;","label":f"Minor Pullback: {chg}%",
                "short":f"Down {abs(chg)}% today. Small decline.",
                "why":f"The stock dipped {abs(chg)}% today. This is a small move down that could simply be profit taking after a recent run. It is not alarming on its own but combined with other negative signals it adds to the caution.",
                "what_to_watch":"If the stock has been trending up before this dip it may just be a breather. If it has been trending down this could confirm the direction.",
                "lesson":"Small daily declines are normal. Every stock goes up and down every day. What matters is the overall trend over days and weeks not a single day move."
            })

        # Analyst consensus with full WHY
        if rec in ["BUY","STRONG_BUY"]:
            score += 2; signals.append("ANALYST_BUY")
            reasons.append({
                "icon":"&#9989;","label":f"Wall Street Rating: {rec.replace('_',' ')}",
                "short":"Professional analysts rate this stock a Buy.",
                "why":"Analysts at major banks and research firms spend months studying this company — reading every financial report, talking to management, and comparing it to competitors. When they say Buy it means their math shows the stock should go higher from here. They are putting their professional reputation behind this call.",
                "what_to_watch":f"Check the analyst price target below. If the target is significantly above the current price that tells you how much upside analysts think is still available.",
                "lesson":"Analyst ratings are not perfect but they represent hundreds of hours of professional research. A Buy rating from multiple analysts independently is a meaningful signal."
            })
        elif rec in ["SELL","STRONG_SELL"]:
            score -= 2; signals.append("ANALYST_SELL")
            reasons.append({
                "icon":"&#9940;","label":f"Wall Street Rating: {rec.replace('_',' ')}",
                "short":"Professional analysts are negative on this stock.",
                "why":"When professional analysts who study a company full time say Sell it is a serious warning. It means their analysis shows the stock is likely to decline from here. This is not one person's opinion — it is the consensus of multiple research teams.",
                "what_to_watch":"Read the news section to understand what concerns are driving the sell rating. Is it slowing growth, competition, debt problems, or something else?",
                "lesson":"Never fight the research. If analysts who know a company better than anyone else are saying sell that warrants serious respect even if you like the company."
            })
        else:
            reasons.append({
                "icon":"&#9888;","label":"Wall Street Rating: Hold",
                "short":"Analysts are neutral on this stock right now.",
                "why":"A Hold rating means analysts do not see a compelling reason to buy OR sell right now. The stock might be fairly valued or analysts might be waiting to see how the next earnings report goes before making a stronger call.",
                "what_to_watch":"A Hold can become a Buy quickly if the company surprises to the upside on earnings or announces good news. Watch for an upcoming earnings date.",
                "lesson":"Hold does not mean nothing is happening. It means the professional community is in a wait and see mode. This is often the calm before a move in either direction."
            })

        # Price target with WHY
        if tgt and price and str(tgt) != "N/A":
            try:
                up = round(((float(tgt) - price) / price) * 100, 1)
                if up > 15:
                    score += 2
                    reasons.append({
                        "icon":"&#127919;","label":f"{up}% Upside to Analyst Target: ${tgt}",
                        "short":f"Analysts see {up}% upside from current price.",
                        "why":f"The average analyst price target is ${tgt}. The stock trades at ${price} today. That gap of {up}% is the upside analysts believe is available based on their models of the company's future earnings and growth. This is a meaningful positive signal.",
                        "what_to_watch":"Price targets can change after earnings. If the company beats expectations targets often move higher. If they miss targets drop.",
                        "lesson":"A price target is an analyst's best estimate of what a stock is worth based on the business fundamentals. A large gap between current price and target suggests undervaluation."
                    })
                elif up > 5:
                    score += 1
                    reasons.append({
                        "icon":"&#127919;","label":f"{up}% Upside to Target: ${tgt}",
                        "short":f"Modest {up}% upside to analyst target.",
                        "why":f"Analysts see ${tgt} as fair value versus today's price of ${price}. That is {up}% potential upside. It is not a huge gap but it confirms analysts still see some room to grow from here.",
                        "what_to_watch":"The closer a stock gets to its analyst target the less upside remains. At that point you need the analyst to raise the target or the stock plateaus.",
                        "lesson":"Price targets narrow as stocks rise. The sweet spot for buying is when a stock is far below its target with strong fundamental support."
                    })
                elif up < -5:
                    score -= 1
                    reasons.append({
                        "icon":"&#127919;","label":f"Trading {abs(up)}% Above Target",
                        "short":f"Stock is above what analysts think it is worth.",
                        "why":f"The analyst consensus target is ${tgt} but the stock is at ${price} — meaning it is already trading {abs(up)}% ABOVE what analysts think it is worth. This is a valuation warning. The stock could be overextended.",
                        "what_to_watch":"Sometimes stocks trade above targets temporarily because of momentum or excitement. But eventually prices tend to return to where the fundamentals say they should be.",
                        "lesson":"When a stock trades significantly above analyst targets it is pricing in perfection. Any disappointment can cause a sharp pullback."
                    })
                else:
                    reasons.append({
                        "icon":"&#127919;","label":f"Near Analyst Target: ${tgt}",
                        "short":f"Stock is trading near fair value.",
                        "why":f"The stock at ${price} is right near the analyst consensus target of ${tgt}. This means analysts believe the stock is fairly priced right now — not cheap and not expensive.",
                        "what_to_watch":"At fair value the next move depends on whether the company can grow its earnings faster than expected. Watch the next earnings report closely.",
                        "lesson":"Fair value is a starting point not an ending point. Companies that consistently beat expectations see their fair value rise over time."
                    })
            except:
                pass

        # PE ratio with full WHY
        if pe and pe != "N/A":
            try:
                pn = float(str(pe))
                if pn < 12:
                    score += 2
                    reasons.append({
                        "icon":"&#128176;","label":f"PE {pn:.1f} — Potentially Undervalued",
                        "short":f"You are paying only {pn:.1f}x earnings. Market average is 20x.",
                        "why":f"The PE ratio of {pn:.1f} means for every dollar this company earns you are paying ${pn:.1f}. The average stock in the market trades at about 20x earnings. This company is significantly cheaper than average which could mean it is a hidden gem — or it could mean the market knows something negative. Research is required.",
                        "what_to_watch":"A low PE is only good if the company's earnings are stable or growing. If earnings are falling a low PE can become a high PE very quickly.",
                        "lesson":"PE ratio measures how much you pay per dollar of profit. Cheap PE means either the stock is undervalued and about to be discovered or there is a real problem most people already know about."
                    })
                elif pn < 20:
                    score += 1
                    reasons.append({
                        "icon":"&#128176;","label":f"PE {pn:.1f} — Reasonably Valued",
                        "short":f"PE of {pn:.1f} is at or below market average.",
                        "why":f"A PE of {pn:.1f} means you are paying a reasonable price for this company's earnings. The market average is around 20x so this stock is not expensive. You are getting earnings at a fair price which reduces the risk that the valuation alone pushes the stock down.",
                        "what_to_watch":"A reasonable PE plus earnings growth is the ideal combination. Check whether the company is growing its earnings year over year.",
                        "lesson":"The best investments combine a reasonable PE with growing earnings. That combination means you buy fair today and get more value tomorrow."
                    })
                elif pn < 40:
                    reasons.append({
                        "icon":"&#128203;","label":f"PE {pn:.1f} — Moderate Premium",
                        "short":f"PE of {pn:.1f} is above average. Growth must justify it.",
                        "why":f"A PE of {pn:.1f} means you are paying above the market average of 20x. This is acceptable for a high growth company where earnings are expected to expand significantly. The question is whether the growth actually materializes to justify the premium price.",
                        "what_to_watch":"Look at the earnings growth rate. A PE of 35 with 30% earnings growth is actually reasonable. A PE of 35 with 5% growth is expensive.",
                        "lesson":"High PE is not automatically bad if growth supports it. The PEG ratio divides PE by growth rate and gives a better picture of true value."
                    })
                elif pn < 80:
                    score -= 1
                    reasons.append({
                        "icon":"&#128184;","label":f"PE {pn:.1f} — High Valuation",
                        "short":f"PE of {pn:.1f} is significantly above market average.",
                        "why":f"A PE of {pn:.1f} means you are paying nearly {int(pn)}x what the company earns annually. This requires exceptional future growth to be justified. If growth disappoints even slightly the stock could drop sharply because investors will not pay this premium anymore.",
                        "what_to_watch":"What is driving the high PE? Is it a temporary dip in earnings that will recover? Or is the stock just expensive because of hype? Separate the story from the math.",
                        "lesson":"High PE stocks are called growth stocks for a reason. They need growth to survive their valuation. When growth slows high PE stocks can crash fast and hard."
                    })
                else:
                    score -= 2
                    reasons.append({
                        "icon":"&#128184;","label":f"PE {pn:.1f} — Extreme Valuation",
                        "short":f"PE of {pn:.1f} requires perfect execution to justify.",
                        "why":f"A PE of {pn:.1f} is extremely high. This means the market is pricing in years of perfect growth. Even one bad earnings quarter could cause a significant drop because investors will question whether the company can grow into this valuation. This level of PE requires conviction in the growth story.",
                        "what_to_watch":"At this PE level the stock needs to beat expectations consistently just to stay flat. Any miss on earnings guidance and the stock will be punished hard.",
                        "lesson":"Extreme valuations require extreme execution. The higher the PE the less margin for error the company has. Manage position size carefully with high PE stocks."
                    })
            except:
                pass

        # Beta context
        if beta and beta != "N/A":
            try:
                b = float(str(beta))
                if b > 1.5:
                    reasons.append({
                        "icon":"&#127774;","label":f"Beta {b:.2f} — High Volatility Stock",
                        "short":f"This stock moves {b:.1f}x as much as the overall market.",
                        "why":f"Beta measures how much a stock moves relative to the overall market. A beta of {b:.2f} means when the market goes up 1% this stock tends to go up {b:.2f}%. When the market drops 1% this stock tends to drop {b:.2f}%. Higher reward but higher risk.",
                        "what_to_watch":"High beta stocks need tighter stop losses. A market correction can hit these stocks much harder than the overall index.",
                        "lesson":"Beta is your risk multiplier. High beta means bigger swings in both directions. Size your position smaller in high beta stocks to manage risk."
                    })
                elif b < 0.5:
                    reasons.append({
                        "icon":"&#128739;","label":f"Beta {b:.2f} — Low Volatility Stock",
                        "short":f"This stock moves less than the overall market.",
                        "why":f"A beta of {b:.2f} means this stock is much more stable than the overall market. It does not swing as hard in either direction. This is typically seen in defensive companies like utilities, consumer staples, and dividend stocks. Lower risk but also lower reward.",
                        "what_to_watch":"Low beta stocks are good for capital preservation. They tend to hold up better during market downturns but lag during bull markets.",
                        "lesson":"Low beta stocks are the shock absorbers of a portfolio. They reduce overall volatility. Good for investors who cannot stomach big swings."
                    })
            except:
                pass

        # Congressional with full WHY
        if congressional:
            buys = [t for t in congressional if "purchase" in str(t.get("action","")).lower()]
            sells = [t for t in congressional if "sale" in str(t.get("action","")).lower()]
            if len(buys) >= 3:
                score += 3; signals.append("CONGRESS_CLUSTER_BUY")
                pols = ", ".join([b.get("politician","Unknown") for b in buys[:3]])
                reasons.append({
                    "icon":"&#127963;","label":f"Congressional Cluster Buy: {len(buys)} politicians",
                    "short":f"Multiple members of Congress bought this stock recently.",
                    "why":f"Under the STOCK Act law, members of Congress must publicly disclose their personal stock trades within 45 days. {len(buys)} politicians including {pols} recently used their own personal money to buy this stock. When multiple politicians buy the same stock at the same time it can signal they have positive expectations about upcoming policy or regulatory decisions that will benefit this company.",
                    "what_to_watch":"Check which committees these politicians sit on. A senator on the technology committee buying a tech stock is a much stronger signal than a random member buying it.",
                    "lesson":"Congressional trades are public information that most people ignore. Apex Q tracks them because politicians often have information about regulatory and policy changes before the public does."
                })
            elif buys:
                score += 2; signals.append("CONGRESS_BUYING")
                reasons.append({
                    "icon":"&#127963;","label":"Congressional Buying Detected",
                    "short":f"{buys[0].get('politician','A politician')} recently bought this stock.",
                    "why":f"{buys[0].get('politician','A politician')} recently purchased shares of this company with their own personal money. This is a legally required public disclosure under the STOCK Act. Politicians who buy stocks in industries they regulate or oversee can sometimes signal positive policy expectations.",
                    "what_to_watch":"One politician buying is interesting. Multiple politicians buying the same stock is much more significant. Watch to see if others follow.",
                    "lesson":"The STOCK Act was passed in 2012 to make congressional trading more transparent. Tracking these disclosures gives everyday investors the same visibility as Wall Street researchers."
                })
            elif sells:
                score -= 1; signals.append("CONGRESS_SELLING")
                reasons.append({
                    "icon":"&#127963;","label":f"Congressional Selling: {len(sells)} trades",
                    "short":f"Politicians are selling this stock.",
                    "why":f"{len(sells)} members of Congress recently sold shares of this company. Politicians sell for many reasons including personal financial planning. However when multiple politicians sell the same stock it can sometimes signal concern about upcoming regulatory changes or policy shifts that could hurt the company.",
                    "what_to_watch":"Check whether these politicians sit on committees that regulate this industry. Relevant committee members selling is more meaningful than unrelated members selling.",
                    "lesson":"Congressional selling is a weaker signal than buying but still worth noting when multiple politicians exit the same position in a short time period."
                })

        # Insider with full WHY
        if insider:
            cb = [t for t in insider if t.get("is_clevel") and t.get("action")=="A"]
            cs = [t for t in insider if t.get("is_clevel") and t.get("action")=="D"]
            if len(cb) >= 3:
                score += 4; signals.append("INSIDER_CLUSTER_BUY")
                names = ", ".join([f"{t.get('name')} ({t.get('title')})" for t in cb[:3]])
                reasons.append({
                    "icon":"&#128188;","label":"C-Level Cluster Buy — HIGHEST CONVICTION SIGNAL",
                    "short":f"{len(cb)} executives are buying their own company stock.",
                    "why":f"Multiple executives including {names} recently purchased shares of their own company with their own personal money. This is one of the most powerful signals in all of investing. These people see every financial report before the public. They know the company better than any outside analyst. The only reason executives buy their own stock with their own money is because they believe the price is going higher. When three or more executives buy at the same time this is called a cluster buy and it is extremely rare and significant.",
                    "what_to_watch":"Look at the dollar amount they bought. A CEO spending $1 million of personal money is a much stronger signal than a director spending $10,000.",
                    "lesson":"Insider buying is the ultimate vote of confidence. You cannot fake it. These executives are putting real money behind their belief that the stock is undervalued. Follow the money."
                })
            elif len(cb) == 2:
                score += 3; signals.append("INSIDER_CLUSTER_BUY")
                reasons.append({
                    "icon":"&#128188;","label":"Dual Executive Buy Signal",
                    "short":f"Two executives bought company stock with personal money.",
                    "why":f"{cb[0].get('name')} ({cb[0].get('title')}) and {cb[1].get('name')} ({cb[1].get('title')}) both independently purchased shares. When two senior executives buy at the same time it shows strong internal alignment. They both looked at the same company data and both decided it was a good time to buy.",
                    "what_to_watch":"Check the dates of both purchases. Purchases within a few days of each other are more significant than purchases weeks apart.",
                    "lesson":"Two executives independently buying at similar times shows conviction. They are not following each other — they are both drawing the same conclusion from the same data."
                })
            elif len(cb) == 1:
                score += 2; signals.append("INSIDER_BUY")
                reasons.append({
                    "icon":"&#128188;","label":f"Executive Buy: {cb[0].get('title')}",
                    "short":f"{cb[0].get('name')} bought {int(cb[0].get('shares',0)):,} shares.",
                    "why":f"{cb[0].get('name')}, the {cb[0].get('title')}, recently purchased {int(cb[0].get('shares',0)):,} shares at ${cb[0].get('price')} per share. Executives receive financial statements and operational data that the public does not see. When they spend their own money on company stock it is a strong signal that they see value that the market has not recognized yet.",
                    "what_to_watch":"Is this a large purchase relative to their likely salary? A CEO buying 10,000 shares when they earn $5 million is more meaningful than buying 100 shares.",
                    "lesson":"C-suite executives buying their own stock is called open market purchase. It is different from stock options which they receive as compensation. Open market purchases mean they paid real money."
                })
            if len(cs) >= 2:
                score -= 2; signals.append("INSIDER_CLUSTER_SELL")
                reasons.append({
                    "icon":"&#128188;","label":f"Executive Cluster Selling: {len(cs)} officers",
                    "short":f"Multiple executives selling shares is a caution flag.",
                    "why":f"{len(cs)} executives recently sold shares. While executives sell for legitimate reasons like taxes, diversification, and personal financial planning, heavy cluster selling sometimes signals insiders positioning ahead of disappointing results. It is not always negative but it reduces conviction.",
                    "what_to_watch":"Are these pre-scheduled 10b5-1 plan sales or open market discretionary sales? Pre-scheduled sales are less meaningful. Sudden discretionary sales are more concerning.",
                    "lesson":"Executives sell for many reasons. The question to ask is whether they are selling based on a pre-planned schedule or as a new decision. New decisions to sell carry more weight."
                })

        # News context
        if news:
            recent = news[:3]
            reasons.append({
                "icon":"&#128240;","label":f"News Intelligence: {len(news)} articles found",
                "short":f"Recent news coverage detected from Finnhub.",
                "why":f"Apex Q found {len(news)} news articles about this company in the last 60 days. News is one of the fastest moving signals — a single announcement can change a stock's trajectory in minutes. Review the news section below to understand what is being said about this company right now.",
                "what_to_watch":"Look for patterns in the news. Is the coverage mostly positive or negative? Are there recurring themes about growth, competition, or regulatory issues?",
                "lesson":"News drives short term stock moves. Fundamentals drive long term stock value. Use news to understand the current environment and fundamentals to decide if the price is right."
            })

        # Confluence
        conf_list = [s for s in ["CONGRESS_BUYING","CONGRESS_CLUSTER_BUY","INSIDER_BUY","INSIDER_CLUSTER_BUY","ANALYST_BUY","STRONG_MOMENTUM","POSITIVE_MOMENTUM"] if s in signals]
        if len(conf_list) >= 3:
            score += 3
            reasons.append({
                "icon":"&#9889;","label":f"CONFLUENCE SIGNAL: {len(conf_list)} Layers Aligned",
                "short":"Multiple independent signals are all pointing the same direction.",
                "why":f"Apex Q detected alignment across {len(conf_list)} completely independent data sources: {', '.join(conf_list)}. Confluence is when multiple unrelated signals agree without knowing about each other. The market data, the professional analysts, the politicians, and the company insiders are all independently pointing the same direction. This is extremely rare and extremely meaningful.",
                "what_to_watch":"Confluence signals are the highest conviction setups in Apex Q. Still manage your position size and set a clear stop loss. No signal is perfect.",
                "lesson":"The best trading setups are when multiple unrelated signals agree. If price momentum, insider buying, and analyst upgrades all happen at the same time the probability of a positive outcome increases significantly."
            })

        # Final verdict with full WATCH explanation
        if score >= 6:
            v = "APPROVE"
            conf_txt = f"HIGH CONVICTION BUY SIGNAL. Score {score}/15. Multiple independent intelligence layers confirm a strong bullish setup. The weight of evidence across price action, analyst consensus, and smart money activity all points higher. This is exactly the type of confluence Apex Q is built to find."
            watch_for = ""
            what_changes = ""
        elif score >= 3:
            v = "APPROVE"
            conf_txt = f"MODERATE BUY SIGNAL. Score {score}/15. More signals favor the upside than the downside. Not a perfect setup but the data leans bullish. Manage your position size and set a clear stop loss before entering."
            watch_for = ""
            what_changes = ""
        elif score <= -5:
            v = "PASS"
            conf_txt = f"HIGH CONVICTION AVOID. Score {score}/15. Multiple independent signals are clearly negative. The data strongly suggests avoiding this position right now and waiting for conditions to improve before reconsidering."
            watch_for = "Watch for: A reversal in price momentum, an analyst upgrade, or insider buying activity that would signal the situation is improving."
            what_changes = "For this to become WATCH: The price needs to stabilize and find support. For this to become APPROVE: Analysts need to upgrade their rating AND insiders need to start buying."
        elif score <= -2:
            v = "PASS"
            conf_txt = f"CAUTION SIGNAL. Score {score}/15. More signals are negative than positive. The risk/reward does not favor entry at current price levels. Monitor for improvement."
            watch_for = "Watch for: Price stabilization above a key support level, positive news catalyst, or analyst commentary suggesting the worst is priced in."
            what_changes = "For this to become WATCH: One or two negative signals need to flip positive. For this to become APPROVE: Multiple signals need to align bullishly including price momentum and analyst sentiment."
        else:
            v = "WATCH"
            # Build specific watch explanation based on what signals are mixed
            pos_signals = [s for s in signals if not any(neg in s for neg in ["SELL","SELLING","HEAVY"])]
            neg_signals = [s for s in signals if any(neg in s for neg in ["SELL","SELLING","HEAVY"])]
            conf_txt = f"MIXED SIGNALS. Score {score}/15. The intelligence agents are not in agreement on this stock right now. "
            if pos_signals and neg_signals:
                conf_txt += f"Positive signals include: {', '.join(pos_signals)}. Negative signals include: {', '.join(neg_signals)}. When signals conflict it means the market itself is undecided."
            elif not pos_signals:
                conf_txt += "No strong positive signals have fired yet. The stock is not showing the type of momentum or smart money activity that would justify an Approve signal right now."
            else:
                conf_txt += "While some positive signals exist the negative signals are pulling the score down. The data says patience is the right move here."
            watch_for = f"Watch for: Strong price momentum above +2% on high volume. An analyst upgrade to Buy. Insider buying by C-level executives. Congressional purchases."
            what_changes = f"For this to become APPROVE: The score needs to reach 3 or higher. Right now it is {score}. You need {3 - score} more positive signal points. The fastest way to get there would be an analyst upgrade (+2 points) or an executive buying shares (+2 points)."

        logger.info(f"Orchestrator: {symbol} verdict={v} score={score}")
        return v, conf_txt, reasons, score, signals, watch_for if 'watch_for' in locals() else "", what_changes if 'what_changes' in locals() else ""

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

.hdr{background:var(--surface);border-bottom:2px solid var(--border);padding:14px 28px;display:flex;align-items:center;justify-content:space-between;}
.hlogo{display:flex;align-items:center;gap:13px;}
.hmark{width:42px;height:42px;background:linear-gradient(135deg,#0052cc,#003399);border-radius:11px;display:flex;align-items:center;justify-content:center;font-size:22px;box-shadow:0 3px 10px rgba(0,82,204,.25);}
.hname{font-size:25px;font-weight:800;letter-spacing:-.5px;}
.hname span{color:var(--accent);}
.hbadge{display:flex;align-items:center;gap:7px;background:var(--gbg);border:1px solid var(--green);border-radius:20px;padding:6px 14px;font-size:11px;color:var(--green);font-family:'JetBrains Mono',monospace;font-weight:700;}
.hdot{width:7px;height:7px;background:var(--green);border-radius:50%;animation:pulse 1.4s infinite;}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}

.mbar{background:var(--surface);border-bottom:1px solid var(--border);display:flex;overflow-x:auto;scrollbar-width:none;}
.mbar::-webkit-scrollbar{display:none;}
.mi{padding:10px 22px;border-right:1px solid var(--border);cursor:pointer;transition:background .2s;min-width:148px;flex-shrink:0;}
.mi:hover{background:var(--s2);}
.ml{font-size:9px;color:var(--muted);font-family:'JetBrains Mono',monospace;text-transform:uppercase;letter-spacing:1.2px;margin-bottom:4px;font-weight:700;}
.mv{font-size:13px;font-weight:700;font-family:'JetBrains Mono',monospace;}
.mv.up{color:var(--green);}
.mv.dn{color:var(--red);}
.mv.ld{color:var(--muted);font-size:11px;}

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

.main{padding:22px 28px 70px;display:grid;grid-template-columns:1fr 340px;gap:22px;}
@media(max-width:980px){.main{grid-template-columns:1fr;}}
.stitle{font-size:10px;text-transform:uppercase;letter-spacing:2px;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-bottom:13px;display:flex;align-items:center;gap:9px;font-weight:700;}
.stitle::after{content:'';flex:1;height:1px;background:var(--border);}

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

/* VERDICT BOX */
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
.vconf{font-size:13px;color:var(--text);margin-bottom:14px;line-height:1.75;font-weight:500;background:rgba(255,255,255,.65);padding:14px 18px;border-radius:10px;}
.watch-guide{background:rgba(255,255,255,.7);border-radius:10px;padding:14px 18px;margin-bottom:14px;border-left:4px solid var(--yellow);}
.wg-title{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:var(--yellow);font-family:'JetBrains Mono',monospace;margin-bottom:6px;}
.wg-text{font-size:12px;color:var(--text);line-height:1.6;}
.pass-guide{background:rgba(255,255,255,.7);border-radius:10px;padding:14px 18px;margin-bottom:14px;border-left:4px solid var(--red);}
.pg-title{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:var(--red);font-family:'JetBrains Mono',monospace;margin-bottom:6px;}
.pg-text{font-size:12px;color:var(--text);line-height:1.6;}

/* EXPANDABLE REASON CARDS */
.vrlist{display:flex;flex-direction:column;gap:8px;}
.vr{background:rgba(255,255,255,.88);border-radius:11px;overflow:hidden;transition:box-shadow .2s;}
.vr:hover{box-shadow:0 2px 12px rgba(0,0,0,.1);}
.vr-header{display:flex;align-items:center;gap:12px;padding:13px 16px;cursor:pointer;user-select:none;}
.vi{font-size:20px;flex-shrink:0;}
.vr-main{flex:1;}
.vlbl{font-weight:700;display:block;color:var(--text);font-size:13px;}
.vshort{color:var(--muted);font-size:12px;margin-top:2px;}
.vexpand-btn{font-size:11px;color:var(--accent);font-family:'JetBrains Mono',monospace;font-weight:700;white-space:nowrap;background:rgba(0,82,204,.1);padding:4px 10px;border-radius:20px;flex-shrink:0;}
.vr-body{display:none;padding:0 16px 16px 52px;border-top:1px solid rgba(0,0,0,.06);}
.vr-body.open{display:block;}
.vr-section{margin-top:12px;}
.vr-section-title{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:var(--accent);font-family:'JetBrains Mono',monospace;margin-bottom:5px;}
.vr-section-text{font-size:13px;color:var(--text);line-height:1.65;}
.vr-lesson{background:var(--s2);border-radius:8px;padding:10px 14px;margin-top:10px;border-left:3px solid var(--accent);}
.vr-lesson-title{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:var(--accent);font-family:'JetBrains Mono',monospace;margin-bottom:4px;}
.vr-lesson-text{font-size:12px;color:var(--text);line-height:1.6;font-style:italic;}

.mets{display:grid;grid-template-columns:repeat(3,1fr);gap:11px;margin-bottom:20px;}
.met{background:var(--s2);border-radius:11px;padding:14px;border:1px solid var(--border);}
.ml2{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:1.2px;margin-bottom:5px;font-family:'JetBrains Mono',monospace;font-weight:700;}
.mv2{font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--text);}
.mv2.pos{color:var(--green);}
.mv2.neg{color:var(--red);}
.mv2.neu{color:var(--accent);}

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

.ni{padding:12px 0;border-bottom:1px solid var(--border);}
.ni:last-child{border-bottom:none;}
.ns{font-size:10px;color:var(--accent);font-family:'JetBrains Mono',monospace;text-transform:uppercase;font-weight:700;margin-bottom:4px;}
.nh{font-size:13px;color:var(--text);line-height:1.55;font-weight:500;}
.nsum{font-size:11px;color:var(--muted);margin-top:3px;line-height:1.4;}

.loading{display:none;padding:50px 30px;text-align:center;color:var(--accent);font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:700;}
.loading.on{display:block;animation:pulse 1.2s infinite;}
.lsteps{margin-top:18px;display:flex;flex-direction:column;gap:7px;max-width:320px;margin:18px auto 0;text-align:left;}
.ls{font-size:11px;color:var(--muted);display:flex;align-items:center;gap:9px;}

.sg{background:var(--surface);border:1px solid var(--border);border-radius:13px;padding:15px;margin-bottom:10px;cursor:pointer;transition:all .2s;box-shadow:0 1px 5px rgba(0,0,0,.04);}
.sg:hover{border-color:var(--accent);box-shadow:0 4px 14px rgba(0,82,204,.1);}
.sgtop{display:flex;align-items:center;justify-content:space-between;margin-bottom:7px;}
.sgsym{font-size:17px;font-weight:800;font-family:'JetBrains Mono',monospace;}
.sgv{font-size:10px;font-weight:700;padding:3px 11px;border-radius:20px;font-family:'JetBrains Mono',monospace;}
.va{background:var(--gbg);color:var(--green);border:1px solid var(--green);}
.vp{background:var(--rbg);color:var(--red);border:1px solid var(--red);}
.vw{background:var(--ybg);color:var(--yellow);border:1px solid var(--yellow);}
.sgi{font-size:11px;font-family:'JetBrains Mono',monospace;color:var(--muted);}

.foot{background:var(--surface);border-top:1px solid var(--border);padding:22px 28px;text-align:center;font-size:10px;color:var(--muted);font-family:'JetBrains Mono',monospace;line-height:2.2;}
</style>
</head>
<body>

<div class="tkbar"><div class="tktrack" id="tktrack"><span class="tki"><span class="tsym">APEX Q</span><span class="tpx">Loading live market data...</span></span></div></div>

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
      Running 4 intelligence agents simultaneously...
      <div class="lsteps">
        <div class="ls">&#128202; Analyst Agent — price, fundamentals, valuation</div>
        <div class="ls">&#127963; Regulatory Agent — congressional trades</div>
        <div class="ls">&#128188; Insider Agent — C-level buy and sell activity</div>
        <div class="ls">&#128240; News Agent — Finnhub live intelligence</div>
        <div class="ls">&#9889; Synthesis Engine — calculating verdict with full reasoning</div>
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
        <div class="vconf" id="vconf">Search any stock or company name above. Apex Q runs four independent intelligence agents and synthesizes all data into a clear verdict. Every signal is fully explained in plain English. Click any signal below to expand the full reasoning, what to watch for, and the educational lesson behind it.</div>
        <div id="wguide"></div>
        <div class="vrlist" id="vrl">
          <div class="vr">
            <div class="vr-header">
              <span class="vi">&#128202;</span>
              <div class="vr-main"><span class="vlbl">Analyst Agent</span><span class="vshort">Price momentum, PE ratio, analyst consensus, price target</span></div>
              <span class="vexpand-btn">TAP TO LEARN</span>
            </div>
          </div>
          <div class="vr">
            <div class="vr-header">
              <span class="vi">&#127963;</span>
              <div class="vr-main"><span class="vlbl">Regulatory Agent</span><span class="vshort">Congressional trading via Quiver Quantitative</span></div>
              <span class="vexpand-btn">TAP TO LEARN</span>
            </div>
          </div>
          <div class="vr">
            <div class="vr-header">
              <span class="vi">&#128188;</span>
              <div class="vr-main"><span class="vlbl">Insider Agent</span><span class="vshort">C-level executive buy and sell detection</span></div>
              <span class="vexpand-btn">TAP TO LEARN</span>
            </div>
          </div>
          <div class="vr">
            <div class="vr-header">
              <span class="vi">&#128240;</span>
              <div class="vr-main"><span class="vlbl">News Agent</span><span class="vshort">Live news from Finnhub</span></div>
              <span class="vexpand-btn">TAP TO LEARN</span>
            </div>
          </div>
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

function buildReasonCard(r, idx){
  const id=`vr-body-${idx}`;
  let bodyHtml='';
  if(r.why) bodyHtml+=`<div class="vr-section"><div class="vr-section-title">&#10067; Why This Matters</div><div class="vr-section-text">${r.why}</div></div>`;
  if(r.what_to_watch) bodyHtml+=`<div class="vr-section"><div class="vr-section-title">&#128064; What To Watch For</div><div class="vr-section-text">${r.what_to_watch}</div></div>`;
  if(r.lesson) bodyHtml+=`<div class="vr-lesson"><div class="vr-lesson-title">&#127891; The Lesson</div><div class="vr-lesson-text">${r.lesson}</div></div>`;
  return `<div class="vr">
    <div class="vr-header" onclick="toggleReason('${id}', this)">
      <span class="vi">${r.icon}</span>
      <div class="vr-main"><span class="vlbl">${r.label}</span><span class="vshort">${r.short||''}</span></div>
      <span class="vexpand-btn" id="btn-${id}">TAP TO LEARN &#9660;</span>
    </div>
    <div class="vr-body" id="${id}">${bodyHtml}</div>
  </div>`;
}

function toggleReason(id, header){
  const body=document.getElementById(id);
  const btn=document.getElementById('btn-'+id);
  const open=body.classList.contains('open');
  body.classList.toggle('open');
  btn.innerHTML=open?'TAP TO LEARN &#9660;':'COLLAPSE &#9650;';
}

function renderCong(data){
  const s=document.getElementById('cong');
  if(!data||!data.length){
    s.innerHTML='<div class="ic"><div class="ih"><div class="it">Congressional Trading</div><div class="ibdg bg">CLEAN</div></div><div class="itxt">No recent congressional trading activity found for this stock. This means no politicians have publicly disclosed trades in this company recently. A clean slate is neutral — not a negative signal.</div></div>';
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
    s.innerHTML='<div class="ic"><div class="ih"><div class="it">Insider Activity</div><div class="ibdg bg">CLEAN</div></div><div class="itxt">No recent insider trading filings detected. A clean insider slate means executives and directors are not making unusual moves with their personal holdings right now. This is a neutral signal.</div></div>';
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
    s.innerHTML='<div class="ic"><div class="ih"><div class="it">Finnhub News Feed</div><div class="ibdg by">NO RESULTS</div></div><div class="itxt">No news articles found in the last 60 days. This may mean low media coverage or a very recently listed company. Low news coverage is not necessarily negative — sometimes the best opportunities are in overlooked stocks.</div></div>';
    return;
  }
  const isGen=data.some(n=>n.source&&n.source.includes('General'));
  s.innerHTML=(isGen?'<div class="ic" style="margin-bottom:10px"><div class="ih"><div class="it">Market News</div><div class="ibdg by">GENERAL</div></div><div class="itxt" style="font-size:12px">No company-specific news found in the last 60 days. Showing general market news instead.</div></div>':'')+data.map(n=>`<div class="ni"><div class="ns">${n.source||'Market News'}</div><div class="nh">${n.headline}</div>${n.summary?`<div class="nsum">${n.summary}</div>`:''}</div>`).join('');
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
    const el=document.getElementById(m.id);
    try{
      const r=await fetch(`${A}/analyze?symbol=${encodeURIComponent(m.s)}`);
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
    document.getElementById('schg').textContent=(chg>=0?'+':'')+chg+'% today';
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

    // Watch/Pass guide boxes
    const wg=document.getElementById('wguide');
    if(v==='WATCH'&&d.watch_for){
      wg.innerHTML=`<div class="watch-guide"><div class="wg-title">&#128064; What To Watch For</div><div class="wg-text">${d.watch_for}</div></div><div class="watch-guide" style="border-color:var(--accent)"><div class="wg-title" style="color:var(--accent)">&#9889; What Changes The Verdict</div><div class="wg-text">${d.what_changes||''}</div></div>`;
    }else if(v==='PASS'&&d.watch_for){
      wg.innerHTML=`<div class="pass-guide"><div class="pg-title">&#128064; What To Watch For Recovery</div><div class="pg-text">${d.watch_for}</div></div><div class="pass-guide" style="border-color:var(--accent)"><div class="pg-title" style="color:var(--accent)">&#9889; What Changes The Verdict</div><div class="pg-text">${d.what_changes||''}</div></div>`;
    }else{
      wg.innerHTML='';
    }

    // Expandable reason cards
    if(d.reasons&&d.reasons.length){
      document.getElementById('vrl').innerHTML=d.reasons.map((r,i)=>buildReasonCard(r,i)).join('');
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
    logger.info(f"ANALYZE: {query} -> {symbol}")
    market = market_agent.get(symbol)
    if not market:
        return jsonify({"error":f"No data found for {symbol}."}),404
    news = news_agent.get(symbol)
    congressional = reg_agent.get_congressional(symbol)
    insider = ins_agent.get(symbol)
    verdict, confidence, reasons, score, signals, watch_for, what_changes = orch.synthesize(symbol, market, congressional, insider, news)
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
