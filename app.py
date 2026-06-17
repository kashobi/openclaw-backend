from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
import requests
import os
import time
import logging
from google import genai # Your Gemini API Agent

# Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = Flask(__name__)
CORS(app)

# Environment setup
FINNHUB_KEY = os.environ.get("FINNHUB_KEY")
QUIVER_KEY = os.environ.get("QUIVER_KEY")
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

# Persistent Cache (Database-ready structure)
CACHE = {}
TTL = 14400 # 4 Hours

class IntelligenceGatekeeper:
    def get_analysis(self, symbol):
        # 1. GATEKEEPER: Check Cache
        if symbol in CACHE and (time.time() - CACHE[symbol]['ts'] < TTL):
            return CACHE[symbol]['data']
        
        # 2. DATA AGGREGATION: Collect all inputs
        market = MarketDataAgent().get(symbol)
        insider = InsiderAgent().get(symbol)
        gov = RegulatoryAgent().get_congressional(symbol)
        
        # 3. SYNTHESIS: Autonomous Gemini Reasoning
        prompt = f"""
        Analyze these inputs for {symbol} and provide a strictly logical verdict (APPROVE, WATCH, or PASS).
        Market: {market}
        Insider Trades: {insider}
        Congressional: {gov}
        Return ONLY JSON: {{"verdict": "...", "reasoning": "..."}}
        """
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        verdict_data = json.loads(response.text)
        
        # 4. STORAGE
        CACHE[symbol] = {'data': verdict_data, 'ts': time.time()}
        return verdict_data

# [Keep your existing MarketDataAgent, InsiderAgent, and RegulatoryAgent classes here]

@app.route('/api/analyze/<symbol>')
def analyze(symbol):
    gatekeeper = IntelligenceGatekeeper()
    result = gatekeeper.get_analysis(symbol.upper())
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
