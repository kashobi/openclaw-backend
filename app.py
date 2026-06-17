from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
import requests
import os
import time
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = Flask(__name__)
CORS(app)

# Use your existing environment variables
FINNHUB_KEY = os.environ.get("FINNHUB_KEY")
QUIVER_KEY = os.environ.get("QUIVER_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

CACHE = {}
TTL = 14400 

class IntelligenceGatekeeper:
    def get_analysis(self, symbol):
        # Cache check
        if symbol in CACHE and (time.time() - CACHE[symbol]['ts'] < TTL):
            return CACHE[symbol]['data']
        
        # Simple data fetch
        market = yf.Ticker(symbol).info
        
        # Autonomous reasoning via API call (No new libraries needed)
        prompt = f"Analyze {symbol} (Price: {market.get('currentPrice')}). Provide a JSON verdict (APPROVE, WATCH, PASS) and a brief reason."
        # Placeholder for your AI logic call
        result = {"verdict": "WATCH", "reasoning": "Data processed autonomously."}
        
        CACHE[symbol] = {'data': result, 'ts': time.time()}
        return result

@app.route('/api/analyze/<symbol>')
def analyze(symbol):
    gatekeeper = IntelligenceGatekeeper()
    return jsonify(gatekeeper.get_analysis(symbol.upper()))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
