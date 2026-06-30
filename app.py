 Backend – historical data endpoint (add to app.py)
This route provides OHLCV data for any symbol with a flexible period and interval.
It caches heavily to avoid hitting Yahoo too often, and always returns the most recent candle.

python
# CHUNK: historical OHLCV data for the interactive candlestick chart
@app.route("/history/<symbol>")
def history(symbol):
    symbol = symbol.strip().upper()
    if not symbol:
        return jsonify({"error": "No symbol"}), 400

    period = request.args.get("period", "1y")   # 1d, 5d, 1mo, 3mo, 6mo, 1y
    interval = request.args.get("interval", "1d")  # 1m, 5m, 15m, 1h, 1d, 1wk, 1mo

    # shorter cache time for intraday data, longer for daily
    ttl = 60 if interval in ("1m", "5m", "15m") else 900
    cache_key = f"hist_{symbol}_{period}_{interval}"
    cached = get_cache(cache_key)
    if cached and (time.time() - cached[1]) < ttl:
        return jsonify(cached[0])

    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period, interval=interval)
        if df.empty:
            return jsonify({"error": f"No data for {symbol}"}), 404

        data = []
        for idx, row in df.iterrows():
            t = int(idx.timestamp())
            data.append({
                "time": t,
                "open": round(float(row["Open"]), 2),
                "high": round(float(row["High"]), 2),
                "low": round(float(row["Low"]), 2),
                "close": round(float(row["Close"]), 2),
                "volume": int(row["Volume"])
            })

        payload = {"symbol": symbol, "data": data, "period": period, "interval": interval}
        set_cache(cache_key, payload)
        return jsonify(payload)
    except Exception as e:
        logger.error(f"History error for {symbol}: {e}")
        return jsonify({"error": str(e)}), 500
