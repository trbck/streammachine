# Alpaca Market Data Examples

This directory contains examples for consuming real-time market data from Alpaca and processing it through StreamMachine.

## Examples

### alpaca_websocket.py

Demonstrates consuming real-time trade and quote data from Alpaca WebSocket API:

- Connecting to Alpaca WebSocket
- Subscribing to trade and quote channels
- Processing incoming data
- Real-time OHLC aggregation
- Symbol-specific handlers

**Requirements:**
```bash
pip install alpaca-trade-api websockets
```

**Configuration:**
```bash
export ALPACA_API_KEY=your_api_key
export ALPACA_SECRET_KEY=your_secret_key
```

**Run:**
```bash
python alpaca_websocket.py
```

### tick_to_ohlc.py

Demonstrates real-time tick aggregation into OHLC candles:

- Receiving tick data
- Aggregating into time-based candles (1-minute, 5-minute)
- Multiple interval support
- Emitting completed candles

**Run:**
```bash
python tick_to_ohlc.py
```

## Architecture

```
Alpaca WebSocket (trade/quote data)
       ↓
@app.agent("trades") → Process incoming ticks
       ↓
tick_to_ohlc → Aggregate into OHLC candles
       ↓
TimeSeriesBuffer → In-memory candle storage
       ↓
@app.timer(60) → Emit completed candles to output stream
```

## Usage Patterns

### Real-time Price Tracking

```python
@app.agent("trades", group="price_trackers")
async def track_prices(record: Message):
    symbol = record.message.get("symbol")
    price = float(record.message.get("price"))

    # Update latest price
    await app.storage.write(f"price_{symbol}", price)
```

### OHLC Aggregation

```python
# Track OHLC for each symbol
ohlc_data = {}

@app.agent("trades", group="ohlc")
async def aggregate_ohlc(record: Message):
    msg = record.message
    symbol = msg.get("symbol")
    price = float(msg.get("price"))

    if symbol not in ohlc_data:
        ohlc_data[symbol] = {"open": price, "high": price, "low": price, "close": price}
    else:
        ohlc_data[symbol]["high"] = max(ohlc_data[symbol]["high"], price)
        ohlc_data[symbol]["low"] = min(ohlc_data[symbol]["low"], price)
        ohlc_data[symbol]["close"] = price
```

### Time-based Candle Emission

```python
@app.timer(60)  # Every minute
async def emit_candles():
    for symbol, data in ohlc_data.items():
        await app.send("minute_candles", {
            "symbol": symbol,
            **data,
            "timestamp": time.time(),
        })
```

## Production Considerations

1. **Connection Management**: Handle WebSocket reconnection on failure
2. **Backpressure**: Monitor queue depth and throttle if needed
3. **Error Handling**: Handle malformed messages and network issues
4. **State Persistence**: Store candle state in Redis for recovery
5. **Monitoring**: Track message rates, latency, and error counts

## API Keys

For Alpaca API access:
1. Sign up at [Alpaca Markets](https://alpaca.markets/)
2. Create API keys in your dashboard
3. Set environment variables or use in code

```python
# Option 1: Environment variables
export ALPACA_API_KEY=your_key
export ALPACA_SECRET_KEY=your_secret

# Option 2: In code
client = AlpacaWebSocketClient(api_key="your_key", secret_key="your_secret")
```

## Related Examples

- `../patterns/pipeline_pattern.py`: Multi-stage processing
- `../advanced/timeseries_windowing.py`: Time series windowing
- `../tutorials/04_data_transformation.py`: Data transformation