"""
Alpaca WebSocket Market Data Example

This example demonstrates consuming real-time market data from Alpaca
and processing it through StreamMachine.

Requirements:
    pip install alpaca-trade-api websockets

Configuration:
    Set ALPACA_API_KEY and ALPACA_SECRET_KEY environment variables

Run with: python alpaca_websocket.py

Note: This is example code showing how to USE StreamMachine.
The Alpaca WebSocket client runs in a separate task and publishes
to StreamMachine streams.
"""
import asyncio
import os
import time
from streammachine import App, Message
from streammachine.models import TimeSeriesBuffer

app = App(name="alpaca_example", to_scan=True)

# Time series buffers for real-time analysis
price_buffer = TimeSeriesBuffer(max_age_seconds=60, max_rows=10000)
trade_buffer = TimeSeriesBuffer(max_age_seconds=300, max_rows=50000)


# =============================================================================
# Alpaca WebSocket Client (Conceptual)
# =============================================================================

class AlpacaWebSocketClient:
    """Conceptual Alpaca WebSocket client.

    In production, use the alpaca-trade-api library:

        from alpaca_trade_api.stream import Stream

        stream = Stream(API_KEY, SECRET_KEY, base_url=URL)

        async def on_trade(trade):
            await app.send("trades", {
                "symbol": trade.symbol,
                "price": trade.price,
                "size": trade.size,
                "timestamp": trade.timestamp,
            })

        stream.subscribe_trades(on_trade, "AAPL", "MSFT")
        await stream.run()
    """

    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self._running = False

    async def connect(self):
        """Connect to Alpaca WebSocket."""
        # In production, this would:
        # 1. Connect to wss://stream.data.alpaca.markets/v2/iex
        # 2. Authenticate with API key and secret
        # 3. Subscribe to trade/quote channels
        print("[Alpaca] Connecting to WebSocket...")
        self._running = True

    async def subscribe_trades(self, symbols: list):
        """Subscribe to trade updates for symbols."""
        # In production, send subscription message
        print(f"[Alpaca] Subscribed to trades: {symbols}")

    async def subscribe_quotes(self, symbols: list):
        """Subscribe to quote updates for symbols."""
        print(f"[Alpaca] Subscribed to quotes: {symbols}")

    async def run(self):
        """Run the WebSocket client."""
        # In production, this would:
        # 1. Listen for incoming messages
        # 2. Parse trade/quote data
        # 3. Send to StreamMachine streams

        # Simulated trade data for demo
        symbols = ["AAPL", "MSFT", "GOOGL"]

        while self._running:
            for symbol in symbols:
                # Simulate trade
                await app.send("trades", {
                    "symbol": symbol,
                    "price": 100 + hash(f"{symbol}{time.time()}") % 50,
                    "size": 100,
                    "timestamp": time.time(),
                })

            await asyncio.sleep(0.5)  # Simulated data rate

    async def close(self):
        """Close the WebSocket connection."""
        self._running = False
        print("[Alpaca] WebSocket closed")


# =============================================================================
# Trade Processor
# =============================================================================

@app.agent("trades", group="trade_processors")
async def process_trade(record: Message):
    """Process incoming trade data."""
    msg = record.message

    # Add to time series buffer
    import pandas as pd
    from streammachine.models import streams_to_dataframe

    # Process the trade
    symbol = msg.get("symbol", "UNKNOWN")
    price = float(msg.get("price", 0))
    size = int(msg.get("size", 0))

    print(f"[Trade] {symbol}: ${price:.2f} x {size}")

    # Forward to symbol-specific streams
    await app.send(f"trades_{symbol}", msg)

    # Update price buffer for aggregation
    await app.storage.write(f"latest_price_{symbol}", price)


# =============================================================================
# Quote Processor
# =============================================================================

@app.agent("quotes", group="quote_processors")
async def process_quote(record: Message):
    """Process incoming quote data."""
    msg = record.message

    symbol = msg.get("symbol", "UNKNOWN")
    bid = float(msg.get("bid_price", 0))
    ask = float(msg.get("ask_price", 0))
    spread = ask - bid

    print(f"[Quote] {symbol}: Bid ${bid:.2f} / Ask ${ask:.2f} (spread: ${spread:.2f})")


# =============================================================================
# Real-time Aggregation
# =============================================================================

# Track OHLC (Open, High, Low, Close) for each symbol
ohlc_data = {}


@app.timer(5)
async def aggregate_prices():
    """Aggregate prices into OHLC candles."""
    # Get all price keys
    keys = await app.storage.keys()
    price_keys = [k for k in keys if k.startswith("latest_price_")]

    for key in price_keys:
        symbol = key.replace("latest_price_", "")
        price = await app.storage.read(key, default=0)

        # Update OHLC
        if symbol not in ohlc_data:
            ohlc_data[symbol] = {
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "start_time": time.time(),
            }
        else:
            ohlc_data[symbol]["high"] = max(ohlc_data[symbol]["high"], price)
            ohlc_data[symbol]["low"] = min(ohlc_data[symbol]["low"], price)
            ohlc_data[symbol]["close"] = price

    # Log OHLC
    print("\n[OHLC]")
    for symbol, data in ohlc_data.items():
        print(f"  {symbol}: O={data['open']:.2f} H={data['high']:.2f} "
              f"L={data['low']:.2f} C={data['close']:.2f}")
    print()


# =============================================================================
# Minute Bar Emitter
# =============================================================================

@app.timer(60)
async def emit_minute_bars():
    """Emit completed minute bars."""
    for symbol, data in ohlc_data.items():
        await app.send("minute_bars", {
            "symbol": symbol,
            "open": data["open"],
            "high": data["high"],
            "low": data["low"],
            "close": data["close"],
            "timestamp": time.time(),
        })

        # Reset for next minute
        ohlc_data[symbol] = {
            "open": data["close"],  # New open = previous close
            "high": data["close"],
            "low": data["close"],
            "close": data["close"],
            "start_time": time.time(),
        }

    print("[Bars] Emitted minute bars")


# =============================================================================
# Symbol-specific Agents
# =============================================================================

@app.agent("trades_AAPL", group="symbol_handlers")
async def handle_aapl(record: Message):
    """Handle AAPL trades specifically."""
    msg = record.message
    print(f"[AAPL] Trade: ${msg.get('price'):.2f}")


@app.agent("trades_MSFT", group="symbol_handlers")
async def handle_msft(record: Message):
    """Handle MSFT trades specifically."""
    msg = record.message
    print(f"[MSFT] Trade: ${msg.get('price'):.2f}")


# =============================================================================
# Market Statistics
# =============================================================================

@app.timer(30)
async def market_statistics():
    """Calculate and display market statistics."""
    keys = await app.storage.keys()
    price_keys = [k for k in keys if k.startswith("latest_price_")]

    print("\n[Market Stats]")
    for key in price_keys:
        symbol = key.replace("latest_price_", "")
        price = await app.storage.read(key, default=0)
        print(f"  {symbol}: ${price:.2f}")
    print()


# =============================================================================
# Main
# =============================================================================

async def run_alpaca_client():
    """Run the Alpaca WebSocket client alongside StreamMachine."""
    api_key = os.environ.get("ALPACA_API_KEY", "demo_key")
    secret_key = os.environ.get("ALPACA_SECRET_KEY", "demo_secret")

    client = AlpacaWebSocketClient(api_key, secret_key)

    await client.connect()
    await client.subscribe_trades(["AAPL", "MSFT", "GOOGL"])
    await client.subscribe_quotes(["AAPL", "MSFT", "GOOGL"])

    # Run client in background
    asyncio.create_task(client.run())

    # Run forever (or until interrupted)
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        await client.close()


if __name__ == "__main__":
    print("=" * 60)
    print("Alpaca WebSocket Market Data Example")
    print("=" * 60)
    print("\nThis example demonstrates:")
    print("  - Consuming real-time market data")
    print("  - Processing trades and quotes")
    print("  - Real-time OHLC aggregation")
    print("  - Symbol-specific handlers")
    print("  - Time series analysis")
    print("\nArchitecture:")
    print("  Alpaca WebSocket → trades stream → process_trade")
    print("    → trades_SYMBOL streams → symbol handlers")
    print("    → OHLC aggregation → minute_bars stream")
    print("\nNote: This uses simulated data for demo purposes.")
    print("In production, use alpaca-trade-api library.")
    print("\nPress Ctrl+C to stop\n")
    print("=" * 60 + "\n")

    try:
        # In production, run both the WebSocket client and StreamMachine
        # asyncio.run(run_alpaca_client())

        # For demo, just run StreamMachine with simulated data
        app.start()
    except KeyboardInterrupt:
        print("\n[Main] Shutting down...")