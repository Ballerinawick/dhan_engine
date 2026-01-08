import time
from dhan_future_feed import DhanFutureFeed

# 🔥 USE REAL FUTURE INSTRUMENT KEY FROM CSV
# Example: NIFTY DEC FUT
FUTURE_ID = 49543   # <-- replace if needed

feed = DhanFutureFeed(FUTURE_ID)

while True:
    tick = feed.fetch_tick()
    if not tick:
        print("❌ No data")
    else:
        print(
            tick.get("ltp"),
            tick.get("bestBidQty"),
            tick.get("bestAskQty"),
            tick.get("lastTradedQty")
        )

    time.sleep(1)
