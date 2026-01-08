import time
from axis_bank_feed import DhanMarketFeed
from tick_filter import TickFilter
from quant_processor import QuantProcessor
from signal_engine import SignalEngine
from microstructure_state import MicrostructureState

feed = DhanMarketFeed("NSE_EQ", 5900)
filter = TickFilter()
quant = QuantProcessor()
micro = MicrostructureState()
signal_engine = SignalEngine()

while True:
    raw = feed.fetch_tick()
    if not raw:
        time.sleep(1)
        continue

    tick = filter.extract(raw)
    q = quant.compute(tick)
    q = micro.update(q)              # 🔥 CRITICAL LINE
    signal = signal_engine.generate(q)

    print(
        f"{q['ts']} | "
        f"LTP:{q['ltp']:.2f} | "
        f"FLOW:{q['flow']:>6} | "
        f"VWAP:{q['vwap']:.2f} | "
        f"TAP:{q['time_at_price']:>4}s | "
        f"ABS:{int(q['absorption_flag'])} | "
        f"VAC:{int(q['vacuum_flag'])} | "
        f"SIGNAL:{signal}"
    )

    if signal == "HOLD" and signal_engine.last_block_reason:
        print("   ↳ BLOCK_REASON:", signal_engine.last_block_reason)

    time.sleep(1)
