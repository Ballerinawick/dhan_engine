import time


class MarketStateEngine:
    """
    Combines CE + PE + Underlying into 1-sec snapshot
    Non-breaking layer (no trading logic)
    """

    def __init__(self):
        self.init()

    def init(self):
        self.state = {}

    def update(self, index, underlying, ce, pe):
        now = int(time.time())

        s = self.state.setdefault(index, {
            "last_sec": None
        })

        # Only 1 snapshot per second
        if s["last_sec"] == now:
            return None

        s["last_sec"] = now

        try:
            ce_ltp = float(ce.get("ltp", 0))
            pe_ltp = float(pe.get("ltp", 0))

            ce_pressure = float(ce.get("imbalance_5", 0))
            pe_pressure = float(pe.get("imbalance_5", 0))

            ce_flow = float(ce.get("flow", 0))
            pe_flow = float(pe.get("flow", 0))

        except Exception as e:
            print(f"❌ SNAPSHOT_ERROR | {index} | {e}")
            return None

        pressure_diff = ce_pressure - pe_pressure
        flow_diff = ce_flow - pe_flow

        if pressure_diff > 0.15:
            bias = "BULLISH"
        elif pressure_diff < -0.15:
            bias = "BEARISH"
        else:
            bias = "NEUTRAL"

        snapshot = {
            "ts": now,
            "index": index,
            "underlying": underlying,
            "ce_ltp": ce_ltp,
            "pe_ltp": pe_ltp,
            "pressure_diff": pressure_diff,
            "flow_diff": flow_diff,
            "bias": bias
        }

        print(
            f"📊 SNAPSHOT | {index} | "
            f"bias={bias} | "
            f"u={underlying:.2f} | "
            f"ce={ce_ltp:.2f} | pe={pe_ltp:.2f} | "
            f"pDiff={pressure_diff:.2f} | fDiff={flow_diff:.2f}"
        )

        return snapshot
