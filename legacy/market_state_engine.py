import math
import time


class MarketStateEngine:
    """
    Combines CE + PE + underlying into 1-second snapshots.
    Observational only; keeps the original contract backward compatible.
    """

    DOMINANCE_THRESHOLD = 0.18
    PRESSURE_BIAS_THRESHOLD = 0.15
    FLOW_NORM_SCALE = 8000.0
    OFI_NORM_SCALE = 5000.0
    SPREAD_STABILITY_WINDOW = 5

    def __init__(self):
        self.init()

    def init(self):
        self.state = {}

    def _safe_float(self, value, default=0.0):
        fallback = 0.0 if default is None else default
        try:
            if value is None:
                return float(fallback)
            return float(value)
        except (TypeError, ValueError):
            return float(fallback)

    def _clamp(self, value, low=-1.0, high=1.0):
        return max(low, min(high, value))

    def _norm(self, value, scale):
        scale = abs(self._safe_float(scale, 1.0)) or 1.0
        return self._clamp(value / scale)

    def _extract_feature(self, raw, *keys, default=0.0):
        for key in keys:
            if key in raw and raw[key] is not None:
                return self._safe_float(raw[key], default)
        return self._safe_float(default)

    def _extract_spread(self, raw):
        spread = raw.get("spread") if isinstance(raw, dict) else None
        if spread is not None:
            try:
                return abs(float(spread))
            except (TypeError, ValueError):
                pass
        ask_price = self._extract_feature(raw, "ask_price", "best_ask", default=0.0)
        bid_price = self._extract_feature(raw, "bid_price", "best_bid", default=0.0)
        if ask_price > 0 and bid_price > 0:
            return max(0.0, ask_price - bid_price)
        return 0.0

    def _spread_stability(self, index, ce_spread, pe_spread):
        s = self.state.setdefault(index, {})
        ce_hist = s.setdefault("ce_spread_hist", [])
        pe_hist = s.setdefault("pe_spread_hist", [])
        ce_hist.append(ce_spread)
        pe_hist.append(pe_spread)
        if len(ce_hist) > self.SPREAD_STABILITY_WINDOW:
            ce_hist.pop(0)
        if len(pe_hist) > self.SPREAD_STABILITY_WINDOW:
            pe_hist.pop(0)

        def stability(values):
            if len(values) < 2:
                return 0.5
            avg_val = sum(values) / len(values)
            variance = sum((x - avg_val) ** 2 for x in values) / len(values)
            std_dev = math.sqrt(variance)
            return 1.0 - self._clamp(std_dev / max(avg_val, 0.01), 0.0, 1.0)

        return (stability(ce_hist) + stability(pe_hist)) / 2.0

    def _calc_dominance_score(self, pressure_diff, flow_diff, ofi_diff, absorb_skew):
        score = (
            0.34 * self._clamp(pressure_diff)
            + 0.24 * self._norm(flow_diff, self.FLOW_NORM_SCALE)
            + 0.22 * self._norm(ofi_diff, self.OFI_NORM_SCALE)
            - 0.20 * self._clamp(absorb_skew)
        )
        return self._clamp(score)

    def _calc_market_regime(self, pressure_diff, flow_diff, dominance_score, ce_spread, pe_spread, compression_score):
        avg_spread = max((ce_spread + pe_spread) / 2.0, 0.0)
        spread_penalty = self._clamp(avg_spread / 2.0, 0.0, 1.0)
        trend_score = (
            0.45 * abs(self._clamp(pressure_diff))
            + 0.25 * abs(self._norm(flow_diff, self.FLOW_NORM_SCALE))
            + 0.20 * abs(dominance_score)
            + 0.10 * (1.0 - spread_penalty)
        )
        if compression_score >= 0.70:
            return "COMPRESSED"
        if trend_score >= 0.42:
            return "TRENDING"
        return "BALANCED"

    def _calc_compression_score(self, pressure_diff, flow_diff, dominance_score, ce_spread, pe_spread, spread_stability):
        pressure_calm = 1.0 - self._clamp(abs(pressure_diff), 0.0, 1.0)
        flow_calm = 1.0 - abs(self._norm(flow_diff, self.FLOW_NORM_SCALE))
        dominance_calm = 1.0 - abs(self._clamp(dominance_score))
        avg_spread = max((ce_spread + pe_spread) / 2.0, 0.0)
        spread_tight = 1.0 - self._clamp(avg_spread / 2.5, 0.0, 1.0)
        score = (
            0.28 * pressure_calm
            + 0.24 * flow_calm
            + 0.22 * dominance_calm
            + 0.14 * spread_tight
            + 0.12 * self._clamp(spread_stability, 0.0, 1.0)
        )
        return self._clamp(score, 0.0, 1.0)

    def _calc_exhaustion_score(self, prev_snapshot, bias, pressure_diff, flow_diff, ce_absorb, pe_absorb, ce_vacuum, pe_vacuum):
        if not prev_snapshot or bias == "NEUTRAL":
            return 0.0

        prev_bias = prev_snapshot.get("bias", "NEUTRAL")
        prev_pressure = self._safe_float(prev_snapshot.get("pressure_diff"))
        prev_flow = self._safe_float(prev_snapshot.get("flow_diff"))
        same_side = prev_bias == bias

        if bias == "BULLISH":
            pressure_weak = self._clamp(max(0.0, prev_pressure - pressure_diff), 0.0, 1.0)
            flow_weak = self._clamp(max(0.0, self._norm(prev_flow - flow_diff, self.FLOW_NORM_SCALE)), 0.0, 1.0)
            opposite_absorb = self._clamp(pe_absorb, 0.0, 1.0)
            dominant_vacuum = self._clamp(ce_vacuum, 0.0, 1.0)
        else:
            pressure_weak = self._clamp(max(0.0, pressure_diff - prev_pressure), 0.0, 1.0)
            flow_weak = self._clamp(max(0.0, self._norm(flow_diff - prev_flow, self.FLOW_NORM_SCALE)), 0.0, 1.0)
            opposite_absorb = self._clamp(ce_absorb, 0.0, 1.0)
            dominant_vacuum = self._clamp(pe_vacuum, 0.0, 1.0)

        bias_bonus = 0.12 if same_side else 0.0
        score = (
            0.28 * pressure_weak
            + 0.28 * flow_weak
            + 0.20 * opposite_absorb
            + 0.12 * dominant_vacuum
            + bias_bonus
        )
        return self._clamp(score, 0.0, 1.0)

    def update(self, index, underlying, ce, pe):
        now = int(time.time())
        s = self.state.setdefault(index, {"last_sec": None})

        if s["last_sec"] == now:
            return None

        s["last_sec"] = now

        try:
            underlying_val = self._safe_float(underlying)
            ce_ltp = self._extract_feature(ce, "ltp")
            pe_ltp = self._extract_feature(pe, "ltp")
            ce_pressure = self._extract_feature(ce, "imbalance_5", "pressure")
            pe_pressure = self._extract_feature(pe, "imbalance_5", "pressure")
            ce_flow = self._extract_feature(ce, "flow")
            pe_flow = self._extract_feature(pe, "flow")
            ce_spread = self._extract_spread(ce)
            pe_spread = self._extract_spread(pe)
            ce_ofi = self._extract_feature(ce, "ofi", "order_flow_imbalance")
            pe_ofi = self._extract_feature(pe, "ofi", "order_flow_imbalance")
            ce_absorb = self._clamp(self._extract_feature(ce, "absorb", "absorption_score", default=0.0), 0.0, 1.0)
            pe_absorb = self._clamp(self._extract_feature(pe, "absorb", "absorption_score", default=0.0), 0.0, 1.0)
            ce_vacuum = self._clamp(self._extract_feature(ce, "vacuum", "vacuum_score", default=0.0), 0.0, 1.0)
            pe_vacuum = self._clamp(self._extract_feature(pe, "vacuum", "vacuum_score", default=0.0), 0.0, 1.0)
        except Exception as e:
            print(f"❌ SNAPSHOT_ERROR | {index} | {e}")
            return None

        pressure_diff = ce_pressure - pe_pressure
        flow_diff = ce_flow - pe_flow
        ofi_diff = ce_ofi - pe_ofi
        absorb_skew = ce_absorb - pe_absorb

        if pressure_diff > self.PRESSURE_BIAS_THRESHOLD:
            bias = "BULLISH"
        elif pressure_diff < -self.PRESSURE_BIAS_THRESHOLD:
            bias = "BEARISH"
        else:
            bias = "NEUTRAL"

        spread_stability = self._spread_stability(index, ce_spread, pe_spread)
        dominance_score = self._calc_dominance_score(pressure_diff, flow_diff, ofi_diff, absorb_skew)
        if dominance_score > self.DOMINANCE_THRESHOLD:
            dominance_side = "BULLISH"
        elif dominance_score < -self.DOMINANCE_THRESHOLD:
            dominance_side = "BEARISH"
        else:
            dominance_side = "NEUTRAL"

        compression_score = self._calc_compression_score(
            pressure_diff,
            flow_diff,
            dominance_score,
            ce_spread,
            pe_spread,
            spread_stability,
        )

        prev_snapshot = s.get("last_snapshot")
        exhaustion_score = self._calc_exhaustion_score(
            prev_snapshot,
            bias,
            pressure_diff,
            flow_diff,
            ce_absorb,
            pe_absorb,
            ce_vacuum,
            pe_vacuum,
        )

        market_regime = self._calc_market_regime(
            pressure_diff,
            flow_diff,
            dominance_score,
            ce_spread,
            pe_spread,
            compression_score,
        )

        snapshot = {
            "ts": now,
            "index": index,
            "underlying": underlying_val,
            "ce_ltp": ce_ltp,
            "pe_ltp": pe_ltp,
            "pressure_diff": pressure_diff,
            "flow_diff": flow_diff,
            "bias": bias,
            "ce_pressure": ce_pressure,
            "pe_pressure": pe_pressure,
            "ce_flow": ce_flow,
            "pe_flow": pe_flow,
            "ce_spread": ce_spread,
            "pe_spread": pe_spread,
            "ce_ofi": ce_ofi,
            "pe_ofi": pe_ofi,
            "ce_absorb": ce_absorb,
            "pe_absorb": pe_absorb,
            "ce_vacuum": ce_vacuum,
            "pe_vacuum": pe_vacuum,
            "synthetic_spread": ce_ltp + pe_ltp,
            "call_put_ratio": ce_ltp / max(pe_ltp, 1e-6),
            "premium_imbalance": pe_ltp - ce_ltp,
            "dominance_score": dominance_score,
            "dominance_side": dominance_side,
            "market_regime": market_regime,
            "compression_score": compression_score,
            "exhaustion_score": exhaustion_score,
        }

        s["last_snapshot"] = snapshot

        print(
            f"📊 SNAPSHOT | {index} | bias={bias} | "
            f"dom={dominance_side}({dominance_score:.2f}) | "
            f"regime={market_regime} | "
            f"u={underlying_val:.2f} | ce={ce_ltp:.2f} | pe={pe_ltp:.2f} | "
            f"pDiff={pressure_diff:.2f} | fDiff={flow_diff:.0f} | "
            f"comp={compression_score:.2f} | exh={exhaustion_score:.2f}"
        )

        return snapshot
