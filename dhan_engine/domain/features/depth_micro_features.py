# depth_micro_features.py
import time
from collections import deque
from typing import Dict, Tuple

from dhan_engine.infrastructure.dhan.async_depth_adapter import DepthSide


class DepthMicroFeatureBuilder:
    """
    PRO v2
    Existing fields preserved + adds:

    velocity
    accel
    deceleration
    real_flow
    spoof_risk
    bid_refill_score
    ask_refill_score
    bull_turn_score
    bear_turn_score
    micro_signal
    trend_fatigue
    """

    def __init__(self):
        self._prev_top5: Dict[int, Tuple[int, int]] = {}
        self._prev_best: Dict[int, Tuple[float, float, int, int]] = {}
        self._prev_bid1: Dict[int, int] = {}
        self._prev_ask1: Dict[int, int] = {}

        self._mid_buf: Dict[int, deque] = {}
        self._best_qty_buf: Dict[int, deque] = {}

        # PRO buffers
        self._mid_hist: Dict[int, deque] = {}
        self._vel_hist: Dict[int, deque] = {}
        self._flow_hist: Dict[int, deque] = {}
        self._ofi_hist: Dict[int, deque] = {}

    @staticmethod
    def _sum_top(side: DepthSide, k: int) -> int:
        return int(sum(side.qty[:k])) if side.qty else 0

    @staticmethod
    def _clamp(v, lo=0.0, hi=1.0):
        return max(lo, min(hi, v))

    def build(self, secid: int, bid: DepthSide, ask: DepthSide) -> dict:
        # ---------------------------
        # BEST LEVELS
        # ---------------------------
        best_bid_px = float(bid.prices[0]) if bid.prices else 0.0
        best_ask_px = float(ask.prices[0]) if ask.prices else 0.0
        best_bid_qty = int(bid.qty[0]) if bid.qty else 0
        best_ask_qty = int(ask.qty[0]) if ask.qty else 0

        prev_bid = self._prev_bid1.get(secid, best_bid_qty)
        prev_ask = self._prev_ask1.get(secid, best_ask_qty)

        ofi = (best_bid_qty - prev_bid) - (best_ask_qty - prev_ask)

        self._prev_bid1[secid] = best_bid_qty
        self._prev_ask1[secid] = best_ask_qty

        # ---------------------------
        # PRICE
        # ---------------------------
        mid = (best_bid_px + best_ask_px) / 2 if best_bid_px and best_ask_px else 0.0

        microprice = 0.0
        if best_bid_qty + best_ask_qty > 0:
            microprice = (
                (best_bid_px * best_ask_qty)
                + (best_ask_px * best_bid_qty)
            ) / (best_bid_qty + best_ask_qty)

        spread = (best_ask_px - best_bid_px) if best_bid_px and best_ask_px else 0.0

        # ---------------------------
        # TOP5
        # ---------------------------
        sum_bid5 = self._sum_top(bid, 5)
        sum_ask5 = self._sum_top(ask, 5)

        denom = sum_bid5 + sum_ask5
        imbalance_5 = ((sum_bid5 - sum_ask5) / denom) if denom > 0 else 0.0

        prev = self._prev_top5.get(secid)
        if prev:
            prev_bid5, prev_ask5 = prev
            flow = (sum_bid5 - prev_bid5) - (sum_ask5 - prev_ask5)
        else:
            flow = 0.0

        self._prev_top5[secid] = (sum_bid5, sum_ask5)

        # ---------------------------
        # HISTORY
        # ---------------------------
        mh = self._mid_hist.setdefault(secid, deque(maxlen=8))
        vh = self._vel_hist.setdefault(secid, deque(maxlen=8))
        fh = self._flow_hist.setdefault(secid, deque(maxlen=8))
        oh = self._ofi_hist.setdefault(secid, deque(maxlen=8))

        prev_mid = mh[-1] if mh else mid
        velocity = mid - prev_mid
        mh.append(mid)

        prev_vel = vh[-1] if vh else velocity
        accel = velocity - prev_vel
        vh.append(velocity)

        fh.append(flow)
        oh.append(ofi)

        # sellers slowing / buyers slowing
        deceleration = -accel

        # ---------------------------
        # REAL FLOW (smooth)
        # ---------------------------
        real_flow = sum(fh) / len(fh) if fh else 0.0

        # spoof risk
        spoof_risk = 0.0
        if len(fh) >= 3:
            if abs(fh[-1]) > 2000 and abs(fh[-2]) > 2000 and (fh[-1] * fh[-2] < 0):
                spoof_risk = 0.8

        # ---------------------------
        # VACUUM
        # ---------------------------
        vacuum_flag = False
        vacuum_strength = 0.0

        prev_best = self._prev_best.get(secid)
        if prev_best:
            pbid_px, pask_px, pbid_qty, pask_qty = prev_best

            pull_bid = best_bid_qty < pbid_qty * 0.45
            pull_ask = best_ask_qty < pask_qty * 0.45

            spread_jump = spread > max((pask_px - pbid_px) * 1.8, 0.05)

            vacuum_flag = pull_bid or pull_ask or spread_jump

            if pull_bid:
                vacuum_strength = self._clamp((pbid_qty - best_bid_qty) / max(pbid_qty, 1))

            elif pull_ask:
                vacuum_strength = self._clamp((pask_qty - best_ask_qty) / max(pask_qty, 1))

        self._prev_best[secid] = (
            best_bid_px,
            best_ask_px,
            best_bid_qty,
            best_ask_qty,
        )

        # ---------------------------
        # ABSORPTION
        # ---------------------------
        mb = self._mid_buf.setdefault(secid, deque(maxlen=6))
        qb = self._best_qty_buf.setdefault(secid, deque(maxlen=6))

        mb.append(mid)
        qb.append((best_bid_qty, best_ask_qty))

        absorption_flag = False
        absorption_strength = 0.0

        if len(mb) >= 6:
            mid_range = max(mb) - min(mb)
            bid_build = qb[-1][0] - qb[0][0]
            ask_build = qb[-1][1] - qb[0][1]

            if mid_range <= 0.10 and (bid_build > 0 or ask_build > 0):
                absorption_flag = True
                absorption_strength = self._clamp(
                    (abs(bid_build) + abs(ask_build)) / 30000.0
                )

        # ---------------------------
        # REFILL DETECTION
        # ---------------------------
        bid_refill_score = 0.0
        ask_refill_score = 0.0

        if best_bid_qty > prev_bid and velocity >= 0:
            bid_refill_score = self._clamp((best_bid_qty - prev_bid) / 3000.0)

        if best_ask_qty > prev_ask and velocity <= 0:
            ask_refill_score = self._clamp((best_ask_qty - prev_ask) / 3000.0)

        # ---------------------------
        # PRESSURE
        # ---------------------------
        pressure_score = (
            0.30 * imbalance_5 +
            0.22 * (ofi / 1000.0) +
            0.18 * (real_flow / 1000.0) +
            0.15 * vacuum_strength +
            0.15 * absorption_strength
        )

        pressure_score = max(min(pressure_score, 1.0), -1.0)

        pressure_side = "NEUTRAL"
        if pressure_score > 0.15:
            pressure_side = "BUY"
        elif pressure_score < -0.15:
            pressure_side = "SELL"

        # ---------------------------
        # TURN SCORES
        # ---------------------------
        bull_turn_score = (
            0.25 * self._clamp(imbalance_5, 0, 1)
            + 0.20 * self._clamp(ofi / 2000.0, 0, 1)
            + 0.20 * self._clamp(real_flow / 3000.0, 0, 1)
            + 0.20 * bid_refill_score
            + 0.15 * self._clamp(deceleration, 0, 1)
        )

        bear_turn_score = (
            0.25 * self._clamp(-imbalance_5, 0, 1)
            + 0.20 * self._clamp(-ofi / 2000.0, 0, 1)
            + 0.20 * self._clamp(-real_flow / 3000.0, 0, 1)
            + 0.20 * ask_refill_score
            + 0.15 * self._clamp(-deceleration, 0, 1)
        )

        if spoof_risk > 0.5:
            bull_turn_score *= 0.7
            bear_turn_score *= 0.7

        bull_turn_score = self._clamp(bull_turn_score)
        bear_turn_score = self._clamp(bear_turn_score)

        micro_signal = "NEUTRAL"

        if bull_turn_score >= 0.62 and bull_turn_score > bear_turn_score:
            micro_signal = "PRE_BULLISH_TURN"

        elif bear_turn_score >= 0.62 and bear_turn_score > bull_turn_score:
            micro_signal = "PRE_BEARISH_TURN"

        trend_fatigue = self._clamp(abs(deceleration))

        # ---------------------------
        # LTP PROXY
        # ---------------------------
        ltp_proxy = microprice if microprice > 0 else mid

        return {
            "ltp": ltp_proxy,
            "ltp_source": "DEPTH_MICROPRICE",

            "bid_price": best_bid_px,
            "ask_price": best_ask_px,
            "best_bid": best_bid_px,
            "best_ask": best_ask_px,

            "bid_qty": best_bid_qty,
            "ask_qty": best_ask_qty,

            "imbalance_5": imbalance_5,
            "flow": flow,
            "real_flow": real_flow,
            "ofi": ofi,

            "velocity": velocity,
            "accel": accel,
            "deceleration": deceleration,

            "vacuum_flag": vacuum_flag,
            "vacuum_strength": vacuum_strength,
            "vacuum_score": vacuum_strength,
            "vacuum": vacuum_strength,

            "absorption_flag": absorption_flag,
            "absorption_strength": absorption_strength,
            "absorption_score": absorption_strength,
            "absorb": absorption_strength,

            "bid_refill_score": bid_refill_score,
            "ask_refill_score": ask_refill_score,

            "bull_turn_score": bull_turn_score,
            "bear_turn_score": bear_turn_score,
            "micro_signal": micro_signal,
            "trend_fatigue": trend_fatigue,

            "spoof_risk": spoof_risk,

            "microprice": microprice,
            "spread": spread,

            "pressure_score": pressure_score,
            "pressure": pressure_score,
            "pressure_side": pressure_side,

            "ts": time.time(),
        }