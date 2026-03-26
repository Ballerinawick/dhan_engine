# depth_micro_features.py
import time
from collections import deque
from typing import Dict, Tuple

from dhan_engine.infrastructure.dhan.async_depth_adapter import DepthSide


class DepthMicroFeatureBuilder:
    """
    Builds:
      - bid_qty, ask_qty (top1)
      - imbalance_5
      - flow (top5 delta)
      - vacuum_flag (liquidity pull)
      - absorption_flag + absorption_strength
      - ltp proxy from mid/last known
    """

    def __init__(self):
        # per security state
        self._prev_top5: Dict[int, Tuple[int, int]] = {}  # (sumBid5, sumAsk5)
        self._prev_best: Dict[int, Tuple[float, float, int, int]] = {}  # (bidPx, askPx, bidQty, askQty)
        self._prev_bid1: Dict[int, int] = {}
        self._prev_ask1: Dict[int, int] = {}

        # stability buffer for absorption detection
        self._mid_buf: Dict[int, deque] = {}   # mid prices
        self._best_qty_buf: Dict[int, deque] = {}  # (bidQty, askQty)

    @staticmethod
    def _sum_top(side: DepthSide, k: int) -> int:
        return int(sum(side.qty[:k])) if side.qty else 0

    def build(self, secid: int, bid: DepthSide, ask: DepthSide) -> dict:
        # best levels
        best_bid_px = float(bid.prices[0]) if bid.prices else 0.0
        best_ask_px = float(ask.prices[0]) if ask.prices else 0.0
        best_bid_qty = int(bid.qty[0]) if bid.qty else 0
        best_ask_qty = int(ask.qty[0]) if ask.qty else 0

        prev_bid = self._prev_bid1.get(secid, best_bid_qty)
        prev_ask = self._prev_ask1.get(secid, best_ask_qty)

        ofi = (best_bid_qty - prev_bid) - (best_ask_qty - prev_ask)

        self._prev_bid1[secid] = best_bid_qty
        self._prev_ask1[secid] = best_ask_qty

        # mid + spread
        mid = (best_bid_px + best_ask_px) / 2.0 if best_bid_px and best_ask_px else 0.0
        microprice = 0.0
        if best_bid_qty + best_ask_qty > 0:
            microprice = (
                (best_bid_px * best_ask_qty) +
                (best_ask_px * best_bid_qty)
            ) / (best_bid_qty + best_ask_qty)
        spread = (best_ask_px - best_bid_px) if best_bid_px and best_ask_px else 0.0

        # top5
        sum_bid5 = self._sum_top(bid, 5)
        sum_ask5 = self._sum_top(ask, 5)

        # imbalance
        denom = (sum_bid5 + sum_ask5)
        imbalance_5 = ((sum_bid5 - sum_ask5) / denom) if denom > 0 else 0.0

        # flow (delta of top5 liquidity)
        prev = self._prev_top5.get(secid)
        if prev:
            prev_bid5, prev_ask5 = prev
            flow = (sum_bid5 - prev_bid5) - (sum_ask5 - prev_ask5)
        else:
            flow = 0.0
        self._prev_top5[secid] = (sum_bid5, sum_ask5)

        # vacuum: sudden pull on one side OR spread expansion + thinning
        prev_best = self._prev_best.get(secid)
        vacuum_flag = False
        vacuum_strength = 0.0
        if prev_best:
            pbid_px, pask_px, pbid_qty, pask_qty = prev_best
            # pull definition (simple + robust)
            pull_bid = (best_bid_qty < pbid_qty * 0.45) and (best_bid_px <= pbid_px)
            pull_ask = (best_ask_qty < pask_qty * 0.45) and (best_ask_px >= pask_px)
            spread_jump = spread > (pask_px - pbid_px) * 1.8 if (pbid_px and pask_px) else False
            vacuum_flag = pull_bid or pull_ask or spread_jump

            if pull_bid:
                vacuum_strength = min(1.0, (pbid_qty - best_bid_qty) / max(pbid_qty, 1))

            elif pull_ask:
                vacuum_strength = min(1.0, (pask_qty - best_ask_qty) / max(pask_qty, 1))
        self._prev_best[secid] = (best_bid_px, best_ask_px, best_bid_qty, best_ask_qty)

        # absorption: mid stable but best qty increases (trapping)
        mb = self._mid_buf.setdefault(secid, deque(maxlen=6))
        qb = self._best_qty_buf.setdefault(secid, deque(maxlen=6))
        mb.append(mid)
        qb.append((best_bid_qty, best_ask_qty))

        absorption_flag = False
        absorption_strength = 0.0
        if len(mb) >= 6:
            # mid stability
            mid_range = max(mb) - min(mb)
            # qty build-up
            bid_build = qb[-1][0] - qb[0][0]
            ask_build = qb[-1][1] - qb[0][1]

            # If price not moving but liquidity stacking -> absorption
            if mid_range <= 0.10 and (bid_build > 0 or ask_build > 0):
                absorption_flag = True
                # normalize (keep bounded)
                absorption_strength = min(1.0, (abs(bid_build) + abs(ask_build)) / 50000.0)

        pressure_score = (
            0.35 * imbalance_5 +
            0.25 * (ofi / 1000.0) +
            0.20 * (flow / 1000.0) +
            0.10 * vacuum_strength +
            0.10 * absorption_strength
        )
        pressure_score = max(min(pressure_score, 1.0), -1.0)

        pressure_side = "NEUTRAL"
        if pressure_score > 0.15:
            pressure_side = "BUY"
        elif pressure_score < -0.15:
            pressure_side = "SELL"

        # LTP proxy:
        # In depth feed you don't get last trade here, so we use mid as "ltp-like" for engine inputs.
        # (Later: if you also run a separate LTP feed, replace this with true LTP.)
        ltp_proxy = mid if mid > 0 else best_bid_px or best_ask_px or 0.0

        return {
            # core price
            "ltp": ltp_proxy,
            "ltp_source": "DEPTH_MID",
            "bid_price": float(best_bid_px),
            "ask_price": float(best_ask_px),
            "best_bid": float(best_bid_px),
            "best_ask": float(best_ask_px),

            # microstructure
            "bid_qty": best_bid_qty,
            "ask_qty": best_ask_qty,
            "imbalance_5": float(imbalance_5),
            "flow": float(flow),
            "ofi": float(ofi),

            "vacuum_flag": bool(vacuum_flag),
            "vacuum_strength": float(vacuum_strength),
            "vacuum_score": float(vacuum_strength),
            "vacuum": float(vacuum_strength),

            "absorption_flag": bool(absorption_flag),
            "absorption_strength": float(absorption_strength),
            "absorption_score": float(absorption_strength),
            "absorb": float(absorption_strength),

            "microprice": float(microprice),
            "spread": float(spread),

            "pressure_score": float(pressure_score),
            "pressure": float(pressure_score),
            "pressure_side": pressure_side,
            "ts": time.time(),
        }
