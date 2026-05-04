from __future__ import annotations

from typing import Any, Dict, Optional


def _f(v: Any, d: float = 0.0) -> float:
    try:
        if v is None:
            return d
        return float(v)
    except Exception:
        return d


def _i(v: Any, d: int = 0) -> int:
    try:
        if v is None:
            return d
        return int(float(v))
    except Exception:
        return d


def derive_full_data_features(raw: dict, previous: Optional[dict] = None) -> dict:
    previous = previous or {}
    depth = raw.get("depth") or []
    bids = [d for d in depth if _f(d.get("bid_price")) > 0]
    asks = [d for d in depth if _f(d.get("ask_price")) > 0]

    ltp = _f(raw.get("LTP") or raw.get("ltp"))
    ltq = _i(raw.get("LTQ") or raw.get("ltq"))
    avg_price = _f(raw.get("avg_price"))
    volume = _i(raw.get("volume"))
    open_price = _f(raw.get("open"))
    prev_close = _f(raw.get("close"))
    day_high = _f(raw.get("high"))
    day_low = _f(raw.get("low"))
    day_range = max(day_high - day_low, 1e-9)
    day_position = (ltp - day_low) / day_range if day_high > day_low and ltp > 0 else 0.5

    best_bid = _f(bids[0].get("bid_price")) if bids else 0.0
    best_ask = _f(asks[0].get("ask_price")) if asks else 0.0
    spread = max(best_ask - best_bid, 0.0) if best_ask and best_bid else 0.0
    spread_pct = (spread / max(ltp, 1e-9)) * 100.0

    bid_qty = [_i(d.get("bid_quantity")) for d in bids[:5]]
    ask_qty = [_i(d.get("ask_quantity")) for d in asks[:5]]
    bid_orders = [_i(d.get("bid_orders")) for d in bids[:5]]
    ask_orders = [_i(d.get("ask_orders")) for d in asks[:5]]

    bid_qty_5 = sum(bid_qty)
    ask_qty_5 = sum(ask_qty)
    top_bid_qty = bid_qty[0] if bid_qty else 0
    top_ask_qty = ask_qty[0] if ask_qty else 0
    bid_orders_5 = sum(bid_orders)
    ask_orders_5 = sum(ask_orders)

    depth_den = max(bid_qty_5 + ask_qty_5, 1)
    depth_imbalance_5 = (bid_qty_5 - ask_qty_5) / depth_den
    top_den = max(top_bid_qty + top_ask_qty, 1)
    top_depth_imbalance = (top_bid_qty - top_ask_qty) / top_den
    bid_wall_ratio = top_bid_qty / max((bid_qty_5 / 5.0) if bid_qty_5 else 1.0, 1.0)
    ask_wall_ratio = top_ask_qty / max((ask_qty_5 / 5.0) if ask_qty_5 else 1.0, 1.0)

    total_buy_quantity = _i(raw.get("total_buy_quantity"))
    total_sell_quantity = _i(raw.get("total_sell_quantity"))
    mq_den = max(total_buy_quantity + total_sell_quantity, 1)
    market_queue_imbalance = (total_buy_quantity - total_sell_quantity) / mq_den
    buy_sell_qty_ratio = total_buy_quantity / max(total_sell_quantity, 1)
    ltq_vs_depth = ltq / max(depth_den, 1)

    oi = _i(raw.get("OI") or raw.get("oi"))
    oi_day_high = _i(raw.get("oi_day_high"))
    oi_day_low = _i(raw.get("oi_day_low"))
    oi_range = max(oi_day_high - oi_day_low, 1)
    oi_position = (oi - oi_day_low) / oi_range if oi_day_high > oi_day_low else 0.5

    ltp_change = ltp - _f(previous.get("ltp"), ltp)
    ltp_change_pct = (ltp_change / max(_f(previous.get("ltp"), ltp), 1e-9)) * 100.0
    volume_change_tick = volume - _i(previous.get("volume"), volume)
    bid_qty_5_change = bid_qty_5 - _i(previous.get("bid_qty_5"), bid_qty_5)
    ask_qty_5_change = ask_qty_5 - _i(previous.get("ask_qty_5"), ask_qty_5)
    depth_imbalance_change = depth_imbalance_5 - _f(previous.get("depth_imbalance_5"), depth_imbalance_5)
    oi_change_tick = oi - _i(previous.get("oi"), oi)

    bid_support_score = max(0.0, depth_imbalance_5) * 0.4 + max(0.0, top_depth_imbalance) * 0.3 + max(0.0, market_queue_imbalance) * 0.3
    ask_pressure_score = max(0.0, -depth_imbalance_5) * 0.4 + max(0.0, -top_depth_imbalance) * 0.3 + max(0.0, -market_queue_imbalance) * 0.3
    recovery_score = min(1.0, max(0.0, 0.35 * bid_support_score + 0.25 * max(0.0, ltp_change_pct / 2.0) + 0.2 * max(0.0, volume_change_tick / 1000.0) + 0.2 * max(0.0, depth_imbalance_5)))
    exhaustion_score = min(1.0, max(0.0, 0.45 * max(0.0, day_position - 0.7) + 0.35 * ask_pressure_score + 0.2 * max(0.0, -ltp_change_pct / 2.0)))
    clean_trade_score = min(1.0, max(0.0, 0.5 * (1.0 - min(spread_pct / 0.08, 1.0)) + 0.3 * (1.0 - min(abs(depth_imbalance_change), 1.0)) + 0.2 * (1.0 - min(max(0.0, ask_wall_ratio - 1.5), 1.0))))
    spoof_risk = min(1.0, max(0.0, 0.5 * max(0.0, bid_wall_ratio - 2.0) + 0.5 * max(0.0, ask_wall_ratio - 2.0)))

    flow = float(volume_change_tick if ltp_change >= 0 else -abs(volume_change_tick))
    ofi = bid_qty_5_change - ask_qty_5_change
    pressure_score = recovery_score - ask_pressure_score

    return {
        "ltp": ltp, "ltq": ltq, "avg_price": avg_price, "volume": volume, "open_price": open_price,
        "prev_close": prev_close, "day_high": day_high, "day_low": day_low, "day_range": day_range, "day_position": day_position,
        "ltp_vs_avg_pct": ((ltp - avg_price) / max(avg_price, 1e-9)) * 100.0 if avg_price else 0.0,
        "distance_from_day_low_pct": ((ltp - day_low) / max(day_low, 1e-9)) * 100.0 if day_low else 0.0,
        "distance_from_day_high_pct": ((ltp - day_high) / max(day_high, 1e-9)) * 100.0 if day_high else 0.0,
        "intraday_return_pct": ((ltp - open_price) / max(open_price, 1e-9)) * 100.0 if open_price else 0.0,
        "best_bid": best_bid, "best_ask": best_ask, "spread": spread, "spread_pct": spread_pct,
        "top_bid_qty": top_bid_qty, "top_ask_qty": top_ask_qty, "bid_qty_5": bid_qty_5, "ask_qty_5": ask_qty_5,
        "bid_orders_5": bid_orders_5, "ask_orders_5": ask_orders_5, "depth_imbalance_5": depth_imbalance_5,
        "top_depth_imbalance": top_depth_imbalance, "bid_wall_ratio": bid_wall_ratio, "ask_wall_ratio": ask_wall_ratio,
        "total_buy_quantity": total_buy_quantity, "total_sell_quantity": total_sell_quantity,
        "market_queue_imbalance": market_queue_imbalance, "buy_sell_qty_ratio": buy_sell_qty_ratio, "ltq_vs_depth": ltq_vs_depth,
        "oi": oi, "oi_position": oi_position, "oi_change_tick": oi_change_tick,
        "ltp_change": ltp_change, "ltp_change_pct": ltp_change_pct, "volume_change_tick": volume_change_tick,
        "bid_qty_5_change": bid_qty_5_change, "ask_qty_5_change": ask_qty_5_change, "depth_imbalance_change": depth_imbalance_change,
        "bid_support_score": bid_support_score, "ask_pressure_score": ask_pressure_score, "recovery_score": recovery_score,
        "exhaustion_score": exhaustion_score, "clean_trade_score": clean_trade_score, "spoof_risk": spoof_risk,
        "imbalance_5": depth_imbalance_5, "flow": flow, "real_flow": flow, "ofi": ofi,
        "pressure_score": pressure_score, "pressure": pressure_score, "bid_price": best_bid, "ask_price": best_ask,
    }
