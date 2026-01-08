def depth_to_tick(bids, asks):
    top_bids = bids[:5]
    top_asks = asks[:5]

    best_bid = top_bids[0][0]
    best_ask = top_asks[0][0]

    bid_qty = sum(q for _, q, _ in top_bids)
    ask_qty = sum(q for _, q, _ in top_asks)

    spread = best_ask - best_bid
    imbalance = (bid_qty - ask_qty) / max(bid_qty + ask_qty, 1)

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "imbalance": imbalance,
        "bid_qty": bid_qty,
        "ask_qty": ask_qty,
    }
