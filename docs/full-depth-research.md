# Full-depth microstructure research service

This isolated, paper-only service uses Dhan's 200-level NSE F&O feed for exactly
three instruments: the nearest index future, ATM CE, and ATM PE. It never sends
orders. Three depth sockets plus one Full Quote socket stay below Dhan's maximum
of five connections.

```text
DHAN_SERVICE=depth-research
DEPTH_RESEARCH_INDEX=NIFTY
DEPTH_RESEARCH_HORIZON_SEC=5
DEPTH_RESEARCH_ROUND_TRIP_FEE=40
DEPTH_RESEARCH_SLIPPAGE_POINTS=0.5
DEPTH_RESEARCH_MIN_CONFIDENCE=0.60
DEPTH_RESEARCH_STALE_SEC=2
```

The service logs one first snapshot per leg, causal per-book state, and a
cross-leg decision. `ELIGIBLE` means only that the measured move estimate exceeds
modeled spread, slippage, and fees; it is not an order and not a profit promise.
Because the feed is aggregated by price, trade/cancel classification remains an
inference made from LTQ/LTT and book changes.

## Reusable implementation/review prompt

> Build or audit a causal 200-level order-book engine using only information
> available at each packet's local receive time. Reconstruct sorted bid/ask
> snapshots, diff price-level quantity and order counts, calculate distance-
> weighted imbalance, depletion, refill, wall creation/removal, persistence and
> velocity. Infer aggressive trades only when LTQ/LTT and executable quotes
> support it; otherwise label the event unknown. Confirm direction across the
> nearest future, ATM CE, and inverse ATM PE. Reject warm-up, stale, conflicting,
> crossed, or incomplete books. Produce an eligibility signal only when the
> forecast executable premium movement exceeds spread, slippage and round-trip
> fees. Keep it paper-only, log every gate reason, and validate using timestamped
> replay with fees and latency before enabling any broker order path.
