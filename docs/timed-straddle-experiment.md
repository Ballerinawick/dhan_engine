# Timed Reverse Iron-Fly Experiment

This is an isolated paper-only Railway service. It does not use TriWave signals and does not place broker orders.

## Loop

1. Between 09:15 and 15:25 IST, select the nearest-expiry ATM strike and equal-distance wings.
2. Buy the ATM CE and PE at their asks; sell the upper CE and lower PE at their bids.
3. Reject a structure whose maximum possible net profit cannot reach the configured target.
4. Mark long legs at bids and short legs at asks, then deduct the configured complete-cycle cost.
5. Exit early when combined net P&L reaches the target; otherwise exit after 300 seconds.
6. Select the current ATM pair and repeat, up to 74 entries.
7. Do not start a new cycle at or after 15:25; force-close an open cycle at 15:30.

## Railway variables

Required:

- `DHAN_SERVICE=timed-straddle`
- `DHAN_CLIENT_ID`
- `DHAN_ACCESS_TOKEN`
- `MONGODB_URI` or `TRADE_SUMMARY_MONGO_URI`

Defaults:

- `TIMED_STRADDLE_INDEX=NIFTY`
- `TIMED_STRADDLE_HOLD_SEC=180`
- `TIMED_STRADDLE_PROFIT_TARGET_NET=100`
- `TIMED_STRADDLE_ROUND_TRIP_COST=160`
- `TIMED_STRADDLE_MAX_CYCLES=74`
- `TIMED_STRADDLE_LOTS=1`
- `TIMED_STRADDLE_WING_STEPS=1`
- `TIMED_STRADDLE_LOT_SIZE=0` (zero reads the current lot size from the instrument master)
- `TIMED_STRADDLE_QUOTE_STALE_SEC=3`
- `TIMED_STRADDLE_MONGO_COLLECTION=timed_straddle_experiments`
- `TIMED_STRADDLE_PORTFOLIO_COLLECTION=timed_straddle_daily`

The ₹160 default models eight order executions per four-leg cycle. With 74 completed cycles it records ₹11,840 in modeled costs. It is not a broker contract note and should be reconciled with actual charges before any live-order work.

## Evidence in logs and MongoDB

- `TIMED_STRADDLE_PAIR_SELECTED`
- `TIMED_STRADDLE_ENTRY`
- `TIMED_STRADDLE_EXIT`
- `TIMED_STRADDLE_HEALTH`

Each completed record stores all four leg entries, exits and P&L values; combined gross/net P&L; modeled fees; net debit; capped maximum profit; hold duration; MFE/MAE; strikes; expiry; lot size; and exit reason.
