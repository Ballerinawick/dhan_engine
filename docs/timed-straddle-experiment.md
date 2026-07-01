# Timed CE+PE Straddle Experiment

This is an isolated paper-only Railway service. It does not use TriWave signals and does not place broker orders.

## Loop

1. Between 09:15 and 15:25 IST, select the nearest-expiry ATM CE and PE at the same strike.
2. Wait for fresh executable quotes for both legs.
3. Enter both legs at their ask prices.
4. Mark both legs at their bid prices and deduct the configured complete-cycle cost.
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
- `TIMED_STRADDLE_HOLD_SEC=300`
- `TIMED_STRADDLE_PROFIT_TARGET_NET=100`
- `TIMED_STRADDLE_ROUND_TRIP_COST=80`
- `TIMED_STRADDLE_MAX_CYCLES=74`
- `TIMED_STRADDLE_LOTS=1`
- `TIMED_STRADDLE_LOT_SIZE=0` (zero reads the current lot size from the instrument master)
- `TIMED_STRADDLE_QUOTE_STALE_SEC=3`
- `TIMED_STRADDLE_MONGO_COLLECTION=timed_straddle_experiments`
- `TIMED_STRADDLE_PORTFOLIO_COLLECTION=timed_straddle_daily`

The ₹80 default is a configurable estimate. With 74 completed cycles it records ₹5,920 in modeled costs. It is not a broker contract note and should be reconciled with actual charges before any live-order work.

## Evidence in logs and MongoDB

- `TIMED_STRADDLE_PAIR_SELECTED`
- `TIMED_STRADDLE_ENTRY`
- `TIMED_STRADDLE_EXIT`
- `TIMED_STRADDLE_HEALTH`

Each completed record stores both leg entries and exits, CE/PE P&L, combined gross P&L, modeled fees, net P&L, hold duration, MFE/MAE, strike, expiry, lot size, and exit reason.
