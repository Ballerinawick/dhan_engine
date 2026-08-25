# Stock Futures Depth to Cash Paper Service

This service is isolated from the NIFTY DeepLOB runtime. It reads each configured
stock's nearest `FUTSTK` contract using Dhan 200-depth plus synchronized Full
Quote/LTP, then paper-trades the corresponding NSE cash equity.

## Direction mapping

- Confirmed bullish futures state: open a cash-equity `LONG` position.
- Confirmed bearish futures state: open a cash-equity `SHORT` position.
- Confirmed opposite state: close the current position first. A reversal cannot
  open on the same tick.
- Sustained uncertain state: close the position.
- Stale cash quotes and the configured market close force positions flat.

Entries use executable cash prices, dynamic NSE intraday charges, quote freshness,
spread quality, signal confirmation, and expected edge after costs. This remains a
paper experiment; it does not place broker orders or guarantee positive PnL.

## Separate service

Run this as a second Railway or systemd/container service. Do not replace the
existing NIFTY service environment.

```text
DHAN_SERVICE=stock-depth-paper
STOCK_DEPTH_SYMBOLS=RELIANCE,HDFCBANK
DEEPLOB_S3_BUCKET=<market-data-bucket>
STOCK_DEPTH_TRADE_S3_PREFIX=paper-trades/stock-depth
```

The Dhan 200-depth adapter supports at most five configured stock futures. The
service uses one shared Full Quote WebSocket for futures and cash instruments plus
one 200-depth connection per stock future.

## Daily ledger

Closed trades are asynchronously consolidated into one object per trading day:

```text
paper-trades/stock-depth/schema=v2/trade_date=YYYY-MM-DD/index=STOCKS/daily-trades.json
```

The runtime refuses to start when `DEEPLOB_S3_BUCKET` is empty, preventing silent
loss of the paper-trade ledger.

Useful log keywords are `STOCK_DEPTH_PAPER_ACTIVE`, `STOCK_DEPTH_SIGNAL`,
`STOCK_DEPTH_PAPER_ENTRY`, `STOCK_DEPTH_TRADE_SUMMARY`, `STOCK_DEPTH_ENTRY_BLOCKED`,
and `STOCK_DEPTH_HEALTH`.
