# Isolated stock percentage paper service

This service is separate from the NIFTY/BANKNIFTY options runtime. It uses one
NSE cash-equity full-quote WebSocket and has its own signal state, portfolio,
cooldowns, risk limits, and logs. Its connection count is pinned to one and
does not inherit option-stream sharding variables.

## Railway service

Create a second Railway service from the same repository. Do not change the
existing options service command.

Use this start command only on the stock service:

```text
python -m dhan_engine.interfaces.cli.run_stock_paper
```

Required variables:

```text
DHAN_CLIENT_ID=<client id>
DHAN_ACCESS_TOKEN=<daily token>
STOCK_PAPER_SYMBOLS=RELIANCE,ICICIBANK,SBIN,HDFCBANK
```

Optional paper controls:

```text
STOCK_PAPER_CAPITAL=500000
STOCK_PAPER_NOTIONAL=75000
STOCK_PAPER_MAX_POSITIONS=2
STOCK_PAPER_MAX_DAILY_LOSS=3000
STOCK_PAPER_MAX_DAILY_TRADES=20
STOCK_PERCENT_ENTRY_SCORE=72
STOCK_PERCENT_EXIT_SCORE=40
```

## Validation logs

Confirm all four `STOCK_PROFILE_REGISTERED` lines, followed by:

- `STOCK_PAPER_RUNTIME_ACTIVE`
- `STOCK_FEED_HEALTH` with low tick ages and `stale=none`
- `STOCK_PERCENT_STATE` for every symbol
- `STOCK_ENTRY_COMMITTED` only after warmup and a confirmed score
- `STOCK_TRADE_SUMMARY` for closed paper positions

This runtime is paper-only. It does not place broker orders or provide a
guaranteed hedge against losses in the index-options strategy.
