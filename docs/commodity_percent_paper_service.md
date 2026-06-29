# Isolated commodity percentage paper service

This service is separate from both the NIFTY/BANKNIFTY options runtime and the
NSE stock paper runtime. It uses one MCX full-quote WebSocket and has its own
commodity signal state, portfolio, cooldowns, risk limits, and logs.

## Railway service

Create a third Railway service from the same repository. Do not change the
existing index/options or stock service commands.

Use this start command only on the commodity service:

```text
python -m dhan_engine.interfaces.cli.run_commodity_paper
```

Required variables:

```text
DHAN_CLIENT_ID=<client id>
DHAN_ACCESS_TOKEN=<daily token>
COMMODITY_PAPER_SYMBOLS=GOLD,CRUDEOIL,NATURALGAS
```

Optional paper controls:

```text
COMMODITY_PAPER_CAPITAL=500000
COMMODITY_PAPER_NOTIONAL=75000
COMMODITY_PAPER_MAX_POSITIONS=2
COMMODITY_PAPER_MAX_DAILY_LOSS=3000
COMMODITY_PAPER_MAX_DAILY_TRADES=20
COMMODITY_PERCENT_ENTRY_SCORE=68
COMMODITY_PERCENT_EXIT_SCORE=38
COMMODITY_MARKET_START=09:00
COMMODITY_MARKET_END=23:25
```

## Validation logs

Confirm all three `COMMODITY_PROFILE_REGISTERED` lines, followed by:

- `COMMODITY_PAPER_RUNTIME_ACTIVE`
- `COMMODITY_FEED_HEALTH` with low tick ages and `stale=none`
- `COMMODITY_PERCENT_STATE` for every symbol
- `COMMODITY_ENTRY_COMMITTED` only after warmup and confirmed score
- `COMMODITY_TRADE_SUMMARY` for closed paper positions

This runtime is paper-only. It does not place broker orders.
