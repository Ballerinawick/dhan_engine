# Isolated NIFTY cross paper book

This is the sole paper method selected by the EC2 deployment template (`DHAN_SERVICE=nifty-cross-paper`). Other paper implementations remain in the repository but are not started by the main service. The deploy script also disables the separate stock-options paper service when it is installed. The cross book never places broker orders. For a local run with the same Dhan credentials:

```bash
export DHAN_CLIENT_ID=...
export DHAN_ACCESS_TOKEN=...
export DHAN_MASTER_CSV=data/api-scrip-master.csv
export NIFTY_CROSS_STATE_PATH=/var/lib/dhan-engine/nifty-cross-paper.json
python -m dhan_engine.interfaces.cli.run_nifty_cross_paper
```

The process loads the NIFTY front-month future, the nearest nonexpired option expiry for underlying 13/IDX_I, and a five-strike CE/PE window. It polls Dhan LTP about once per second, refreshes the strike window about every two minutes, and saves the separate daily JSON book about five seconds after changes. It refreshes the instrument master at startup.

Prices in this book are **idealized paper prices**. Entries use interpolated cross premiums after a finished-minute signal, and a turned winner uses the earlier high quote. These are deliberately the requested rules, but they are not executable-price estimates. Do not use this book as a live-order signal or interpret its P&L as achievable returns.

