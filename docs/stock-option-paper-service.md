# Isolated Stock Option Paper Service

This opt-in service applies the merged PR 283 futures/options hybrid to
`HDFCBANK` and `RELIANCE` without importing or starting the NIFTY live runtime.

For each stock it maintains:

- one nearest-expiry FUTSTK 200-depth connection;
- one synchronized FUTSTK Full Quote subscription;
- one same-expiry ATM CE/PE pair selected from the refreshed instrument master;
- one independent LTP-anchored market-by-price worker;
- one independent executable V1 strategy ledger and V2 long-option portfolio;
- separate state history, capital, lot size, position and S3 partitions.

The two stocks share one Full Quote WebSocket. Dhan's 200-depth contract requires
one WebSocket per instrument, so this service opens two depth connections. The
existing NIFTY process and its connections are not changed.

## Deployment

The service is intentionally not installed or enabled by the normal NIFTY deploy
script. After the image containing this code has been deployed, install it once:

```bash
sudo install -d -m 0755 /var/lib/dhan-engine-stock-options
sudo install -m 0644 deploy/aws/ec2/dhan-engine-stock-options.service \
  /etc/systemd/system/dhan-engine-stock-options.service
sudo cp deploy/aws/ec2/dhan-engine-stock-options.env.example \
  /etc/dhan-engine/stock-options.env
sudo chmod 600 /etc/dhan-engine/stock-options.env
```

Set `DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN`, and `DEEPLOB_S3_BUCKET` in the new env
file. Then start only the stock service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now dhan-engine-stock-options
sudo systemctl status dhan-engine-stock-options --no-pager -l
```

Stopping the stock experiment does not stop NIFTY:

```bash
sudo systemctl stop dhan-engine-stock-options
```

## Storage

Raw depth and daily ledgers are separated from NIFTY:

```text
market-data/deeplob-stock-options/schema=v1/trade_date=YYYY-MM-DD/index=HDFCBANK/...
market-data/deeplob-stock-options/schema=v1/trade_date=YYYY-MM-DD/index=RELIANCE/...
paper-trades/stock-options/schema=v2/trade_date=YYYY-MM-DD/index=HDFCBANK/daily-trades.json
paper-trades/stock-options/schema=v2/trade_date=YYYY-MM-DD/index=RELIANCE/daily-trades.json
```

Useful logs:

```bash
sudo journalctl -u dhan-engine-stock-options -f -o cat | \
grep --line-buffered -E \
'STOCK_OPTION_(PAPER_ACTIVE|CONTRACTS|STATE|ENTRY|TRADE_SUMMARY|HEALTH|COMPOSITE_REJECTED)'
```
