# EC2 DeepLOB live pipeline

`DHAN_SERVICE=deeplob-live` uses two market-data connections for NIFTY:

1. One Full Quote connection for LTP, LTQ, volume, OI and OHLC.
2. One 200-depth connection for the NIFTY futures order book.

Every complete 200-depth bid/ask snapshot is offered to a bounded Parquet
recorder queue. A separate bounded inference queue samples the same snapshot
stream at the model cadence. S3 writes and Torch inference never run in a
WebSocket callback.

## S3 layout

Objects are Zstandard-compressed Parquet files:

```text
s3://dhan-engine-market-data-774075584062/
  market-data/deeplob/
    schema=v1/
      index=NIFTY/
        expiry=YYYY-MM-DD/
          trade_date=YYYY-MM-DD/
            instrument=NIFTY_FUT/
              symbol=.../
                hour=HH/
                  depth-FIRST_NS-LAST_NS-ROWS.parquet
```

Rows contain nanosecond receipt time, full-quote fields, and 200 levels of bid
and ask price, quantity and order count. Capture stores every complete book
event. Offline training resamples the event stream to `250ms`, matching live
inference.

## Required IAM access

The EC2 instance role needs `s3:ListBucket` on the bucket and
`s3:GetObject`/`s3:PutObject` on these prefixes:

```text
arn:aws:s3:::dhan-engine-market-data-774075584062/market-data/deeplob/*
arn:aws:s3:::dhan-engine-market-data-774075584062/models/production/*
```

Upload a validated model pair before deploying the combined service:

```text
models/production/deeplob.pt
models/production/deeplob.json
```

The deploy script downloads both files atomically and the runtime refuses to
start if the model metadata does not match the configured sampling cadence.

## Runtime evidence

```bash
sudo journalctl -u dhan-engine -f | grep -E \
  "DEEPLOB_LIVE|DEEPLOB_RECORDER|DEEPLOB_S3|DEEPLOB_INFERENCE|DEEPLOB_PAPER_PREDICTION"
```

Healthy operation shows:

- `DEEPLOB_LIVE_PIPELINE_ACTIVE` with `fullquote=true`, `recorder=true`,
  `inference=true`, and `orders=false`.
- `DEEPLOB_LIVE_PIPELINE_HEALTH` with both workers alive and dispatch failures
  at zero.
- `DEEPLOB_RECORDER_HEALTH` with received/written increasing and dropped at
  zero.
- `DEEPLOB_S3_UPLOAD_OK` for each Parquet chunk.
- `DEEPLOB_INFERENCE_HEALTH` with predictions increasing and stale/dropped at
  zero.
- `DEEPLOB_PAPER_PREDICTION` with DOWN/FLAT/UP probabilities.

This service produces paper observations only. It does not place broker orders.

