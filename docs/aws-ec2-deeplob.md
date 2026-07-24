# EC2 and DeepLOB foundation

This deployment keeps the existing trading services unchanged. The new
`deeplob-recorder` records only the NIFTY nearest-future 200-level book, writes
bounded Parquet/Zstandard chunks, and uploads them to S3.

## AWS resources

1. Create an encrypted S3 bucket with Block Public Access and versioning.
2. Add a lifecycle rule to transition old raw chunks to a cheaper storage class.
3. Create an EC2 role that permits only `s3:PutObject`,
   `s3:AbortMultipartUpload`, and `s3:ListBucket` for the selected prefix.
4. Launch one Ubuntu EC2 instance in Mumbai with an encrypted EBS volume.
5. Restrict SSH to your IP. The recorder needs no inbound application port.
6. Attach the EC2 role. Do not place AWS keys in `.env`.

## Installation

```bash
git clone <repository-url> dhan_engine
cd dhan_engine
sudo bash deploy/aws/ec2/install.sh
sudo nano /etc/dhan-engine/dhan-engine.env
sudo systemctl enable --now dhan-engine
sudo journalctl -u dhan-engine -f
```

On every code release:

```bash
git pull --ff-only
sudo docker build --build-arg DEEPLOB_INSTALL=recorder -t dhan-engine:latest .
sudo systemctl restart dhan-engine
```

The recorder image does not need PyTorch. Build a paper-inference image only
after a model has passed offline replay:

```bash
sudo docker build --build-arg DEEPLOB_INSTALL=inference -t dhan-engine:latest .
sudo mkdir -p /var/lib/dhan-engine/models
# Place one approved .pt artifact and its matching .json metadata here.
sudo nano /etc/dhan-engine/dhan-engine.env
# Set DHAN_SERVICE=deeplob-inference and the two DEEPLOB_*_PATH values.
sudo systemctl restart dhan-engine
```

## Data contract

Every Parquet row is a causally received composite book:

- receive timestamp in UTC nanoseconds;
- security ID and instrument tag;
- 200 bid prices, quantities, and order counts;
- 200 ask prices, quantities, and order counts.

The default sampler retains one complete 200-level snapshot every 250 ms. Raw
packet count, sampled-out count, queue drops, and written rows remain separate
health metrics. Training and inference refuse a mismatched sampling interval.

Files are partitioned by UTC date, instrument, and hour. A SHA-256 sidecar is
written and uploaded for corruption checks. Queue overflow is visible as
`DEEPLOB_RECORDER_HEALTH dropped=...`; any non-zero value invalidates that
capture interval for training.

## Model lifecycle

Live training is prohibited. After at least 60 clean sessions:

1. download a fixed date range from S3;
2. split chronologically by complete trading days;
3. train separate 5-minute and 10-minute artifacts with
   `scripts/train_deeplob.py --horizon-sec 300` and `--horizon-sec 600`;
4. compare against flat, last-move, and imbalance baselines;
5. replay with latency, spread, slippage, and fees;
6. publish the TorchScript file and matching JSON metadata as one immutable
   model version;
7. run paper-only inference for at least 20 sessions.

The model loader refuses missing or malformed metadata. No DeepLOB component is
connected to broker order placement in this foundation.

`deeplob-inference` subscribes to the selected NIFTY future book, maintains a
bounded queue, rejects stale snapshots, and logs `DEEPLOB_PAPER_PREDICTION`.
It maps a qualified `UP` observation to paper action `BUY_CE` and `DOWN` to
`BUY_PE`; otherwise it emits `NO_TRADE`. CE and PE books are not recorded or
used as model inputs. The service has no paper-broker or live-broker dependency,
so it cannot place an order.

## Safe migration order

1. Leave the current Railway/Lightsail trading services running.
2. Start only `deeplob-recorder` on EC2 and verify seven sessions with zero
   dropped rows before considering the EC2 host stable.
3. Keep collecting while training and replay happen offline.
4. Run `deeplob-inference` as a separate paper-only EC2 service.
5. Migrate an existing trading service only after a controlled shadow run.

This avoids combining infrastructure migration, model validation, and live
order risk in one deployment.
