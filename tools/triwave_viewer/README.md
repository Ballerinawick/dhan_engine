# TriWave Viewer

Static replay UI for recorded TriWave sessions.

## Cloud usage

The running bot serves this viewer from the same Railway service when `TRIWAVE_VIEWER_ENABLED` is not disabled.

Open the Railway public service URL:

```text
https://<your-railway-domain>/
```

The viewer reads recorded sessions from:

```text
TRIWAVE_SESSION_BASE_DIR=data/triwave_sessions
```

For stable cloud history, attach a Railway volume and set:

```text
TRIWAVE_SESSION_BASE_DIR=/data/triwave_sessions
```

Optional private access:

```text
TRIWAVE_VIEWER_TOKEN=<secret>
```

Then open:

```text
https://<your-railway-domain>/?token=<secret>
```

## Local file usage

Open `index.html` in a browser and load:

- `ticks.jsonl`
- `trades.jsonl`
- `signals.jsonl` optional

The files are produced by `TriWaveSessionRecorder` under:

```text
data/triwave_sessions/YYYY-MM-DD/expiry=<key>/
```
