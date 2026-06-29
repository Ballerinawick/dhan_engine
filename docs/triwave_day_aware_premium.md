# TriWave Day-Aware Premium Guard

TriWave V2 now has a lightweight expiry-cycle profile for the weekly Wednesday-to-Tuesday option cycle. It does not replace the existing entry, hold, or exit logic. It adds one day-aware gate before entry and one day-aware safety guard before the existing exit checks.

## Runtime Flags

The defaults are production-on:

```env
TRIWAVE_DAY_AWARE_PREMIUM=1
TRIWAVE_DAY_AWARE_ENTRY_FILTER=1
TRIWAVE_DAY_AWARE_EXIT_GUARD=1
```

To disable the full layer without changing code:

```env
TRIWAVE_DAY_AWARE_PREMIUM=0
```

## Current Cycle Map

- Wednesday: Day 1, more room for premium recovery.
- Thursday: Day 2, moderate room.
- Friday: Day 3, tighter adverse handling.
- Monday: Day 4, theta pressure is higher.
- Tuesday: Day 5, expiry-day behavior is strictest.

Weekend profiles are proxies only for dry-runs and do not affect a live NSE trading day.

## Lightsail Token Rotation

For the current Lightsail deployment, rotate only the Dhan token and restart the service:

```bash
cd /home/ubuntu/dhan_engine
nano .env
sudo systemctl restart dhan-engine
sudo journalctl -u dhan-engine -f
```

Confirm startup with:

```text
MODE: FUTURE_WS_STREAM + OPTION_WS_STREAM + OPTION_DEPTH_STREAM
PAIR_STATE_READY
FEED_HEALTH
```

Do not commit `.env`, `.env.*`, access tokens, or virtual environments.
