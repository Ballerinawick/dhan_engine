# Production service socket budget

Dhan documents a maximum of five live-market-feed WebSocket connections per user. Opening an additional connection can disconnect the oldest connection with error `805`.

The current Railway services share one Dhan client ID, so their connection count is an account-wide budget:

| Service | Live-feed sockets | Depth sockets |
| --- | ---: | ---: |
| `index` | 2 (future + option premium) | 1 |
| `stock` | 1 | 0 |
| `timed-straddle` | 1 | 0 |

This is the maximum intended deployment. Do not enable option quote sharding, duplicate replicas, commodity, or depth-research services under the same client ID while these three services are running.

Required production settings:

```text
OPTION_QUOTE_WS_SHARDS=1
TIMED_STRADDLE_MAX_CONSECUTIVE_LOSSES=3
TIMED_STRADDLE_DAILY_LOSS_LIMIT=1000
```

Premium recovery first sends Dhan's official Full unsubscribe (`22`) and subscribe (`21`) requests over the existing socket. A replacement client is allowed only after the existing client has sent disconnect (`12`) and its socket thread has stopped.

Before real orders, run each strategy as paper-only for multiple sessions and require:

- no `PREMIUM_STREAM_REBUILD_VERIFY_FAILED`
- no `FULLQUOTE_CLOSE_TIMEOUT`
- no disconnect code `805`
- positive net results after modeled fees and slippage
- risk halt proven in a losing session
