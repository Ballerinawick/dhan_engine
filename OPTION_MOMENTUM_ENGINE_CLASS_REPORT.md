# OptionsMomentumEngine Class Report

## 1) Class Identity
- **File:** `options_momentum_engine.py`
- **Class:** `OptionsMomentumEngine`
- **Purpose:** Convert high-frequency depth-derived ticks into normalized 1-second and 3-second structures, apply microstructure quality gates, and emit actionable trade signals (`TURN_ENTRY`, `EXIT`, `NO_TRADE`).

---

## 2) Inputs, Outputs, and Position in Flow

### Input contract (`on_tick(secid, tick)`)
Expected keys in `tick` include:
- `ltp`, `ts`
- `bid_qty`, `ask_qty`
- `imbalance_5`, `flow`, `vacuum_flag`
- `absorption_flag`, `absorption_strength`
- `spread`

### Output contract
`on_tick()` returns one of:
- `NO_TRADE`
- `TURN_ENTRY`
- `EXIT`

### Upstream/Downstream position
- Upstream producer: depth callback + feature builder (`DepthMicroFeatureBuilder`).
- Downstream consumer: institutional decision layer (`InstitutionalDecisionEngine.on_signal`).

---

## 3) Configuration & Sensitivity Knobs

Primary constants controlling sensitivity:
- `MICRO_MAX_SPREAD_PCT = 0.015`
- `MICRO_MIN_ABS_IMB = 0.08`
- `MICRO_MIN_ABSORB = 0.12`
- `MICRO_MIN_FLOW = 800`
- `TURN_SPEED_RATIO_THRESHOLD = 1.2`

Session/time gating:
- Trading window: `09:10` to `15:30` IST.
- Time regime classification: OPEN / MID / TREND / CLOSE.

These constants currently define the effective strictness of entry readiness.

---

## 4) Internal State Map

The class maintains a rich per-instrument state:
- **Tick-level:** `tick_buffer`
- **1s bars:** `candles`
- **3s bars:** `candles_3s`, `last_3s_bucket`
- **Trade state:** `active_trade`, `last_action_sec`, `last_exit_reason`
- **PnL stats:** `last_trade_pnl`, `cum_pnl`, `entries_taken`, `exits_taken`, `total_hold_sec`
- **Micro diagnostics:** `micro_stats_window`, reject trackers
- **Warmup baselines:** `warmup_stats`, `warmup_start_sec`, `warmup_reported`

This makes the class both signal generator and telemetry hub.

---

## 5) Processing Pipeline (Method-Level)

### A. `on_tick()`
1. Rejects invalid ticks (`ts` missing or `ltp <= 0`).
2. Rejects ticks outside market hours.
3. Appends into per-second tick buffer.
4. Builds 1-second candle (`_build_1s_candle`).
5. Prevents repeated candle processing in same second (`last_candle_sec`).
6. Builds rolling 3-second candle blocks.
7. Calls `_evaluate()` for trade decision.

### B. `_build_1s_candle()`
- Constructs OHLC from buffered ticks.
- Constructs synthetic activity volume from:
  - top-book depth change (`bid_qty + ask_qty` deltas), plus
  - absolute `flow` sum.

### C. `_evaluate()`
Core decision method:
1. Requires minimum candle history (`len(candles) >= 8`).
2. Applies one-action-per-second guard (`last_action_sec`).
3. Computes turn metrics: `speed`, `prev_speed`, `speed_ratio`, `avg_range_5`.
4. Logs `TURN_CHECK` diagnostics.
5. Runs `_micro_ok()` gate.
6. If in trade, evaluates opposite-turn exit logic.
7. If flat, checks entry pattern:
   - prior down move
   - down exhaustion (`speed_ratio`, range shrink, speed collapse)
   - reversal confirmation (`close > midpoint_prev`)
   - 3-second confirmation (`tf3_ok`)
8. Applies fee-risk block (`expected_move <= spread * 1.2`).
9. Emits `TURN_ENTRY` else `NO_TRADE`.

### D. `_micro_ok()`
Applies microstructure quality filter:
- spread sanity (`spread_pct <= MICRO_MAX_SPREAD_PCT`)
- directional evidence (`abs(imbalance_5)` or absorption)
- confirmation evidence (`abs(flow)` or absorption strength)
- vacuum veto (`vacuum_flag`)
- tracks pass/fail reason counters every 30s.

---

## 6) Why You See Frequent `NO_TRADE`

Most common structural blockers:
1. **Startup latency:** waits for 8 one-second candles.
2. **One-action lock:** no second action in same second for a secid.
3. **Micro gate strictness:** spread/flow/imbalance/absorption constraints.
4. **Turn confirmation stack:** requires multi-condition 1s + 3s confirmation.
5. **Fee-risk guard:** expected move must exceed spread-weighted threshold.

So `NO_TRADE` is usually produced in momentum engine before institutional decision layer is asked to approve entry.

---

## 7) Structural Weaknesses Observed

1. **Potential key mismatch for spread fallback**
   - `_micro_ok()` uses `bid_price`/`ask_price`, but `_evaluate()` spread fallback uses `bid`/`ask`.
   - If upstream tick only has one naming convention, spread fallback can silently degrade.

2. **Duplicate micro computation**
   - `_evaluate()` computes `micro_ok` variables and then calls `_micro_ok()` separately.
   - This adds drift risk between debug variables and authoritative gate.

3. **Hard thresholds across all instruments/regimes**
   - Single constants may underfit instrument-specific liquidity regimes.

4. **Entry path is turn-pattern dominant**
   - Microstructure can veto entries, but there is no standalone micro-impulse entry path.

---

## 8) Sensitivity Upgrade Targets (10× Objective)

### Best insertion point
`_evaluate()` immediately after `last_tick` extraction and before final micro rejection.

### Recommended additions
1. **Imbalance/flow spike detector (short horizon)**
   - Add rolling z-score or EMA-deviation for `imbalance_5` and `flow`.
2. **Micro impulse trigger branch**
   - Allow early entry candidate when spike + spread_ok + non-vacuum + minimal trend alignment.
3. **Adaptive thresholds**
   - Use warmup percentiles to set instrument-specific `MICRO_MIN_FLOW` / `MICRO_MIN_ABS_IMB`.
4. **Decision-layer metadata pass-through**
   - Send spike strength and trigger reason downstream for risk-aware gating.

This keeps instruments CSV-driven while increasing sensitivity without hardcoding SecurityIds.

---

## 9) Operational Logging You Should Monitor Live

High-value logs for diagnosis:
- `TURN_CHECK` for speed/avg_range dynamics.
- `TF_CONFIRM` for 1s+3s alignment.
- `MICRO_STATS` for pass/fail distribution by reason.
- `ENTRY_BLOCK_FEE_RISK` for spread-cost suppression.
- `ENTRY_ALLOWED` and `TURN_ENTRY` for successful trigger path.

---

## 10) Bottom Line

`OptionsMomentumEngine` is the principal trade-intent control plane in the current architecture. If sensitivity is low, this class is where most suppression happens and where the most leverage exists for controlled upgrades.
