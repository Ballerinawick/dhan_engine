# Institutional Options Depth Engine Audit Report

## DATA FLOW MAP

### Stage 1
- **Stage:** Websocket binary packet reception
- **Class:** `FullDepth`
- **Method:** `get_instrument_data()`
- **Input:** `self.ws.recv()` bytes or text frame from Dhan depth websocket.
- **Output:** raw JSON dict (text frame) or parsed packet dict list emitted one-by-one.
- **Transform vs pass-through:** **Transform** for binary frames (delegates parsing), pass-through for JSON frames.
- **Status:** **OK** (connected and yielding downstream updates).

### Stage 2
- **Stage:** Binary packet parsing
- **Class:** `FullDepth`
- **Method:** `_parse_binary_message()` -> `_parse_packet()`
- **Input:** binary frame bytes.
- **Output:** per-packet dict with `msg_code`, `exchange_segment`, `security_id`, `levels`.
- **Transform vs pass-through:** **Transform** (binary decode into structured dict).
- **Status:** **OK**.

### Stage 3
- **Stage:** Depth update pairing (bid + ask)
- **Class:** `DhanAsyncDepthAdapter`
- **Method:** `_process_update()`
- **Input:** parsed dict update with `msg_code`, `security_id`, and levels.
- **Output:** paired `DepthSide` bid/ask objects for a `secid`.
- **Transform vs pass-through:** **Transform** (normalization + caching latest bid/ask and pairing).
- **Status:** **OK, but PARTIAL quality gate** (pairs are emitted on every packet once both sides exist; no dedup/filter).

### Stage 4
- **Stage:** Adapter callback invocation
- **Class:** `DhanAsyncDepthAdapter`
- **Method:** `_process_update()`
- **Input:** paired `DepthSide` bid/ask.
- **Output:** callback call `on_depth(secid, tag, bid_side, ask_side)`.
- **Transform vs pass-through:** **Pass-through** (only tag lookup + callback dispatch).
- **Status:** **OK**.

### Stage 5
- **Stage:** Feature builder invocation
- **Class:** `DepthMicroFeatureBuilder`
- **Method:** `build()` (called inside `on_opt_depth` in `run_ws_9_instruments.py`)
- **Input:** `(secid, bid_side, ask_side)`.
- **Output:** micro feature dict: `ltp`, `bid_qty`, `ask_qty`, `imbalance_5`, `flow`, `vacuum_flag`, `absorption_flag`, `absorption_strength`, `spread`, `ts`.
- **Transform vs pass-through:** **Transform**.
- **Status:** **OK**.

### Stage 6
- **Stage:** Momentum engine invocation
- **Class:** `OptionsMomentumEngine`
- **Method:** `on_tick()` -> `_build_1s_candle()` -> `_evaluate()`
- **Input:** feature dict from stage 5.
- **Output:** action string (`NO_TRADE`, `TURN_ENTRY`, `EXIT`, etc).
- **Transform vs pass-through:** **Transform** (candle aggregation + micro gating + turn logic).
- **Status:** **OK**.

### Stage 7
- **Stage:** Decision layer evaluation
- **Class:** `InstitutionalDecisionEngine`
- **Method:** `on_signal()`
- **Input:** `(secid, tag, ltp, signal, momentum_engine, paper_trader)` from `on_opt_depth`.
- **Output:** entry/exit permission dict (`entry_allowed`, `exit_allowed`) and state updates.
- **Transform vs pass-through:** **Transform** (structure/cooldown/shadow/index-lock logic).
- **Status:** **OK** for signal gating; **PARTIAL** for micro-feature awareness (only receives signal + ltp, not full micro context).

---

## FEATURE PIPELINE STATUS

Target features inspected:
- `imbalance_5`
- `flow`
- `vacuum_flag`
- `absorption_flag`
- `absorption_strength`

### Where produced
All five are produced in `DepthMicroFeatureBuilder.build()`.

### Where consumed
- **Consumed in momentum layer:** `OptionsMomentumEngine._micro_ok()` and `_evaluate()` use all five as part of entry filter logic.
- **Not consumed in decision layer:** `InstitutionalDecisionEngine.on_signal()` receives only `signal` and `ltp`; no `raw`/feature payload.

### Pipeline stop point
The micro-feature payload stops at `run_ws_9_instruments.py:on_opt_depth()` when only `action` (signal string) and `ltp` are forwarded to `decision_engine.on_signal()`.

### Class that must be modified
- Primary: `InstitutionalDecisionEngine` (`on_signal`) to accept micro feature context (or derived composite score).
- Integration caller change: `run_ws_9_instruments.py:on_opt_depth()` to pass the features into decision layer.

---

## CALLBACK ISSUE LOCATION

### Observation
Repeated logs like `DEPTH_CALLBACK | secid=... | ltp=...` are consistent with callback-per-packet behavior.

### Root cause
`DhanAsyncDepthAdapter._process_update()` calls `self.on_depth(...)` every time a new 41/51 packet arrives **after both sides have at least one cached snapshot**. This means:
- If only bid updates are arriving, callback still fires each packet using stale ask cache.
- No check exists for unchanged best prices/qty/mid.

### Exact location
- Callback generation: `dhan_async_depth_adapter.py`, method `_process_update()`.
- Log emission showing duplicates: `run_ws_9_instruments.py`, function `on_opt_depth()` at `print(f"📡 DEPTH_CALLBACK ...")`.

### How to filter duplicates
Recommended filter point: `DhanAsyncDepthAdapter._process_update()` before invoking `on_depth`.
Suggested key per secid:
- `(bid.prices[0], bid.qty[0], ask.prices[0], ask.qty[0])` for top-of-book change filtering, or
- hash of top-5 ladder for stricter dedup.

Emit callback only when key changes.

---

## TRADE TRIGGER LOGIC

### Why `signal=NO_TRADE` while `struct_ok=True`
`struct_ok` is from decision layer and only tells you structure compression state; it does **not** force entries.
Most NO_TRADE events originate upstream in momentum engine.

### Primary blockers in momentum path
1. **Warm readiness / candle availability**
   - `_evaluate()` exits until at least 8 candles are available.
2. **Per-second single-action throttle**
   - `last_action_sec` blocks repeated action in same second.
3. **Micro gate failures (`_micro_ok`)**
   - spread too wide (`MICRO_MAX_SPREAD_PCT`)
   - weak directional evidence (`MICRO_MIN_ABS_IMB`)
   - weak confirm evidence (`MICRO_MIN_FLOW` and `MICRO_MIN_ABSORB`)
   - `vacuum_flag=True`
4. **Turn pattern conditions in `TF_CONFIRM` path**
   - requires prior down move, exhaustion, reversal confirm, and 3s timeframe confirmation.
5. **Fee-risk block**
   - entry blocked if `expected_move <= spread * 1.2`.

### TURN_CHECK / TF_CONFIRM / speed_ratio / avg_range_5 interpretation
- `TURN_CHECK` computes `speed_ratio = abs(speed) / avg_range_5` and logs local reversal context.
- `TF_CONFIRM` requires both 1s turn pattern and 3s candle direction confirmation.
- Low `speed_ratio` or high `avg_range_5` relative to speed can fail exhaustion/turn criteria.

### Which class controls thresholds
- `OptionsMomentumEngine` class constants control main trading sensitivity and gating.

### Where tuning should happen
- In `OptionsMomentumEngine` constants and `_evaluate()`/`_micro_ok()` logic:
  - `TURN_SPEED_RATIO_THRESHOLD`
  - `MICRO_MIN_ABS_IMB`
  - `MICRO_MIN_FLOW`
  - `MICRO_MIN_ABSORB`
  - `MICRO_MAX_SPREAD_PCT`
  - fee-risk multiplier in `expected_move <= spread * 1.2`

---

## UPGRADE LOCATION (10× SENSITIVITY)

### Where imbalance should be computed
- Already computed in `DepthMicroFeatureBuilder.build()` as `imbalance_5` and `flow`.
- For sensitivity upgrade, add **short-horizon spike score** there (e.g., z-score/EMA deviation of imbalance & flow per secid).

### Which class should detect imbalance spikes
- `OptionsMomentumEngine` should detect actionable spikes because it already combines micro + candle context.
- Integration point: `_evaluate()` immediately after `last_tick` extraction and before `_micro_ok` hard reject.

### Where decision layer should consume it
- Extend `InstitutionalDecisionEngine.on_signal()` signature to receive either:
  - `micro_context` (raw fields + spike score), or
  - condensed `signal_meta` (e.g., `imbalance_spike=True`, `spike_strength`).

### Exact integration points
1. **Feature compute extension**
   - File: `depth_micro_features.py`
   - Class: `DepthMicroFeatureBuilder`
   - Method: `build()`
   - Add: rolling baseline + `imbalance_spike`, `flow_spike`, `ofi_score`.
2. **Trigger detection**
   - File: `options_momentum_engine.py`
   - Class: `OptionsMomentumEngine`
   - Method: `_evaluate()`
   - Add: early “micro impulse entry candidate” branch using spike signals + reduced turn lag.
3. **Decision consumption**
   - File: `institutional_decision_engine.py`
   - Class: `InstitutionalDecisionEngine`
   - Method: `on_signal()`
   - Add: optional micro-context gates for index lock exceptions / faster confirmations.
4. **Pipeline wiring**
   - File: `run_ws_9_instruments.py`
   - Function: `on_opt_depth()`
   - Pass raw features (or derived meta) into decision call.

---

## NEXT CLASS TO INSPECT

- **File:** `options_momentum_engine.py`
- **Class:** `OptionsMomentumEngine`
- **Reason (critical control point):**
  It is the primary choke point where live depth-derived features are converted into executable intent (`TURN_ENTRY`/`NO_TRADE`/`EXIT`). It controls almost all sensitivity via micro filters, turn logic, timeframe confirmation, and fee-risk gating. Any 10× sensitivity upgrade will succeed or fail primarily based on this class’s gating design.
