# Current Architecture

## Scope and evidence rule

This document describes the repository as implemented on 2026-09-09. File paths and symbols below are the evidence for each claim. The target generic multi-leg architecture is not treated as existing functionality.

## Runtime entry points

- `dhan_engine/interfaces/cli/run_deeplob_live.py:main` loads environment variables, refreshes the instrument master, starts the session viewer, and runs `build_deeplob_live_runtime(...).run()`.
- `dhan_engine/interfaces/cli/run_deeplob_inference.py` and `run_deeplob_recorder.py` provide inference-only and recorder-only variants.
- `dhan_engine/interfaces/cli/run_timed_straddle.py` starts the independent four-leg reverse-iron-fly experiment.
- `dhan_engine/interfaces/cli/run_stock_option_paper.py`, `run_stock_paper.py`, `run_commodity_paper.py`, `run_service.py`, and `run_ws.py` are separate stock, commodity, service, and websocket entry points.
- `build_deeplob_live_runtime` in `dhan_engine/application/deeplob/live_runtime.py` assembles the NIFTY DeepLOB runtime. It constructs `InstrumentMaster`, `ParquetDepthRecorder`, `FullDepth200Adapter`, `DhanLiveMarketFeedWS`, `OptionChainSelector`, inference workers, and isolated paper executors.

## Component and dependency map

```text
Dhan Full Quote websocket (marketfeed_ws.py)
  -> DeepLobLiveRuntime.on_fullquote
  -> latest future/option bid, ask, LTP, timestamps

Dhan 200-depth adapter (full_depth_200_adapter.py)
  -> DeepLobLiveRuntime.on_book
  -> recorder.record
  -> validate_composite_snapshot
  -> derive_market_by_price_features / LiquidityEventTracker
  -> inference.on_book
  -> prediction sink

OptionChainSelector.select_best("NIFTY")
  -> executor.register_contracts
  -> Full Quote subscription replacement (currently two option contracts)

LongOptionRegimeExecutor
  -> ExecutableStrategyLedger
  -> evidence and state classification
  -> PaperTradeManager
  -> StateDrivenPositionKeeper
  -> PaperTradeManager.on_exit and TradeSummaryS3Sink
```

`DeepLobLiveRuntime.on_book` records before rejecting an unsynchronized composite. Invalid composites do not reach inference or paper execution, while recorder behavior is intentionally more permissive for capture diagnostics.

## Market-data-to-V2 call graph

1. `DhanLiveMarketFeedWS` subscribes Full Quote request code 21 and dispatches `on_fullquote`; it deduplicates `(ExchangeSegment, SecurityId)` keys and chunks subscription messages at 100 instruments.
2. `FullDepth200Adapter` subscribes NIFTY futures depth and dispatches `on_book` with bid/ask depth arrays.
3. `DeepLobLiveRuntime.on_fullquote` stores the latest quote and forwards option quotes to each option executor's `on_quote`.
4. `DeepLobLiveRuntime.on_book` synchronizes the future depth snapshot with the latest Full Quote, validates freshness and spread, records it, derives market-by-price and liquidity evidence, and calls inference.
5. The inference runtime calls `LongOptionRegimeExecutor.on_prediction` through the configured prediction sink.
6. `LongOptionRegimeExecutor._on_prediction_locked` calls `_derive_evidence`, `_classify`, `_advance_state`, and `_log_v1`.
7. When there is no open position and state confirmations permit entry, it calls `_try_entry`.
8. `_try_entry` uses the selected option's current ask for entry, requires fresh bid/ask, positive selected V1 book movement, ready positive short and medium executable timeframes, and positive observed net movement after fee. It calls `PaperTradeManager.on_entry` with V1 metadata.
9. When a position exists, `_manage_open_position` uses the position's current executable bid and calls `StateDrivenPositionKeeper.observe` with the current evidence, stable state, instant state, and round-trip fee.
10. Keeper `HOLD`, `DEFEND`, or `EXIT` decisions are combined with `_catastrophic_guard_triggered`. `heartbeat` separately applies market-close exit.
11. `_exit` requires a positive executable bid, calls `PaperTradeManager.on_exit`, enriches the summary with keeper and latest V1 evidence, and writes it to `TradeSummaryS3Sink`.

## Existing V1 virtual books

`ExecutableStrategyLedger` in `dhan_engine/application/deeplob/virtual_strategy_books.py` has one `MarketMark` entry and creates exactly these hard-coded `VirtualBook` instances: `future_long`, `future_short`, `long_ce`, `long_pe`, `synthetic_long`, `synthetic_short`, `long_straddle`, and `short_straddle`.

It enters long option books at ask and marks them at bid. Futures use LTP as a proxy. Synthetic short books are calculations only; no short broker or paper position is opened. `VirtualBook.mark` tracks lifetime P&L, P&L percentage, MFE percentage, MAE percentage, and update count. `recent_changes` calculates recent executable movement from an earlier mark, separate from lifetime P&L. The fixed ledger does not implement a general short cash-flow ledger, aggregate synchronized strategy trajectory, or separate liquidation-value accounting.

This is an executable-price-aware evidence ledger for one selected CE/PE pair, not a generic multi-leg strategy model. It has no leg identity, strategy identity, lifecycle state, quantity, expiry per leg, independent leg entry/exit events, or multiple independent strategies.

## V1 evidence and state classification

`LongOptionRegimeExecutor._derive_evidence` in `long_option_regime.py` requires fresh CE, PE, and future observations. It derives trajectory, velocity, acceleration, pressure, long-volatility, executable-book, and multi-timeframe evidence. `_derive_v1_books` combines fast trajectory direction with lifetime executable books and `recent_changes` when the hybrid book is warm.

`_classify` produces `BULLISH_EXPANSION`, `BEARISH_EXPANSION`, exhaustion states, volatility expansion/contraction, or `UNCERTAIN`. `_advance_state` applies confirmation and reversal-confirmation counts. The current state is one executor-wide state, not a cross-strategy intelligence object.

## Existing V2 entry, holding, and exit

`LongOptionRegimeExecutor._try_entry` selects only CE or PE based on the stable directional state and calls the paper trader once. Entry metadata includes `v1_entry_state`, score, CE/PE changes, long volatility, velocity and acceleration spread, V1 books, timeframe state, future LTP, pressure, strike, expiry, and the basis `RECENT_EXECUTABLE_MOVE_NOT_FORECAST`.

The V2 position's own economics are maintained by `PaperTradeManager`: long entry at ask, LTP tick marking, exit at the executor-provided bid, quantity from one NIFTY lot, fees, gross/net realized P&L, and cash. This current manager is long-only, so it does not implement short-entry bid, short-exit ask, or short-premium cash-flow accounting. The executor does not copy all V1 books into V2 positions.

`StateDrivenPositionKeeper` tracks one `secid`, executable P&L excursions, best/worst observed P&L, support history, price history, phases, and MFE capture. It uses V1 evidence and state alignment for holding decisions, while the V2 position's bid-based P&L controls price deterioration and quote-confirmed exits. The executor additionally applies catastrophic-loss confirmation and market-close exit. Entry uses selected-side V1 book movement, short/medium executable timeframe direction, observed net movement after fees, state score, and fresh option bid/ask. Holding and exit use continuing evidence, stable/instant state, keeper support/timeframe values, current V2 bid economics, MFE/MAE excursions, and catastrophic/market-close safeguards. Entry metadata and later exit summaries are descriptive fields, not an immutable lineage contract.

## Portfolio isolation and constraints

`build_deeplob_live_runtime` creates a distinct `PaperTradeManager` for each configured executor/profile. Parallel profiles therefore have separate cash, positions, realized P&L, and counters.

Within one `PaperTradeManager`, `on_entry` first scale-ins an existing same-security position, then blocks any new entry while `has_open_position()` is true. It is long-only, full-lot exit, and keyed by security ID. Therefore it supports one open position globally, with same-security scale-in, not multiple independent positions or multi-leg portfolios.

## Reverse-iron-fly experiment

`OptionChainSelector.select_atm_reverse_iron_fly` in `dhan_engine/infrastructure/dhan/option_chain_selector.py` fetches one nearest expiry option-chain response, computes ATM from `last_price` and `strike_step_map`, chooses an upper and lower strike using `wing_steps`, and resolves four exact security IDs through `InstrumentMaster.find_option_security_id`: long ATM CE, long ATM PE, short upper CE, and short lower PE. It also returns wing width, expiry, underlying LTP, and ATM lot size from the master.

This selector is consumed by `TimedStraddleRuntime._select_pair` in `dhan_engine/application/experiments/timed_straddle.py`, which subscribes exactly those four contracts. `TimedStraddleBook` has four-leg executable accounting: long legs enter at ask and exit at bid; short legs enter at bid and exit at ask; it calculates per-leg P&L, combined gross/net P&L, debit, max profit, MFE-like max/min net values, and lifecycle close reasons.

It is a single timed experiment, with one `position` and cycle/risk state in `TimedStraddleRuntime`. It is not connected to `LongOptionRegimeExecutor`, does not provide generic strategy lifecycle, does not provide cross-strategy aggregation, and does not establish V1-to-V2 provenance. Selection, accounting, and lifecycle are separate concerns. PR-04 may reuse its exact four-leg selection shape and executable pricing tests, but must not silently reuse its fixed timed lifecycle as the generic V1 engine.

## Persistence, recording, and replay

`ParquetDepthRecorder` in `dhan_engine/analytics/deeplob_recorder.py` asynchronously stores sampled or every-book NIFTY 200-depth rows, synchronized Full Quote fields when available, timestamps, instrument metadata, partitions, and optional S3 uploads. `TradeSummaryS3Sink` stores paper trade summaries separately. `PostMarketAnalysisRuntime` reads canonical S3 Parquet for reports.

`TriWaveSessionRecorder` and `TriWaveReplayAnalyzer` are separate JSONL recording/replay tools for the older TriWave path. There is no repository implementation that replays the generic multi-leg V1 model deterministically from historical multi-strike quote data.

## Existing tests and coverage

Direct virtual-ledger behavior is covered by `tests/test_virtual_strategy_books.py`: `test_strategy_books_start_with_executable_spread_cost`, `test_bullish_market_marks_directional_books_and_excursions`, and `test_invalid_quotes_do_not_initialize_or_mutate_books`.

Direct regime behavior is covered by `tests/test_long_option_regime.py`, including `test_v1_bullish_state_retains_profit_then_independently_confirms_pe`, `test_pair_structure_reflects_selected_strikes_without_extra_contracts`, `test_v1_books_use_future_options_synthetic_straddle_and_depth`, `test_warm_hybrid_books_veto_fast_direction_when_executable_pnl_disagrees`, `test_warm_hybrid_books_allow_direction_when_both_layers_agree`, entry quote/cost and state confirmation tests, keeper challenge/recovery tests, catastrophic and quote-confirmed exit tests, invalid quote rejection, and `test_all_requested_market_timeframes_are_measured`.

The exact direct keeper behavior is exercised through `tests/test_long_option_regime.py` because the keeper is called by the executor; there is no standalone `StateDrivenPositionKeeper` test module.

The four-leg experiment is directly covered by `tests/test_timed_straddle.py`: `test_four_leg_executable_prices_and_costs`, `test_calculates_debit_and_capped_max_profit`, `test_timeout_reason_is_duration_neutral`, `test_force_close_overrides_profit`, `test_five_minute_cycle_exits_on_positive_net_or_cycle_timeout`, and the runtime selection/subscription/risk tests in `TimedStraddleRuntimeTests`.

Recorder, composite validation, option execution, subscription lifecycle, and runtime wiring are directly tested in `tests/test_deeplob_foundation.py`, `tests/test_marketfeed_subscription_lifecycle.py`, `tests/test_ltp_execution_path.py`, and `tests/test_run_deeplob_live.py`. These tests do not prove generic multi-leg, cross-strategy, lineage, or deterministic replay behavior.

## Known limitations

- Normal DeepLOB subscriptions are one NIFTY future plus the selected CE/PE pair; selector capability is broader than actual runtime coverage.
- The V1 ledger is fixed-shape and pair-scoped.
- V1 book lifetime P&L and recent executable movement are present, but generic leg and strategy lifecycle are absent. The current fixed books do not define gross realized P&L, gross unrealized P&L, fees, and liquidation value as separate generic accounting quantities.
- The V2 portfolio is isolated per executor but globally single-position within each manager.
- Provenance is metadata fields, not an enforced lineage contract; entry evidence and later holding/exit evidence are not separately immutable identity records.
- Historical Parquet capture does not imply complete option-chain or multi-strike history.
- External Dhan limits and semantics not encoded in this repository remain unverified.