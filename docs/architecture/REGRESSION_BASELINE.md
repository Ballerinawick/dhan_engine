# Regression Baseline

## Date and scope

Run on 2026-09-09 from the repository root on Windows. No broker credentials were supplied, no live service was started, and no orders were placed.

## Commands

1. `python -m pytest`
   - Result: blocked before test collection.
   - Cause: `python.exe` resolves to `C:\Users\dines\AppData\Local\Microsoft\WindowsApps\python.exe`, which reported that Python was not found and offered the Microsoft Store.

2. `py -3 -m pytest`
   - Result: completed successfully.
   - Environment: Python 3.11.9, pytest 9.1.1, pluggy 1.6.0, Windows.
   - Collection: 157 tests.
   - Passed: 155.
   - Skipped: 2.
   - Failed: 0.
   - Duration: 9.56 seconds.

## Test coverage observed

The run included direct tests for application guards, commodity and stock paper engines, DeepLOB recorder/runtime/option paper behavior, liquidity and market features, `LongOptionRegimeExecutor`, LTP execution, market-feed subscription lifecycle, master CSV resolution, percentage reversal, timed straddle, trade-summary persistence, virtual strategy books, and full-depth parsing/microstructure.

Important exact tests for this architecture audit include:

- `tests/test_virtual_strategy_books.py`: `test_strategy_books_start_with_executable_spread_cost`, `test_bullish_market_marks_directional_books_and_excursions`, `test_invalid_quotes_do_not_initialize_or_mutate_books`.
- `tests/test_long_option_regime.py`: `test_v1_bullish_state_retains_profit_then_independently_confirms_pe`, `test_pair_structure_reflects_selected_strikes_without_extra_contracts`, `test_v1_books_use_future_options_synthetic_straddle_and_depth`, `test_warm_hybrid_books_veto_fast_direction_when_executable_pnl_disagrees`, `test_warm_hybrid_books_allow_direction_when_both_layers_agree`, `test_state_entry_requires_observed_executable_cost_coverage`, `test_entry_requires_current_instant_state_to_match_stable_state`, `test_confirmed_opposite_v1_state_challenges_before_accepting_exit`, `test_fee_clearing_mfe_uses_pullback_then_recovery_without_fixed_trail`, `test_continued_state_and_price_decay_captures_positive_mfe_in_summary`, `test_earned_move_exits_after_state_defence_and_quote_confirmation`, `test_losing_premium_needs_state_deterioration_before_exit`, `test_invalid_and_older_quotes_cannot_change_position_or_history`, and `test_all_requested_market_timeframes_are_measured`.
- `tests/test_timed_straddle.py`: `test_four_leg_executable_prices_and_costs`, `test_calculates_debit_and_capped_max_profit`, `test_timeout_reason_is_duration_neutral`, `test_force_close_overrides_profit`, `test_five_minute_cycle_exits_on_positive_net_or_cycle_timeout`, `test_selects_four_legs_and_enters_only_when_max_profit_can_hit_target`, `test_contract_change_reconnects_stream`, `test_missing_quotes_trigger_recovery`, and `test_three_consecutive_losses_halt_new_cycles`.
- `tests/test_deeplob_foundation.py`: `test_recorder_runtime_persists_only_synchronized_fullquote_and_depth`, `test_recorder_row_contains_training_partitions_and_full_depth_schema`, `test_option_selection_failure_retries_without_stopping_recorder`, `test_composite_rejects_missing_or_stale_fullquote`, `test_option_paper_entry_uses_ask_and_exit_uses_bid`, `test_option_paper_blocks_stale_option_quote`, and `test_parallel_option_paper_profiles_are_isolated`.

These tests prove current fixed-book, fixed-pair, executor, recorder, and timed four-leg behavior. They do not prove generic multi-leg accounting, arbitrary strategy lifecycle, cross-strategy intelligence, mandatory V1-to-V2 lineage, complete multi-strike historical coverage, or deterministic generic replay.

## Baseline conclusion

The existing test suite is green under the repository's available Python launcher. The ordinary `python` command is environment-blocked, but the Windows `py -3` command provides a complete safe baseline. PR-01 changed documentation only, so no runtime regression is expected or introduced.