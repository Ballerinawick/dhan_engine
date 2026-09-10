# Implementation Roadmap

All stages are opt-in and paper/research-only unless explicitly stated. Existing production and paper services remain the default path until PR-09 has passed shadow acceptance.

## PR-02: Generic multi-leg accounting

Scope: implement only a pure domain accounting foundation with synthetic deterministic fixtures. It covers arbitrary two-leg, four-leg, and more-leg positions; leg and strategy identities; lifecycle records; quantities; timestamps; executable marks; fees; lifetime P&L; recent executable movement; and leg/strategy MFE/MAE. Long entry/mark/exit uses ask/bid. Short entry/mark/exit uses bid/ask. Short premium received is recorded as entry cash flow, not realized profit. Gross realized P&L, gross unrealized P&L, fees, net P&L, and liquidation value remain separate quantities. Strategy aggregates use synchronized leg marks and strategy-level MFE/MAE comes from the combined P&L trajectory.

Reuse: `VirtualBook` mark conventions, `TimedStraddleBook` executable four-leg formulas, `LegQuote`, and `PaperTradeManager` fee evidence as reviewed source material only. No existing runtime component is changed or routed through the new foundation.

New modules: only a domain accounting module and focused deterministic tests, with concrete names selected after checking existing interfaces. No speculative public API is required by this roadmap.

Dependencies: current Python/runtime dependencies only.

Tests and acceptance: synthetic tests must cover long and short executable P&L; two-leg and four-leg aggregate accounting; quantity and fee consistency; credit received versus actual P&L; strategy-level MFE/MAE from synchronized aggregate marks; invalid, stale, and out-of-order marks; independent strategy isolation; lifecycle transitions; immutable historical records; lifetime versus recent movement; and no fabricated valuation. Existing tests must remain green.

Explicit out of scope: Dhan API integration, market-data subscriptions, option/strategy selection, simulation scheduling, cross-strategy intelligence, V2 execution, existing runtime replacement, deployment/configuration changes, broker-equivalent margin calculations, partial fills, live execution, and profitability claims.

Rollback: remove the unused accounting module and tests without changing existing imports or behavior.

## PR-03: V1 simulation engine

Scope: initialize predefined virtual strategies without directional signals, consume declared marks, maintain independent strategy sessions, and emit replayable leg/strategy observations. Each strategy must provide an explicit required-instrument manifest before initialization. The engine must define its synchronized snapshot policy, quote completeness rule, timestamp ordering policy, and missing-data policy for pending, blocked, invalid, and resumed states. Configured market start is only the initialization attempt time; active status requires every required leg and complete synchronized executable data.

Reuse: PR-02 accounting, `ExecutableStrategyLedger` evidence concepts, `TimedStraddleRuntime` lifecycle observations, Parquet schema inspection.

Proposed modules: V1 simulation orchestration and strategy-definition adapters, chosen only after interface review.

Dependencies: PR-02; explicit clock/session identity; required-instrument manifests; data-quality handling.

Tests: initialization without prediction, two simultaneous strategies, incomplete-manifest blocking, quote completeness, synchronized snapshot acceptance/rejection, deterministic mark ordering, pending/resume behavior, and aggregate snapshots.

Acceptance: V1 runs without calling V2 entry, does not create partial artificial P&L, records explicit missing-data states, and can replay a small complete fixture deterministically.

Rollback/out of scope: feature flag and isolated research entry point. Out of scope: production runtime replacement, cross-strategy intelligence, and broker execution.

## PR-04: Strategy coverage

Scope: cover NIFTY selected CE/PE, the existing four-leg reverse-iron-fly selection, and additional declared strategies only where required live or persisted data coverage is demonstrated. A selector's ability to return contract metadata is never sufficient evidence of historical quote coverage.

Reuse: `OptionChainSelector.select_best`, `select_atm_pair`, `select_atm_reverse_iron_fly`, `InstrumentMaster.find_option_security_id`, `TimedStraddleBook` tests.

Proposed modules: strategy definitions/adapters, not a replacement selector.

Dependencies: PR-02/03; required-instrument manifests; subscription and historical coverage decisions from the data matrix.

Tests: exact-leg selection, expiry/lot consistency, missing strike/quote handling, and four-leg executable accounting.

Acceptance: every advertised strategy declares its required legs, live subscription coverage, persisted historical coverage, quote fields, synchronization policy, and missing-data behavior. Strategies with unavailable required live or persisted coverage are blocked explicitly and cannot be silently simulated from selector output.

Rollback/out of scope: disable individual strategy definitions. Out of scope: V2 decisions and live order execution.

## PR-05: State intelligence

Scope: derive cross-strategy directional, volatility, momentum, decay, exhaustion, and agreement/disagreement evidence with freshness, source identity, and correlated-exposure controls.

Reuse: `_derive_evidence`, `_derive_v1_books`, `_classify`, `StateDrivenPositionKeeper` support concepts.

Proposed modules: read-only intelligence aggregation and evidence schemas.

Dependencies: PR-03 observations and PR-04 strategy identities.

Tests: source attribution, stale evidence, disagreement, duplicate/correlated exposure, and deterministic aggregation.

Acceptance: evidence is explainable and does not produce independent votes for duplicated exposure.

Rollback/out of scope: leave current executor evidence untouched and keep the new layer research-only. Out of scope: V2 portfolio integration.

## PR-06: V2 integration and lineage

Scope: add an opt-in V2 candidate decision boundary, isolated paper portfolio contract, selective entries, and mandatory V1 strategy/position/evidence provenance.

Reuse: `PaperTradeManager` accounting behavior, `_try_entry` safeguards, `TradeSummaryS3Sink` persistence.

Proposed modules: V2 adapter/lineage validation selected against existing executor interfaces.

Dependencies: PR-02 through PR-05.

Tests: selective non-copying decisions, isolated cash/positions, missing provenance rejection, persisted lineage, and unchanged existing runtime tests.

Acceptance: V2 can select fewer positions than V1, every trade is traceable, and the default runtime remains unchanged.

Rollback/out of scope: feature flag disables the adapter. Out of scope: holding/exit replacement and live execution.

## PR-07: Holding and exit integration

Scope: integrate continuing intelligence with V2 own executable economics while preserving keeper, market-close, catastrophic, stale-data, and no-bid safeguards.

Reuse: `StateDrivenPositionKeeper.observe`, `observe_quote`, `_catastrophic_guard_triggered`, `_exit`, and current guard tests.

Dependencies: PR-06 lineage and V2 position identity.

Tests: hold/defend/exit transitions, bid/ask economics, stale/no-bid blocking, market close, catastrophic confirmation, and regression tests.

Acceptance: state evidence cannot fabricate economics; all existing safeguards remain effective.

Rollback/out of scope: retain current executor as fallback. Out of scope: broker orders and strategy profitability claims.

## PR-08: Research validation

Scope: deterministic replay fixtures, historical coverage reports, sensitivity checks, attribution, and comparison of V1 evidence to V2 decisions.

Reuse: Parquet recorder, S3 reader, post-market analysis, TriWave replay patterns where applicable.

Proposed modules: generic replay/validation reporting only after persisted schema is defined.

Dependencies: PR-02 through PR-07 and verified data coverage.

Tests: replay determinism, fixture checksums, missing-data behavior, and report reproducibility.

Acceptance: results are reproducible and clearly separate observed behavior from unsupported profitability claims.

Rollback/out of scope: research tooling only. Out of scope: enabling any live or shadow production path.

## PR-09: Shadow deployment

Scope: run the new V1/intelligence/V2 paper path beside existing services in non-authoritative shadow mode, with monitoring and rollback.

Reuse: existing deployment templates, health logging, S3 sinks, and feature-flag conventions.

Proposed modules/configuration: shadow orchestration and observability, only after deployment review.

Dependencies: all prior PRs, operational budget, verified data subscriptions, and rollback runbook.

Tests: deployment smoke tests without credentials/orders, duplicate-subscription checks, resource limits, and shadow-vs-baseline comparisons.

Acceptance: no live orders, no change to authoritative runtime decisions, bounded resource/storage use, and one-step disablement.

Rollback/out of scope: disable shadow service and preserve existing deployment. Out of scope: making the new architecture authoritative or claiming profitability.