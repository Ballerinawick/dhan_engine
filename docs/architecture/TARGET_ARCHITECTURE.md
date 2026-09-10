# Target Architecture Contract

## Purpose

This contract defines the staged destination for NIFTY research and paper execution. It does not authorize live broker execution, replace the existing runtime, or make profitability claims.

## Boundaries

### V1 generic multi-leg simulation laboratory

V1 initializes predefined strategies at the configured simulation start without waiting for a directional entry signal. A strategy may contain two, four, or more legs. Each strategy runs independently and records strategy-level and leg-level observations.

V1 owns simulated strategy lifecycle, executable marks, accounting, MFE/MAE, and observed behavior. It does not decide which V2 trade to place.

### Cross-strategy state intelligence

This layer consumes observed V1 strategy and leg behavior. It derives directional, volatility, momentum, decay, exhaustion, and agreement/disagreement evidence. It must identify correlated or duplicated exposure so multiple strategies do not produce falsely independent evidence.

It publishes evidence and provenance references, not orders.

### V2 selective paper execution

V2 decides selectively from evidence and must not blindly copy all V1 positions. V2 has its own isolated capital, positions, accounting, executable entry/exit economics, and risk. Every V2 trade records the V1 strategy/position evidence that justified it.

V2 holding and exit use continuing state evidence together with the V2 position's own executable economics.

## Required position model

The model must represent:

- `strategy_id`, strategy definition/version, simulation session, lifecycle, and timestamps;
- one or more independently identified legs;
- per leg: instrument/security identity, index, expiry, strike, option type, buy/sell side, quantity/lots, entry executable price, current executable mark, timestamps/freshness, lifecycle, realized/unrealized P&L, MFE, MAE, and data-quality status;
- strategy aggregate quantities, debit/credit, cash/margin usage, realized/unrealized P&L, fees, MFE/MAE, and lifecycle;
- immutable event or snapshot identity sufficient for replay and lineage.

Long and short marks must use the correct executable side: long entry at ask and exit/mark at bid; short entry at bid and exit/mark at ask. Missing or stale executable prices must produce a blocked/unknown valuation state, not a fabricated mark.

## Accounting contract

- A long leg enters at executable ask. Its liquidation mark or exit uses executable bid.
- A short leg enters at executable bid. Its liquidation mark or exit uses executable ask.
- The short entry premium is a cash flow received at entry. It is not realized profit. Profit or loss is determined only from the complete entry/mark/exit economics, including the later cost of liquidation.
- Gross realized P&L is the sum of closed-leg price P&L before fees. Gross unrealized P&L is the current executable liquidation value of open legs minus their entry cash flows, before fees. Fees are recorded separately. Net P&L is gross realized P&L plus gross unrealized P&L minus applicable fees. Liquidation value is the executable value obtainable by closing currently open legs and is reported separately from P&L.
- Strategy aggregate P&L is derived from its legs using consistent quantities, executable sides, and synchronized timestamps. It is not calculated by mixing marks from different times or by adding unrelated strategy values.
- Strategy-level MFE and MAE are calculated from the combined strategy P&L trajectory at synchronized aggregate marks. They are not the sum of individual leg MFE or MAE values. Leg-level excursions remain separately available.
- Lifetime P&L is measured from the strategy or leg initialization/entry baseline. Recent executable movement is a windowed change from a defined recent anchor. Neither measure silently substitutes for the other.
- Invalid, stale, missing, or out-of-order marks do not update executable valuation. The affected leg or aggregate becomes blocked/unknown or remains at its last valid observation with an explicit data-quality state; no synthetic price is fabricated.
- This contract does not define broker-equivalent margin, collateral, liquidation policy, or exchange risk calculations. Any future cash or margin field must be clearly labeled as a simulation accounting quantity unless externally verified.

## Strategy initialization and lifecycle

Predefined strategies must attempt initialization at the configured market start without waiting for a directional entry signal. Market start is the initialization attempt time, not a guarantee of executable entry. A strategy becomes active only when all required legs have valid, synchronized executable quotes and complete contract metadata. Incomplete data produces a blocked or pending state, not partial artificial P&L. Partial initialization or partial fills are out of scope unless explicitly modeled by a later PR. The lifecycle must explicitly represent at least planned, pending, initialized, active, partially closed, closed, blocked, and invalid-data states. A blocked data state is distinct from a directional no-trade decision.

## State evidence contract

Evidence must identify its source session, timestamp, freshness window, input strategy/leg identities, and calculation version. It must distinguish lifetime behavior from recent executable movement and retain component evidence for direction, volatility, momentum, decay, exhaustion, and agreement/disagreement. Aggregation must expose correlation and double-counting controls rather than summing repeated exposure as independent votes.

## V1-to-V2 lineage

Each V2 entry and exit must carry explicit immutable provenance. Required identity categories are the V1 session/strategy identity, relevant V1 position and leg identities, the entry evidence identity and timestamp/freshness, the holding/exit evidence identity and timestamp/freshness, the decision reason, and the architecture/calculation versions. These are identity categories, not a premature implementation API. Entry provenance and subsequent holding/exit evidence must be separately identifiable. Original entry provenance is append-only and must never be overwritten by later state snapshots. The complete lineage must remain reconstructable and queryable after the V1 positions close.

## Data and replay requirements

- Contract selection must be separated from strategy construction and accounting.
- Strategies requiring multiple strikes must declare an explicit required-instrument manifest before simulation. A synchronized snapshot is complete only when every required instrument has a valid quote for the same accepted timestamp policy.
- The simulator must define a missing-data policy for pending, blocked, invalid, and resumed states; it must not silently carry partial strategy P&L as if all legs were marked.
- The simulator must refuse or explicitly mark incomplete coverage; it must not infer unobserved option quotes from a selector response.
- Replays must consume timestamped persisted inputs, use deterministic ordering and clock injection, and produce reproducible strategy/evidence/trade outputs.
- Historical coverage, quote freshness, bid/ask availability, depth availability, and subscription gaps must be recorded as data-quality facts.

## Risk and operational invariants

- Existing market-close, catastrophic-loss, data-quality, no-executable-price, stale-market, and configured paper-risk safeguards remain effective.
- V1 is virtual and cannot place broker orders.
- V2 remains paper-only through the migration stages.
- V1 and V2 capital, positions, accounting, and risk are isolated.
- Multiple strategies must not silently share mutable positions or cash.
- The existing production and paper services remain runnable and behaviorally unchanged until an explicitly approved integration stage.
- Changes are incremental, testable, replayable, observable, and reversible.

## Explicit non-goals

- No live broker execution.
- No unvalidated profitability or performance claims.
- No silent replacement of the existing runtime.
- No changes to existing trading decisions, subscriptions, deployment behavior, or live/paper services in PR-01.