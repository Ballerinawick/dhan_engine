from __future__ import annotations

import logging
import os
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, time as clock_time
from typing import Mapping
from zoneinfo import ZoneInfo

from dhan_engine.application.deeplob.virtual_strategy_books import (
    ExecutableStrategyLedger,
    MarketMark,
)
from dhan_engine.domain.market.expiry_cycle import expiry_cycle_context

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _clock(value: str) -> clock_time:
    return datetime.strptime(value, "%H:%M").time()


@dataclass(frozen=True)
class LongOptionRegimeSettings:
    enabled: bool
    capital: float
    max_quote_age_sec: float
    observation_sec: float
    minimum_samples: int
    state_confirmations: int
    reversal_confirmations: int
    minimum_state_score: float
    fee_buffer_multiple: float
    round_trip_fee: float
    catastrophic_loss_pct: float
    catastrophic_confirmations: int
    market_start: clock_time
    entry_cutoff: clock_time
    market_end: clock_time
    hybrid_enabled: bool = True
    hybrid_book_weight: float = 0.55
    hybrid_min_updates: int = 4

    @classmethod
    def from_env(cls) -> "LongOptionRegimeSettings":
        prefix = "DEEPLOB_REGIME_V2_"
        return cls(
            enabled=_env_bool(prefix + "ENABLED", "1"),
            capital=float(os.getenv(prefix + "CAPITAL", "500000")),
            max_quote_age_sec=float(os.getenv(prefix + "MAX_QUOTE_AGE_SEC", "2")),
            observation_sec=max(3.0, float(os.getenv(prefix + "OBSERVATION_SEC", "12"))),
            minimum_samples=max(4, int(os.getenv(prefix + "MIN_SAMPLES", "8"))),
            state_confirmations=max(2, int(os.getenv(prefix + "STATE_CONFIRMATIONS", "3"))),
            reversal_confirmations=max(2, int(os.getenv(prefix + "REVERSAL_CONFIRMATIONS", "2"))),
            minimum_state_score=float(os.getenv(prefix + "MIN_STATE_SCORE", "0.58")),
            fee_buffer_multiple=float(os.getenv(prefix + "FEE_BUFFER_MULTIPLE", "1.25")),
            round_trip_fee=float(os.getenv(prefix + "ROUND_TRIP_FEE", "60")),
            catastrophic_loss_pct=max(
                1.0, float(os.getenv(prefix + "CATASTROPHIC_LOSS_PCT", "8.0"))
            ),
            catastrophic_confirmations=max(
                2, int(os.getenv(prefix + "CATASTROPHIC_CONFIRMATIONS", "3"))
            ),
            market_start=_clock(os.getenv(prefix + "MARKET_START", "09:15")),
            entry_cutoff=_clock(os.getenv(prefix + "ENTRY_CUTOFF", "15:24")),
            market_end=_clock(os.getenv(prefix + "MARKET_END", "15:25")),
            hybrid_enabled=_env_bool(prefix + "HYBRID_ENABLED", "1"),
            hybrid_book_weight=max(
                0.0, min(1.0, float(os.getenv(prefix + "HYBRID_BOOK_WEIGHT", "0.55")))
            ),
            hybrid_min_updates=max(
                2, int(os.getenv(prefix + "HYBRID_MIN_UPDATES", "4"))
            ),
        )


class LongOptionRegimeExecutor:
    """V1 virtual long-option regime book feeding one isolated V2 portfolio."""

    profile = "regime_v2"
    strategy = "deeplob_long_option_regime_v2"

    def __init__(
        self,
        settings,
        paper_trader,
        *,
        trade_summary_sink=None,
        live_order_canary=None,
    ):
        self.settings = settings
        self.paper_trader = paper_trader
        self.trade_summary_sink = trade_summary_sink
        self.live_order_canary = live_order_canary
        self.contracts: dict[str, dict] = {}
        self.quotes: dict[int, dict] = {}
        self.history = {"CE": deque(maxlen=512), "PE": deque(maxlen=512)}
        self.future_history = deque(maxlen=512)
        self._timezone = ZoneInfo("Asia/Kolkata")
        self._state = "UNCERTAIN"
        self._instant_state = "UNCERTAIN"
        self._candidate = "UNCERTAIN"
        self._candidate_count = 0
        self._catastrophic_side = ""
        self._catastrophic_count = 0
        self._entry_rearm_state = ""
        self._expiry_cycle: dict = {}
        self._entries = 0
        self._exits = 0
        self._blocks = 0
        self._last_log_mono = 0.0
        self._last_hold_log_mono = 0.0
        self._last_evidence: dict = {}
        self._last_v1_books: dict = {}
        self._v1_ledger = ExecutableStrategyLedger()

    def register_contracts(self, selection: Mapping[str, Mapping]) -> list[dict]:
        subscriptions = []
        selected = set()
        for side in ("CE", "PE"):
            contract = dict(selection.get(side) or {})
            secid = int(contract.get("security_id", 0) or 0)
            if not secid:
                continue
            contract.update(security_id=secid, tag=f"NIFTY_{side}")
            self.contracts[side] = contract
            selected.add(secid)
            subscriptions.append(
                {"ExchangeSegment": "NSE_FNO", "SecurityId": str(secid), "tag": contract["tag"]}
            )
        self.quotes = {key: value for key, value in self.quotes.items() if key in selected}
        for values in self.history.values():
            values.clear()
        self.future_history.clear()
        self._last_v1_books = {}
        self._v1_ledger.reset()
        self._refresh_expiry_cycle()
        if self.live_order_canary is not None:
            self.live_order_canary.register_contracts(self.contracts)
        logger.info(
            "DEEPLOB_V1_CONTRACTS | ce_id=%s | ce_strike=%s | pe_id=%s | pe_strike=%s | "
            "pair_structure=%s | cycle=%s | premium_regime=%s | extra_subscriptions=0",
            self.contracts.get("CE", {}).get("security_id"),
            self.contracts.get("CE", {}).get("strike"),
            self.contracts.get("PE", {}).get("security_id"),
            self.contracts.get("PE", {}).get("strike"),
            self._pair_structure(),
            self._expiry_cycle.get("cycle_label", "UNKNOWN"),
            self._expiry_cycle.get("premium_regime", "UNKNOWN"),
        )
        return subscriptions

    def on_quote(self, secid, tag, ltp, *, bid, ask, received_ts) -> None:
        secid = int(secid)
        side = next(
            (name for name, item in self.contracts.items() if int(item["security_id"]) == secid),
            None,
        )
        if side is None:
            return
        quote = {
            "tag": tag,
            "ltp": float(ltp),
            "bid": float(bid or 0.0),
            "ask": float(ask or 0.0),
            "received_ts": float(received_ts),
        }
        self.quotes[secid] = quote
        self.history[side].append(
            (float(received_ts), float(ltp), float(bid or 0.0), float(ask or 0.0))
        )
        self.paper_trader.on_tick(secid, float(ltp))
        self._prune(side, float(received_ts))

    def on_prediction(
        self,
        *,
        paper_action,
        confidence,
        composite,
        probability_down,
        probability_flat,
        probability_up,
        model_version,
        horizon_sec,
        signal_metadata=None,
    ) -> None:
        if self.live_order_canary is not None:
            self.live_order_canary.heartbeat()
        if not self.settings.enabled or composite is None:
            return
        evidence = self._derive_evidence(composite, paper_action, confidence, signal_metadata)
        if evidence is None:
            return
        self._last_evidence = evidence
        proposed = self._classify(evidence)
        self._instant_state = self._normalize_state(proposed)
        self._advance_state(proposed)
        self._log_v1(evidence, proposed)

        if self.paper_trader.has_open_position():
            self._manage_open_position(evidence, self._instant_state)
            return
        now = datetime.now(self._timezone).time()
        if not (self.settings.market_start <= now < self.settings.entry_cutoff):
            return
        if self._state not in {"BULLISH_EXPANSION", "BEARISH_EXPANSION"}:
            return
        if self._instant_state != self._state:
            return
        if self._candidate_count < self.settings.state_confirmations:
            return
        if self._entry_rearm_state == self._state:
            return
        self._try_entry("CE" if self._state == "BULLISH_EXPANSION" else "PE", evidence)

    def heartbeat(self) -> None:
        if self.live_order_canary is not None:
            self.live_order_canary.heartbeat()
        if not self.paper_trader.has_open_position():
            return
        if datetime.now(self._timezone).time() >= self.settings.market_end:
            self._exit("DEEPLOB_V2_EXIT:MARKET_CLOSE")

    def health(self) -> dict:
        return {
            "profile": self.profile,
            "strategy": self.strategy,
            "enabled": self.settings.enabled,
            "v1_state": self._state,
            "v1_instant_state": self._instant_state,
            "state_confirmations": self._candidate_count,
            "entry_rearm_state": self._entry_rearm_state or None,
            "expiry_cycle": dict(self._expiry_cycle),
            "samples": {side: len(values) for side, values in self.history.items()},
            "future_samples": len(self.future_history),
            "v1_books": dict(self._last_v1_books),
            "open_positions": len(self.paper_trader.positions),
            "entries": self._entries,
            "exits": self._exits,
            "blocks": self._blocks,
        }

    def _prune(self, side: str, now_ts: float) -> None:
        cutoff = now_ts - max(60.0, self.settings.observation_sec * 4.0)
        values = self.history[side]
        while values and values[0][0] < cutoff:
            values.popleft()

    def _prune_future(self, now_ts: float) -> None:
        cutoff = now_ts - max(60.0, self.settings.observation_sec * 4.0)
        while self.future_history and self.future_history[0][0] < cutoff:
            self.future_history.popleft()

    def _leg_metrics(self, side: str) -> dict | None:
        values = self.history[side]
        if len(values) < self.settings.minimum_samples:
            return None
        now_ts, last, last_bid, last_ask = values[-1]
        cutoff = now_ts - self.settings.observation_sec
        window = [item for item in values if item[0] >= cutoff]
        if len(window) < self.settings.minimum_samples or window[0][1] <= 0:
            return None
        elapsed = max(window[-1][0] - window[0][0], 0.001)
        change_pct = (window[-1][1] / window[0][1] - 1.0) * 100.0
        midpoint = max(1, len(window) // 2)
        first = window[:midpoint]
        second = window[midpoint:]
        first_elapsed = max(first[-1][0] - first[0][0], 0.001)
        second_elapsed = max(second[-1][0] - second[0][0], 0.001)
        first_velocity = (first[-1][1] / first[0][1] - 1.0) * 100.0 / first_elapsed
        second_velocity = (second[-1][1] / second[0][1] - 1.0) * 100.0 / second_elapsed
        return {
            "ltp": last,
            "bid": last_bid,
            "ask": last_ask,
            "change_pct": change_pct,
            "executable_change_pct": (
                (last_bid / window[0][3] - 1.0) * 100.0
                if last_bid > 0 and window[0][3] > 0
                else change_pct
            ),
            "velocity_pct_sec": change_pct / elapsed,
            "acceleration": second_velocity - first_velocity,
            "range_pct": (
                max(item[1] for item in window) - min(item[1] for item in window)
            ) / window[0][1] * 100.0,
            "window_start_bid": window[0][2],
            "window_start_ask": window[0][3],
            "received_ts": now_ts,
        }

    def _future_metrics(self, composite) -> dict | None:
        full_quote = composite.full_quote or {}
        future_ltp = float(full_quote.get("ltp", 0.0) or 0.0)
        received_ts = float(full_quote.get("received_ts", time.time()) or time.time())
        if future_ltp <= 0:
            return None
        if not self.future_history or (
            received_ts > self.future_history[-1][0]
            or future_ltp != self.future_history[-1][1]
        ):
            self.future_history.append((received_ts, future_ltp))
        self._prune_future(received_ts)
        cutoff = received_ts - self.settings.observation_sec
        window = [item for item in self.future_history if item[0] >= cutoff]
        if len(window) < self.settings.minimum_samples or window[0][1] <= 0:
            return None
        elapsed = max(window[-1][0] - window[0][0], 0.001)
        change_pct = (window[-1][1] / window[0][1] - 1.0) * 100.0
        range_pct = (
            max(value for _, value in window) - min(value for _, value in window)
        ) / window[0][1] * 100.0
        return {
            "ltp": future_ltp,
            "change_pct": change_pct,
            "short_change_pct": -change_pct,
            "velocity_pct_sec": change_pct / elapsed,
            "range_pct": range_pct,
        }

    @staticmethod
    def _signed_strength(value: float, observed_range: float) -> float:
        scale = max(abs(observed_range), abs(value), 0.0001)
        return max(-1.0, min(1.0, value / scale))

    def _derive_v1_books(self, ce: dict, pe: dict, future: dict, pressure: float) -> dict:
        ledger = self._v1_ledger.update(
            MarketMark(
                received_ts=max(float(ce["received_ts"]), float(pe["received_ts"])),
                future_ltp=float(future["ltp"]),
                ce_bid=float(ce["bid"] or 0.0),
                ce_ask=float(ce["ask"] or 0.0),
                pe_bid=float(pe["bid"] or 0.0),
                pe_ask=float(pe["ask"] or 0.0),
            )
        )
        executable = ledger.get("books", {})

        def book_pct(name: str, fallback: float = 0.0) -> float:
            return float(executable.get(name, {}).get("pnl_pct", fallback) or 0.0)

        future_long_pct = book_pct("future_long", future["change_pct"])
        future_short_pct = book_pct("future_short", future["short_change_pct"])
        long_ce_pct = book_pct("long_ce", ce["executable_change_pct"])
        long_pe_pct = book_pct("long_pe", pe["executable_change_pct"])
        synthetic_long_pct = book_pct("synthetic_long")
        synthetic_short_pct = book_pct("synthetic_short")
        long_straddle_pct = book_pct("long_straddle")
        short_straddle_pct = book_pct("short_straddle")
        option_range = max(ce["range_pct"] + pe["range_pct"], 0.0001)
        future_strength = self._signed_strength(
            future["change_pct"], future["range_pct"]
        )
        option_strength = self._signed_strength(
            ce["executable_change_pct"] - pe["executable_change_pct"],
            option_range,
        )
        synthetic_strength = self._signed_strength(
            synthetic_long_pct - synthetic_short_pct,
            option_range * 2.0,
        )
        pressure_strength = max(-1.0, min(1.0, pressure))
        fast_direction_score = (
            future_strength + option_strength + synthetic_strength + pressure_strength
        ) / 4.0
        fast_bull_support = sum(
            value > 0.0
            for value in (
                future["change_pct"],
                ce["executable_change_pct"] - pe["executable_change_pct"],
                synthetic_long_pct - synthetic_short_pct,
                pressure,
            )
        )
        fast_bear_support = sum(
            value < 0.0
            for value in (
                future["change_pct"],
                ce["executable_change_pct"] - pe["executable_change_pct"],
                synthetic_long_pct - synthetic_short_pct,
                pressure,
            )
        )
        executable_components = (
            self._signed_strength(
                future_long_pct - future_short_pct,
                max(future["range_pct"] * 2.0, 0.0001),
            ),
            self._signed_strength(long_ce_pct - long_pe_pct, option_range),
            self._signed_strength(
                synthetic_long_pct - synthetic_short_pct,
                option_range * 2.0,
            ),
        )
        executable_direction_score = sum(executable_components) / len(
            executable_components
        )
        executable_bull_support = sum(value > 0.0 for value in executable_components)
        executable_bear_support = sum(value < 0.0 for value in executable_components)
        hybrid_ready = bool(
            self.settings.hybrid_enabled
            and ledger.get("ready")
            and int(ledger.get("updates", 0)) >= self.settings.hybrid_min_updates
        )
        hybrid_agreement = bool(
            fast_direction_score * executable_direction_score > 0.0
        )
        if hybrid_ready:
            weight = self.settings.hybrid_book_weight
            direction_score = (
                fast_direction_score * (1.0 - weight)
                + executable_direction_score * weight
            )
        else:
            direction_score = fast_direction_score
        return {
            "future_long_pct": future_long_pct,
            "future_short_pct": future_short_pct,
            "long_ce_pct": long_ce_pct,
            "long_pe_pct": long_pe_pct,
            "synthetic_long_pct": synthetic_long_pct,
            "synthetic_short_pct": synthetic_short_pct,
            "long_straddle_pct": long_straddle_pct,
            "short_straddle_pct": short_straddle_pct,
            "fast_direction_score": fast_direction_score,
            "executable_direction_score": executable_direction_score,
            "hybrid_direction_score": direction_score,
            "hybrid_ready": hybrid_ready,
            "hybrid_agreement": hybrid_agreement,
            "book_updates": int(ledger.get("updates", 0)),
            "book_age_sec": float(ledger.get("age_sec", 0.0)),
            "direction_score": direction_score,
            "bull_support": fast_bull_support + executable_bull_support,
            "bear_support": fast_bear_support + executable_bear_support,
            "fast_bull_support": fast_bull_support,
            "fast_bear_support": fast_bear_support,
            "executable_bull_support": executable_bull_support,
            "executable_bear_support": executable_bear_support,
            "executable_books": ledger,
        }

    def _derive_evidence(self, composite, paper_action, confidence, metadata) -> dict | None:
        self._refresh_expiry_cycle()
        ce = self._leg_metrics("CE")
        pe = self._leg_metrics("PE")
        future = self._future_metrics(composite)
        if ce is None or pe is None or future is None:
            return None
        pressure = float(getattr(composite.features, "pressure_score", 0.0) or 0.0)
        v1_books = self._derive_v1_books(ce, pe, future, pressure)
        self._last_v1_books = dict(v1_books)
        future_ltp = future["ltp"]
        directional = ce["change_pct"] - pe["change_pct"]
        velocity = ce["velocity_pct_sec"] - pe["velocity_pct_sec"]
        acceleration = ce["acceleration"] - pe["acceleration"]
        long_vol = ce["change_pct"] + pe["change_pct"]
        scale = max(ce["range_pct"] + pe["range_pct"], 0.05)
        score = min(
            1.0,
            abs(v1_books["direction_score"]) * 0.70
            + min(1.0, abs(directional) / scale) * 0.30,
        )
        return {
            "ce": ce,
            "pe": pe,
            "directional_pct": directional,
            "velocity_spread": velocity,
            "acceleration_spread": acceleration,
            "long_vol_pct": long_vol,
            "pressure": pressure,
            "future_ltp": future_ltp,
            "future": future,
            "v1_books": v1_books,
            "model_action": str(paper_action),
            "model_confidence": float(confidence),
            "state_score": score,
            "pair_structure": self._pair_structure(),
            "signal_metadata": dict(metadata or {}),
            "expiry_cycle": dict(self._expiry_cycle),
        }

    def _classify(self, evidence: dict) -> str:
        ce = evidence["ce"]
        pe = evidence["pe"]
        books = evidence["v1_books"]
        score = evidence["state_score"]
        hybrid_direction_confirmed = not books.get("hybrid_ready", False) or bool(
            books.get("hybrid_agreement", False)
        )
        bullish = (
            books["bull_support"] >= 3
            and books["direction_score"] > 0
            and evidence["velocity_spread"] > 0
            and score >= self.settings.minimum_state_score
            and hybrid_direction_confirmed
        )
        bearish = (
            books["bear_support"] >= 3
            and books["direction_score"] < 0
            and evidence["velocity_spread"] < 0
            and score >= self.settings.minimum_state_score
            and hybrid_direction_confirmed
        )
        if bullish:
            return "REVERSAL_TO_BULLISH" if self._state in {"BEARISH_EXPANSION", "BEARISH_EXHAUSTION"} else "BULLISH_EXPANSION"
        if bearish:
            return "REVERSAL_TO_BEARISH" if self._state in {"BULLISH_EXPANSION", "BULLISH_EXHAUSTION"} else "BEARISH_EXPANSION"
        if self._state == "BULLISH_EXPANSION":
            exhausted = sum(
                value <= 0.0
                for value in (
                    books["future_long_pct"],
                    books["synthetic_long_pct"],
                    evidence["acceleration_spread"],
                    evidence["pressure"],
                )
            )
            if exhausted >= 3:
                return "BULLISH_EXHAUSTION"
        if self._state == "BEARISH_EXPANSION":
            exhausted = sum(
                value <= 0.0
                for value in (
                    books["future_short_pct"],
                    books["synthetic_short_pct"],
                    -evidence["acceleration_spread"],
                    -evidence["pressure"],
                )
            )
            if exhausted >= 3:
                return "BEARISH_EXHAUSTION"
        if books["long_straddle_pct"] > 0 and ce["change_pct"] > 0 and pe["change_pct"] > 0:
            return "VOLATILITY_EXPANSION"
        if books["long_straddle_pct"] <= 0 and abs(books["direction_score"]) < self.settings.minimum_state_score:
            return "VOLATILITY_CONTRACTION"
        return "UNCERTAIN"

    def _advance_state(self, proposed: str) -> None:
        normalized = self._normalize_state(proposed)
        if normalized == self._candidate:
            self._candidate_count += 1
        else:
            self._candidate = normalized
            self._candidate_count = 1
        required = self.settings.reversal_confirmations if proposed.startswith("REVERSAL_") else self.settings.state_confirmations
        if self._candidate_count >= required and normalized != self._state:
            previous = self._state
            self._state = normalized
            if self._entry_rearm_state and self._state != self._entry_rearm_state:
                self._entry_rearm_state = ""
            logger.info(
                "DEEPLOB_V1_REGIME_TRANSITION | previous=%s | current=%s | confirmations=%s | "
                "cycle=%s | premium_regime=%s",
                previous,
                self._state,
                self._candidate_count,
                self._expiry_cycle.get("cycle_label", "UNKNOWN"),
                self._expiry_cycle.get("premium_regime", "UNKNOWN"),
            )

    def _try_entry(self, side: str, evidence: dict) -> None:
        signal_ns = time.perf_counter_ns()
        contract = self.contracts.get(side)
        quote = self.quotes.get(int((contract or {}).get("security_id", 0) or 0))
        ask = float((quote or {}).get("ask", 0.0) or 0.0)
        bid = float((quote or {}).get("bid", 0.0) or 0.0)
        age = time.time() - float((quote or {}).get("received_ts", 0.0) or 0.0)
        if not contract or not quote or ask <= 0 or bid <= 0 or age > self.settings.max_quote_age_sec:
            self._blocks += 1
            logger.info("DEEPLOB_V2_ENTRY_BLOCKED | reason=OPTION_QUOTE_NOT_EXECUTABLE | side=%s", side)
            return
        lot_size = int(self.paper_trader.LOT_SIZES["NIFTY"])
        spread_cost = max(0.0, ask - bid) * lot_size
        metadata = {
            "strategy": self.strategy,
            "profile": self.profile,
            "paper_profile": self.profile,
            "strategy_owner": self.strategy,
            "v1_entry_state": self._state,
            "v1_state_score": evidence["state_score"],
            "v1_pair_structure": evidence["pair_structure"],
            "v1_ce_change_pct": evidence["ce"]["change_pct"],
            "v1_pe_change_pct": evidence["pe"]["change_pct"],
            "v1_long_vol_pct": evidence["long_vol_pct"],
            "v1_velocity_spread": evidence["velocity_spread"],
            "v1_acceleration_spread": evidence["acceleration_spread"],
            "v1_books": dict(evidence["v1_books"]),
            "future_ltp": evidence["future_ltp"],
            "pressure_score": evidence["pressure"],
            "entry_option_spread": ask - bid,
            "entry_spread_cost": spread_cost,
            "option_strike": contract.get("strike"),
            "option_expiry": contract.get("expiry"),
            **evidence.get("expiry_cycle", {}),
        }
        accepted = self.paper_trader.on_entry(
            int(contract["security_id"]), contract["tag"], "LONG", ask, lots=1,
            reason=f"DEEPLOB_V2_ENTRY:{self._state}", metadata=metadata,
        )
        if accepted:
            self._entries += 1
            self._catastrophic_side = ""
            self._catastrophic_count = 0
            if self.live_order_canary is not None:
                self.live_order_canary.submit_entry(
                    side=side,
                    state=self._state,
                    signal_ns=signal_ns,
                )
            logger.info(
                "DEEPLOB_V2_ENTRY | side=%s | state=%s | instant_state=%s | price=%.2f | "
                "score=%.3f | ce_pct=%+.3f | pe_pct=%+.3f | pressure=%+.3f | "
                "direction_score=%+.3f | spread_cost=%.2f | cycle=%s | premium_regime=%s",
                side, self._state, self._instant_state, ask, evidence["state_score"], evidence["ce"]["change_pct"],
                evidence["pe"]["change_pct"], evidence["pressure"],
                evidence["v1_books"]["direction_score"], spread_cost,
                self._expiry_cycle.get("cycle_label", "UNKNOWN"),
                self._expiry_cycle.get("premium_regime", "UNKNOWN"),
            )

    def _manage_open_position(self, evidence: dict, instant_state: str) -> None:
        secid, position = next(iter(self.paper_trader.positions.items()))
        side = "CE" if str(position.get("tag", "")).endswith("_CE") else "PE"
        supporting = (side == "CE" and self._state == "BULLISH_EXPANSION") or (
            side == "PE" and self._state == "BEARISH_EXPANSION"
        )
        opposite_state = "BEARISH_EXPANSION" if side == "CE" else "BULLISH_EXPANSION"
        if self._state == opposite_state:
            self._exit(f"DEEPLOB_V2_EXIT:V1_REVERSAL_TO_{'PE' if side == 'CE' else 'CE'}")
            return
        matching_exhaustion = (side == "CE" and self._state == "BULLISH_EXHAUSTION") or (
            side == "PE" and self._state == "BEARISH_EXHAUSTION"
        )
        if matching_exhaustion:
            self._exit(f"DEEPLOB_V2_EXIT:{self._state}")
            return
        if self._catastrophic_guard_triggered(side, position, evidence, instant_state):
            self._entry_rearm_state = self._state
            self._exit("DEEPLOB_V2_EXIT:DEPTH_CONFIRMED_CATASTROPHIC_GUARD")
            return
        now_mono = time.monotonic()
        if now_mono - self._last_hold_log_mono >= 5.0:
            self._last_hold_log_mono = now_mono
            logger.info(
                "DEEPLOB_V2_HOLD | side=%s | state=%s | instant_state=%s | score=%.3f | "
                "supporting=%s | direction_score=%+.3f | cycle=%s | premium_regime=%s",
                side,
                self._state,
                instant_state,
                evidence["state_score"],
                supporting,
                evidence.get("v1_books", {}).get("direction_score", 0.0),
                self._expiry_cycle.get("cycle_label", "UNKNOWN"),
                self._expiry_cycle.get("premium_regime", "UNKNOWN"),
            )

    def _catastrophic_guard_triggered(
        self, side: str, position: Mapping, evidence: dict, instant_state: str
    ) -> bool:
        secid = int(position.get("secid", 0) or 0)
        quote = self.quotes.get(secid, {})
        bid = float(quote.get("bid", 0.0) or 0.0)
        ask = float(quote.get("ask", 0.0) or 0.0)
        entry = float(position.get("entry", 0.0) or 0.0)
        if bid <= 0 or ask <= 0 or entry <= 0:
            self._reset_catastrophic_guard()
            return False

        loss_pct = max(0.0, (entry - bid) / entry * 100.0)
        entry_spread_pct = (
            float(position.get("entry_option_spread", 0.0) or 0.0) / entry * 100.0
        )
        current_spread_pct = max(0.0, ask - bid) / max((ask + bid) / 2.0, 0.01) * 100.0
        loss_floor = max(
            self.settings.catastrophic_loss_pct,
            entry_spread_pct * 4.0,
            current_spread_pct * 3.0,
        )
        leg = evidence[side.lower()]
        adverse = (
            side == "CE"
            and instant_state in {"BEARISH_EXPANSION", "BEARISH_EXHAUSTION"}
            and evidence["pressure"] < 0
            and evidence["velocity_spread"] < 0
            and leg["change_pct"] < 0
        ) or (
            side == "PE"
            and instant_state in {"BULLISH_EXPANSION", "BULLISH_EXHAUSTION"}
            and evidence["pressure"] > 0
            and evidence["velocity_spread"] > 0
            and leg["change_pct"] < 0
        )
        if loss_pct < loss_floor or not adverse:
            self._reset_catastrophic_guard()
            return False

        if self._catastrophic_side == side:
            self._catastrophic_count += 1
        else:
            self._catastrophic_side = side
            self._catastrophic_count = 1
        logger.warning(
            "DEEPLOB_V2_CATASTROPHIC_GUARD_PENDING | side=%s | loss_pct=%.3f | "
            "loss_floor_pct=%.3f | state=%s | instant_state=%s | pressure=%+.3f | "
            "confirmations=%s/%s",
            side,
            loss_pct,
            loss_floor,
            self._state,
            instant_state,
            evidence["pressure"],
            self._catastrophic_count,
            self.settings.catastrophic_confirmations,
        )
        return self._catastrophic_count >= self.settings.catastrophic_confirmations

    def _reset_catastrophic_guard(self) -> None:
        self._catastrophic_side = ""
        self._catastrophic_count = 0

    def _exit(self, reason: str) -> None:
        if not self.paper_trader.positions:
            return
        secid, position = next(iter(self.paper_trader.positions.items()))
        quote = self.quotes.get(int(secid), {})
        bid = float(quote.get("bid", 0.0) or 0.0)
        if bid <= 0:
            logger.warning("DEEPLOB_V2_EXIT_BLOCKED | reason=NO_EXECUTABLE_BID | secid=%s", secid)
            return
        self.paper_trader.on_exit(int(secid), bid, reason=reason)
        if self.live_order_canary is not None:
            if reason.endswith("MARKET_CLOSE") or "CATASTROPHIC" in reason:
                self.live_order_canary.request_exit(reason=reason)
            else:
                self.live_order_canary.notify_paper_exit(reason)
        summary = dict(self.paper_trader.last_trade_summary or {})
        if summary and self.trade_summary_sink is not None:
            summary.update({key: value for key, value in position.items() if key.startswith("v1_")})
            summary.update(
                {
                    key: position.get(key)
                    for key in (
                        "cycle_day",
                        "cycle_label",
                        "sessions_to_expiry",
                        "premium_regime",
                    )
                    if position.get(key) is not None
                }
            )
            summary.update(
                schema_version=2, index="NIFTY", runtime="deeplob_live_regime_v2",
                strategy=self.strategy, profile=self.profile, paper_profile=self.profile,
                paper=True, exit_execution_side="BID", v1_exit_state=self._state,
                v1_exit_instant_state=self._instant_state,
                v1_exit_score=self._last_evidence.get("state_score"),
            )
            self.trade_summary_sink.record(summary)
        self._exits += 1
        self._reset_catastrophic_guard()
        logger.info(
            "DEEPLOB_V2_EXIT | tag=%s | price=%.2f | state=%s | instant_state=%s | "
            "reason=%s | cycle=%s | premium_regime=%s | s3_profile=%s",
            position.get("tag"), bid, self._state, self._instant_state, reason,
            self._expiry_cycle.get("cycle_label", "UNKNOWN"),
            self._expiry_cycle.get("premium_regime", "UNKNOWN"), self.profile,
        )

    def _log_v1(self, evidence: dict, proposed: str) -> None:
        now = time.monotonic()
        if now - self._last_log_mono < 5.0:
            return
        self._last_log_mono = now
        logger.info(
            "DEEPLOB_V1_STATE | state=%s | instant_state=%s | proposed=%s | score=%.3f | "
            "call_pct=%+.3f | put_pct=%+.3f | "
            "long_%s_pct=%+.3f | velocity_spread=%+.4f | acceleration=%+.4f | "
            "future_ltp=%.2f | future_pct=%+.4f | synthetic_long=%+.3f | "
            "synthetic_short=%+.3f | straddle=%+.3f | direction_score=%+.3f | "
            "fast_direction=%+.3f | executable_direction=%+.3f | hybrid_ready=%s | "
            "hybrid_agreement=%s | book_updates=%s | "
            "bull_support=%s | bear_support=%s | pressure=%+.3f | confirmations=%s | cycle=%s | "
            "premium_regime=%s",
            self._state, self._instant_state, proposed, evidence["state_score"], evidence["ce"]["change_pct"],
            evidence["pe"]["change_pct"], evidence["pair_structure"].lower(),
            evidence["long_vol_pct"], evidence["velocity_spread"],
            evidence["acceleration_spread"], evidence["future_ltp"],
            evidence["v1_books"]["future_long_pct"],
            evidence["v1_books"]["synthetic_long_pct"],
            evidence["v1_books"]["synthetic_short_pct"],
            evidence["v1_books"]["long_straddle_pct"],
            evidence["v1_books"]["direction_score"],
            evidence["v1_books"].get("fast_direction_score", 0.0),
            evidence["v1_books"].get("executable_direction_score", 0.0),
            evidence["v1_books"].get("hybrid_ready", False),
            evidence["v1_books"].get("hybrid_agreement", False),
            evidence["v1_books"].get("book_updates", 0),
            evidence["v1_books"]["bull_support"],
            evidence["v1_books"]["bear_support"],
            evidence["pressure"], self._candidate_count,
            self._expiry_cycle.get("cycle_label", "UNKNOWN"),
            self._expiry_cycle.get("premium_regime", "UNKNOWN"),
        )

    @staticmethod
    def _normalize_state(state: str) -> str:
        return {
            "REVERSAL_TO_BULLISH": "BULLISH_EXPANSION",
            "REVERSAL_TO_BEARISH": "BEARISH_EXPANSION",
        }.get(state, state)

    def _refresh_expiry_cycle(self, trade_date=None) -> None:
        expiry = self.contracts.get("CE", {}).get("expiry") or self.contracts.get("PE", {}).get(
            "expiry"
        )
        if not expiry:
            self._expiry_cycle = {}
            return
        trading_day = trade_date or datetime.now(self._timezone).date()
        if self._expiry_cycle.get("trade_date") == str(trading_day):
            return
        try:
            self._expiry_cycle = expiry_cycle_context(trading_day, expiry).as_dict()
        except (TypeError, ValueError):
            self._expiry_cycle = {}
            logger.warning("DEEPLOB_V1_EXPIRY_CYCLE_UNAVAILABLE | expiry=%s", expiry)

    def _pair_structure(self) -> str:
        ce_strike = self.contracts.get("CE", {}).get("strike")
        pe_strike = self.contracts.get("PE", {}).get("strike")
        return "STRADDLE" if ce_strike is not None and ce_strike == pe_strike else "STRANGLE"
