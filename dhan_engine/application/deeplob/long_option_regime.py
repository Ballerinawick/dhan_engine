from __future__ import annotations

import logging
import os
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, time as clock_time
from typing import Mapping
from zoneinfo import ZoneInfo

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
        )


class LongOptionRegimeExecutor:
    """V1 virtual long-option regime book feeding one isolated V2 portfolio."""

    profile = "regime_v2"
    strategy = "deeplob_long_option_regime_v2"

    def __init__(self, settings, paper_trader, *, trade_summary_sink=None):
        self.settings = settings
        self.paper_trader = paper_trader
        self.trade_summary_sink = trade_summary_sink
        self.contracts: dict[str, dict] = {}
        self.quotes: dict[int, dict] = {}
        self.history = {"CE": deque(maxlen=512), "PE": deque(maxlen=512)}
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
        self._refresh_expiry_cycle()
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
        self.history[side].append((float(received_ts), float(ltp)))
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

    def _leg_metrics(self, side: str) -> dict | None:
        values = self.history[side]
        if len(values) < self.settings.minimum_samples:
            return None
        now_ts, last = values[-1]
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
            "change_pct": change_pct,
            "velocity_pct_sec": change_pct / elapsed,
            "acceleration": second_velocity - first_velocity,
            "range_pct": (max(v for _, v in window) - min(v for _, v in window)) / window[0][1] * 100.0,
        }

    def _derive_evidence(self, composite, paper_action, confidence, metadata) -> dict | None:
        self._refresh_expiry_cycle()
        ce = self._leg_metrics("CE")
        pe = self._leg_metrics("PE")
        if ce is None or pe is None:
            return None
        pressure = float(getattr(composite.features, "pressure_score", 0.0) or 0.0)
        future_ltp = float((composite.full_quote or {}).get("ltp", 0.0) or 0.0)
        directional = ce["change_pct"] - pe["change_pct"]
        velocity = ce["velocity_pct_sec"] - pe["velocity_pct_sec"]
        acceleration = ce["acceleration"] - pe["acceleration"]
        long_vol = ce["change_pct"] + pe["change_pct"]
        scale = max(ce["range_pct"] + pe["range_pct"], 0.05)
        depth_direction = 1.0 if pressure > 0 else -1.0 if pressure < 0 else 0.0
        model_direction = 1.0 if paper_action == "BUY_CE" else -1.0 if paper_action == "BUY_PE" else 0.0
        direction = 1.0 if directional > 0 else -1.0 if directional < 0 else 0.0
        alignment = (direction == depth_direction) + (direction == model_direction)
        score = min(1.0, abs(directional) / scale * 0.55 + abs(pressure) * 1.5 + alignment * 0.12)
        return {
            "ce": ce,
            "pe": pe,
            "directional_pct": directional,
            "velocity_spread": velocity,
            "acceleration_spread": acceleration,
            "long_vol_pct": long_vol,
            "pressure": pressure,
            "future_ltp": future_ltp,
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
        score = evidence["state_score"]
        if score < self.settings.minimum_state_score:
            if ce["change_pct"] < 0 and pe["change_pct"] < 0:
                return "VOLATILITY_CONTRACTION"
            return "UNCERTAIN"
        bullish = (
            ce["change_pct"] > 0
            and pe["change_pct"] < 0
            and evidence["velocity_spread"] > 0
            and evidence["pressure"] >= 0
        )
        bearish = (
            pe["change_pct"] > 0
            and ce["change_pct"] < 0
            and evidence["velocity_spread"] < 0
            and evidence["pressure"] <= 0
        )
        if bullish:
            return "REVERSAL_TO_BULLISH" if self._state in {"BEARISH_EXPANSION", "BEARISH_EXHAUSTION"} else "BULLISH_EXPANSION"
        if bearish:
            return "REVERSAL_TO_BEARISH" if self._state in {"BULLISH_EXPANSION", "BULLISH_EXHAUSTION"} else "BEARISH_EXPANSION"
        if ce["change_pct"] > 0 and pe["change_pct"] > 0:
            return "VOLATILITY_EXPANSION"
        if self._state == "BULLISH_EXPANSION" and evidence["acceleration_spread"] < 0:
            return "BULLISH_EXHAUSTION"
        if self._state == "BEARISH_EXPANSION" and evidence["acceleration_spread"] > 0:
            return "BEARISH_EXHAUSTION"
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
        contract = self.contracts.get(side)
        quote = self.quotes.get(int((contract or {}).get("security_id", 0) or 0))
        ask = float((quote or {}).get("ask", 0.0) or 0.0)
        bid = float((quote or {}).get("bid", 0.0) or 0.0)
        age = time.time() - float((quote or {}).get("received_ts", 0.0) or 0.0)
        if not contract or not quote or ask <= 0 or bid <= 0 or age > self.settings.max_quote_age_sec:
            self._blocks += 1
            logger.info("DEEPLOB_V2_ENTRY_BLOCKED | reason=OPTION_QUOTE_NOT_EXECUTABLE | side=%s", side)
            return
        leg = evidence[side.lower()]
        lot_size = int(self.paper_trader.LOT_SIZES["NIFTY"])
        observed_move = max(0.0, leg["change_pct"] / 100.0 * ask)
        expected_gross = (observed_move - max(0.0, ask - bid)) * lot_size
        required = self.settings.round_trip_fee * self.settings.fee_buffer_multiple
        if expected_gross < required:
            self._blocks += 1
            logger.info(
                "DEEPLOB_V2_ENTRY_BLOCKED | reason=V1_EXECUTABLE_EDGE_BELOW_COST | side=%s | "
                "state=%s | expected_gross=%.2f | required_gross=%.2f | score=%.3f",
                side, self._state, expected_gross, required, evidence["state_score"],
            )
            return
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
            "future_ltp": evidence["future_ltp"],
            "pressure_score": evidence["pressure"],
            "entry_option_spread": ask - bid,
            "expected_gross": expected_gross,
            "required_gross": required,
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
            logger.info(
                "DEEPLOB_V2_ENTRY | side=%s | state=%s | instant_state=%s | price=%.2f | "
                "score=%.3f | ce_pct=%+.3f | pe_pct=%+.3f | pressure=%+.3f | "
                "expected_gross=%.2f | cycle=%s | premium_regime=%s",
                side, self._state, self._instant_state, ask, evidence["state_score"], evidence["ce"]["change_pct"],
                evidence["pe"]["change_pct"], evidence["pressure"], expected_gross,
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
        if not supporting and self._state in {
            "VOLATILITY_CONTRACTION",
            "BULLISH_EXHAUSTION",
            "BEARISH_EXHAUSTION",
        }:
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
                "cycle=%s | premium_regime=%s",
                side,
                self._state,
                instant_state,
                evidence["state_score"],
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
            "future_ltp=%.2f | pressure=%+.3f | confirmations=%s | cycle=%s | "
            "premium_regime=%s",
            self._state, self._instant_state, proposed, evidence["state_score"], evidence["ce"]["change_pct"],
            evidence["pe"]["change_pct"], evidence["pair_structure"].lower(),
            evidence["long_vol_pct"], evidence["velocity_spread"],
            evidence["acceleration_spread"], evidence["future_ltp"],
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
