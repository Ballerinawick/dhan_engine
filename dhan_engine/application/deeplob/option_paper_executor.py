from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, time as market_time
from typing import Mapping
from zoneinfo import ZoneInfo

from dhan_engine.domain.market.market_by_price_execution import CompositeMarketSnapshot

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeepLobOptionPaperSettings:
    enabled: bool
    capital: float
    confidence_threshold: float
    pressure_threshold: float
    confirmation_count: int
    entry_cooldown_sec: float
    max_quote_age_sec: float
    take_profit_pct: float
    stop_loss_pct: float
    max_hold_sec: float
    enforce_market_hours: bool
    market_start: market_time
    entry_cutoff: market_time
    market_end: market_time

    @classmethod
    def from_env(
        cls,
        prefix: str = "DEEPLOB_OPTION_PAPER",
        *,
        defaults: Mapping[str, str] | None = None,
    ) -> "DeepLobOptionPaperSettings":
        values = {
            "ENABLED": "1",
            "CAPITAL": "500000",
            "CONFIDENCE": "0.65",
            "PRESSURE": "0.05",
            "CONFIRMATIONS": "3",
            "COOLDOWN_SEC": "60",
            "MAX_QUOTE_AGE_SEC": "2",
            "TAKE_PROFIT_PCT": "2.0",
            "STOP_LOSS_PCT": "1.5",
            "MAX_HOLD_SEC": "600",
            "ENFORCE_MARKET_HOURS": "1",
            "MARKET_START": "09:15",
            "ENTRY_CUTOFF": "15:25",
            "MARKET_END": "15:30",
        }
        values.update(dict(defaults or {}))

        def read(name: str) -> str:
            return os.getenv(f"{prefix}_{name}", values[name]).strip()

        def parse_clock(name: str) -> market_time:
            return datetime.strptime(read(name), "%H:%M").time()

        return cls(
            enabled=read("ENABLED").lower() in {"1", "true", "yes", "on"},
            capital=max(1.0, float(read("CAPITAL"))),
            confidence_threshold=max(
                0.0,
                min(1.0, float(read("CONFIDENCE"))),
            ),
            pressure_threshold=max(
                0.0,
                min(1.0, float(read("PRESSURE"))),
            ),
            confirmation_count=max(1, int(read("CONFIRMATIONS"))),
            entry_cooldown_sec=max(0.0, float(read("COOLDOWN_SEC"))),
            max_quote_age_sec=max(0.1, float(read("MAX_QUOTE_AGE_SEC"))),
            take_profit_pct=max(0.01, float(read("TAKE_PROFIT_PCT"))),
            stop_loss_pct=max(0.01, float(read("STOP_LOSS_PCT"))),
            max_hold_sec=max(1.0, float(read("MAX_HOLD_SEC"))),
            enforce_market_hours=read("ENFORCE_MARKET_HOURS").lower()
            in {"1", "true", "yes", "on"},
            market_start=parse_clock("MARKET_START"),
            entry_cutoff=parse_clock("ENTRY_CUTOFF"),
            market_end=parse_clock("MARKET_END"),
        )


class DeepLobOptionPaperExecutor:
    """Paper-only CE/PE execution driven by NIFTY futures evidence."""

    def __init__(
        self,
        settings: DeepLobOptionPaperSettings,
        paper_trader,
        *,
        trade_summary_sink=None,
        profile: str = "dynamic",
        strategy: str = "deeplob_mbp_dynamic_v1",
    ):
        self.settings = settings
        self.paper_trader = paper_trader
        self.trade_summary_sink = trade_summary_sink
        self.profile = str(profile)
        self.strategy = str(strategy)
        self.contracts: dict[str, dict] = {}
        self.quotes: dict[int, dict] = {}
        self._candidate_action = "NO_TRADE"
        self._candidate_count = 0
        self._last_exit_mono = float("-inf")
        self._entries = 0
        self._exits = 0
        self._blocks = 0
        self._timezone = ZoneInfo("Asia/Kolkata")

    def register_contracts(self, selection: Mapping[str, Mapping]) -> list[dict]:
        subscriptions = []
        for side in ("CE", "PE"):
            leg = dict(selection.get(side) or {})
            secid = int(leg.get("security_id", 0) or 0)
            if not secid:
                continue
            leg["security_id"] = secid
            leg["tag"] = f"NIFTY_{side}"
            self.contracts[side] = leg
            subscriptions.append(
                {
                    "ExchangeSegment": "NSE_FNO",
                    "SecurityId": str(secid),
                    "tag": leg["tag"],
                }
            )
            logger.info(
                "DEEPLOB_OPTION_PAPER_CONTRACT | profile=%s | side=%s | secid=%s | strike=%s | "
                "expiry=%s | source=%s",
                self.profile,
                side,
                secid,
                leg.get("strike"),
                leg.get("expiry"),
                leg.get("selection_source", "UNKNOWN"),
            )
        return subscriptions

    def on_quote(
        self,
        secid: int,
        tag: str,
        ltp: float,
        *,
        bid: float,
        ask: float,
        received_ts: float,
    ) -> None:
        secid = int(secid)
        if secid not in {
            int(contract["security_id"]) for contract in self.contracts.values()
        }:
            return
        self.quotes[secid] = {
            "tag": tag,
            "ltp": float(ltp),
            "bid": float(bid or 0.0),
            "ask": float(ask or 0.0),
            "received_ts": float(received_ts),
        }
        self.paper_trader.on_tick(secid, float(ltp))
        self._evaluate_price_exit(secid)

    def on_prediction(
        self,
        *,
        paper_action: str,
        confidence: float,
        composite: CompositeMarketSnapshot | None,
        probability_down: float,
        probability_flat: float,
        probability_up: float,
        model_version: str,
        horizon_sec: int,
    ) -> None:
        if not self.settings.enabled or composite is None:
            return
        if self.paper_trader.has_open_position():
            self._evaluate_prediction_exit(paper_action, confidence)
            return
        clock = datetime.now(self._timezone).time()
        if self.settings.enforce_market_hours and not (
            self.settings.market_start <= clock < self.settings.entry_cutoff
        ):
            self._blocks += 1
            self._reset_candidate()
            return

        pressure = float(composite.features.pressure_score)
        aligned = (
            paper_action == "BUY_CE" and pressure >= self.settings.pressure_threshold
        ) or (
            paper_action == "BUY_PE" and pressure <= -self.settings.pressure_threshold
        )
        if confidence < self.settings.confidence_threshold or not aligned:
            self._reset_candidate()
            return

        if paper_action == self._candidate_action:
            self._candidate_count += 1
        else:
            self._candidate_action = paper_action
            self._candidate_count = 1
        if self._candidate_count < self.settings.confirmation_count:
            return
        if time.monotonic() - self._last_exit_mono < self.settings.entry_cooldown_sec:
            self._blocks += 1
            self._reset_candidate()
            return

        side = "CE" if paper_action == "BUY_CE" else "PE"
        contract = self.contracts.get(side)
        if not contract:
            self._blocks += 1
            logger.warning(
                "DEEPLOB_OPTION_PAPER_ENTRY_BLOCKED | profile=%s | reason=CONTRACT_MISSING | side=%s",
                self.profile,
                side,
            )
            self._reset_candidate()
            return
        secid = int(contract["security_id"])
        quote = self.quotes.get(secid)
        quote_age = time.time() - float((quote or {}).get("received_ts", 0.0) or 0.0)
        entry_price = float((quote or {}).get("ask", 0.0) or 0.0)
        if not quote or quote_age > self.settings.max_quote_age_sec or entry_price <= 0:
            self._blocks += 1
            logger.warning(
                "DEEPLOB_OPTION_PAPER_ENTRY_BLOCKED | profile=%s | reason=OPTION_QUOTE_NOT_EXECUTABLE | "
                "side=%s | secid=%s | quote_age_sec=%s | ask=%s",
                self.profile,
                side,
                secid,
                f"{quote_age:.3f}" if quote else "NA",
                f"{entry_price:.2f}",
            )
            self._reset_candidate()
            return

        metadata = {
            "strategy": self.strategy,
            "paper_profile": self.profile,
            "future_ltp": float(composite.full_quote.get("ltp", 0.0) or 0.0),
            "future_mid": float(composite.features.mid),
            "pressure_score": pressure,
            "model_confidence": float(confidence),
            "probability_down": float(probability_down),
            "probability_flat": float(probability_flat),
            "probability_up": float(probability_up),
            "model_version": model_version,
            "model_horizon_sec": int(horizon_sec),
            "option_strike": contract.get("strike"),
            "option_expiry": contract.get("expiry"),
            "entry_quote_age_sec": quote_age,
            "entry_execution_side": "ASK",
        }
        reason = (
            f"DEEPLOB_MBP_ENTRY:{side}|confidence={confidence:.4f}|"
            f"pressure={pressure:.4f}"
        )
        accepted = self.paper_trader.on_entry(
            secid,
            contract["tag"],
            "LONG",
            entry_price,
            lots=1,
            reason=reason,
            metadata=metadata,
        )
        if accepted:
            self._entries += 1
            logger.info(
                "DEEPLOB_OPTION_PAPER_ENTRY | profile=%s | strategy=%s | tag=%s | secid=%s | price=%.2f | "
                "execution=ASK | future_ltp=%.2f | confidence=%.4f | pressure=%.4f | "
                "horizon_sec=%s",
                self.profile,
                self.strategy,
                contract["tag"],
                secid,
                entry_price,
                metadata["future_ltp"],
                confidence,
                pressure,
                horizon_sec,
            )
        self._reset_candidate()

    def heartbeat(self) -> None:
        if not self.paper_trader.has_open_position():
            return
        clock = datetime.now(self._timezone).time()
        if self.settings.enforce_market_hours and clock >= self.settings.market_end:
            secid = int(next(iter(self.paper_trader.positions)))
            self._exit(secid, "DEEPLOB_MBP_EXIT:MARKET_CLOSE")
            return
        secid = int(next(iter(self.paper_trader.positions)))
        self._evaluate_price_exit(secid)

    def _evaluate_prediction_exit(self, paper_action: str, confidence: float) -> None:
        secid, position = next(iter(self.paper_trader.positions.items()))
        held_side = "CE" if str(position.get("tag", "")).endswith("_CE") else "PE"
        opposite = (held_side == "CE" and paper_action == "BUY_PE") or (
            held_side == "PE" and paper_action == "BUY_CE"
        )
        if opposite and confidence >= self.settings.confidence_threshold:
            self._exit(int(secid), "DEEPLOB_MBP_EXIT:MODEL_REVERSAL")

    def _evaluate_price_exit(self, secid: int) -> None:
        position = self.paper_trader.positions.get(int(secid))
        quote = self.quotes.get(int(secid))
        if not position or not quote:
            return
        executable_bid = float(quote.get("bid", 0.0) or 0.0)
        if executable_bid <= 0:
            return
        entry = float(position["entry"])
        pnl_pct = (executable_bid - entry) / entry * 100.0
        hold_sec = time.time() - float(position["entry_ts"])
        if pnl_pct >= self.settings.take_profit_pct:
            self._exit(secid, "DEEPLOB_MBP_EXIT:TAKE_PROFIT")
        elif pnl_pct <= -self.settings.stop_loss_pct:
            self._exit(secid, "DEEPLOB_MBP_EXIT:STOP_LOSS")
        elif hold_sec >= self.settings.max_hold_sec:
            self._exit(secid, "DEEPLOB_MBP_EXIT:TIMEOUT")

    def _exit(self, secid: int, reason: str) -> None:
        quote = self.quotes.get(int(secid), {})
        executable_bid = float(quote.get("bid", 0.0) or 0.0)
        if executable_bid <= 0:
            logger.warning(
                "DEEPLOB_OPTION_PAPER_EXIT_BLOCKED | profile=%s | reason=NO_EXECUTABLE_BID | secid=%s",
                self.profile,
                secid,
            )
            return
        position = dict(self.paper_trader.positions.get(int(secid)) or {})
        self.paper_trader.on_exit(int(secid), executable_bid, reason=reason)
        summary = dict(getattr(self.paper_trader, "last_trade_summary", None) or {})
        if summary and self.trade_summary_sink is not None:
            context_keys = (
                "strategy",
                "paper_profile",
                "future_ltp",
                "future_mid",
                "pressure_score",
                "model_confidence",
                "probability_down",
                "probability_flat",
                "probability_up",
                "model_version",
                "model_horizon_sec",
                "option_strike",
                "option_expiry",
                "entry_quote_age_sec",
                "entry_execution_side",
            )
            summary.update(
                {
                    key: position.get(key)
                    for key in context_keys
                    if position.get(key) is not None
                }
            )
            summary.update(
                {
                    "schema_version": 1,
                    "index": "NIFTY",
                    "runtime": f"deeplob_live_{self.profile}",
                    "strategy": self.strategy,
                    "paper_profile": self.profile,
                    "paper": True,
                    "exit_execution_side": "BID",
                    "exit_quote_age_sec": max(
                        0.0,
                        time.time()
                        - float(quote.get("received_ts", 0.0) or 0.0),
                    ),
                }
            )
            self.trade_summary_sink.record(summary)
        self._last_exit_mono = time.monotonic()
        self._exits += 1
        logger.info(
            "DEEPLOB_OPTION_PAPER_EXIT | profile=%s | strategy=%s | secid=%s | price=%.2f | execution=BID | reason=%s",
            self.profile,
            self.strategy,
            secid,
            executable_bid,
            reason,
        )

    def _reset_candidate(self) -> None:
        self._candidate_action = "NO_TRADE"
        self._candidate_count = 0

    def health(self) -> dict:
        return {
            "profile": self.profile,
            "strategy": self.strategy,
            "enabled": self.settings.enabled,
            "contracts": {
                side: int(contract["security_id"])
                for side, contract in self.contracts.items()
            },
            "fresh_quotes": sum(
                1
                for quote in self.quotes.values()
                if time.time() - float(quote["received_ts"]) <= self.settings.max_quote_age_sec
            ),
            "open_positions": len(self.paper_trader.positions),
            "entries": self._entries,
            "exits": self._exits,
            "blocks": self._blocks,
        }


class ParallelDeepLobOptionPaperExecutor:
    """Fans one live evidence stream into isolated paper portfolios."""

    def __init__(self, executors):
        self.executors = tuple(executor for executor in executors if executor is not None)

    @property
    def contracts(self) -> dict[str, dict]:
        return self.executors[0].contracts if self.executors else {}

    def register_contracts(self, selection: Mapping[str, Mapping]) -> list[dict]:
        unique = {}
        for executor in self.executors:
            for subscription in executor.register_contracts(selection):
                key = (
                    subscription.get("ExchangeSegment"),
                    subscription.get("SecurityId"),
                )
                unique[key] = subscription
        return list(unique.values())

    def on_quote(self, *args, **kwargs) -> None:
        for executor in self.executors:
            try:
                executor.on_quote(*args, **kwargs)
            except Exception:
                logger.exception(
                    "DEEPLOB_OPTION_PAPER_PROFILE_FAILED | profile=%s | stage=QUOTE",
                    executor.profile,
                )

    def on_prediction(self, *args, **kwargs) -> None:
        for executor in self.executors:
            try:
                executor.on_prediction(*args, **kwargs)
            except Exception:
                logger.exception(
                    "DEEPLOB_OPTION_PAPER_PROFILE_FAILED | profile=%s | stage=PREDICTION",
                    executor.profile,
                )

    def heartbeat(self) -> None:
        for executor in self.executors:
            try:
                executor.heartbeat()
            except Exception:
                logger.exception(
                    "DEEPLOB_OPTION_PAPER_PROFILE_FAILED | profile=%s | stage=HEARTBEAT",
                    executor.profile,
                )

    def health(self) -> dict:
        return {
            "profiles": {
                executor.profile: executor.health() for executor in self.executors
            }
        }

