from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, time as dtime
from typing import TYPE_CHECKING, Any, Callable
from zoneinfo import ZoneInfo

from dhan_engine.infrastructure.mongo.trade_summary_sink import TradeSummarySink

if TYPE_CHECKING:
    from dhan_engine.application.market_data import FutureQuoteStream
    from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
    from dhan_engine.infrastructure.dhan.option_chain_selector import OptionChainSelector


logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)) or default)


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)) or default)


@dataclass(frozen=True)
class TimedStraddleSettings:
    client_id: str
    access_token: str
    csv_file: str = "api-scrip-master.csv"
    index: str = "NIFTY"
    strike_step: int = 50
    wing_steps: int = 1
    lots: int = 1
    lot_size_override: int = 0
    hold_sec: float = 180.0
    profit_target_net: float = 100.0
    round_trip_cost: float = 160.0
    quote_stale_sec: float = 3.0
    max_cycles: int = 74
    heartbeat_sec: float = 10.0
    market_start: dtime = dtime(9, 15)
    entry_cutoff: dtime = dtime(15, 25)
    force_close: dtime = dtime(15, 30)

    @classmethod
    def from_env(cls) -> "TimedStraddleSettings":
        client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
        access_token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
        if not client_id or not access_token:
            raise RuntimeError("Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN")
        index = os.getenv("TIMED_STRADDLE_INDEX", "NIFTY").strip().upper() or "NIFTY"
        default_step = 100 if index == "BANKNIFTY" else 50
        return cls(
            client_id=client_id,
            access_token=access_token,
            csv_file=os.getenv("CSV_FILE", "api-scrip-master.csv").strip() or "api-scrip-master.csv",
            index=index,
            strike_step=_env_int("TIMED_STRADDLE_STRIKE_STEP", default_step),
            wing_steps=max(1, _env_int("TIMED_STRADDLE_WING_STEPS", 1)),
            lots=max(1, _env_int("TIMED_STRADDLE_LOTS", 1)),
            lot_size_override=max(0, _env_int("TIMED_STRADDLE_LOT_SIZE", 0)),
            hold_sec=max(1.0, _env_float("TIMED_STRADDLE_HOLD_SEC", 180.0)),
            profit_target_net=_env_float("TIMED_STRADDLE_PROFIT_TARGET_NET", 100.0),
            round_trip_cost=max(0.0, _env_float("TIMED_STRADDLE_ROUND_TRIP_COST", 160.0)),
            quote_stale_sec=max(0.5, _env_float("TIMED_STRADDLE_QUOTE_STALE_SEC", 3.0)),
            max_cycles=max(1, _env_int("TIMED_STRADDLE_MAX_CYCLES", 74)),
            heartbeat_sec=max(1.0, _env_float("TIMED_STRADDLE_HEARTBEAT_SEC", 10.0)),
        )


@dataclass
class LegQuote:
    secid: int
    side: str
    ltp: float
    bid: float
    ask: float
    ts: float

    @property
    def valid(self) -> bool:
        return self.bid > 0 and self.ask >= self.bid and self.ltp > 0


@dataclass
class TimedStraddlePosition:
    cycle: int
    index: str
    strike: float
    expiry: str
    upper_strike: float
    lower_strike: float
    wing_width: float
    long_ce_secid: int
    long_pe_secid: int
    short_ce_secid: int
    short_pe_secid: int
    lot_size: int
    lots: int
    entry_ts: float
    long_ce_entry: float
    long_pe_entry: float
    short_ce_entry: float
    short_pe_entry: float
    net_debit_points: float
    max_profit_net: float
    max_net_pnl: float
    min_net_pnl: float


class TimedStraddleBook:
    """Reverse iron-fly accounting using executable prices for all four legs."""

    def __init__(self, settings: TimedStraddleSettings):
        self.settings = settings
        self.position: TimedStraddlePosition | None = None

    def open(
        self,
        *,
        cycle: int,
        selection: dict[str, Any],
        long_ce: LegQuote,
        long_pe: LegQuote,
        short_ce: LegQuote,
        short_pe: LegQuote,
        now: float,
    ) -> TimedStraddlePosition:
        if self.position is not None:
            raise RuntimeError("Timed straddle position already open")
        legs = (long_ce, long_pe, short_ce, short_pe)
        if not all(leg.valid for leg in legs):
            raise ValueError("All reverse iron-fly legs require valid executable quotes")
        lot_size = self.settings.lot_size_override or int(selection.get("lot_size", 1) or 1)
        quantity = lot_size * self.settings.lots
        net_debit_points = long_ce.ask + long_pe.ask - short_ce.bid - short_pe.bid
        max_profit_net = (float(selection["wing_width"]) - net_debit_points) * quantity - self.settings.round_trip_cost
        initial_net = -self.settings.round_trip_cost
        self.position = TimedStraddlePosition(
            cycle=cycle,
            index=self.settings.index,
            strike=float(selection["strike"]),
            expiry=str(selection["expiry"]),
            upper_strike=float(selection["upper_strike"]),
            lower_strike=float(selection["lower_strike"]),
            wing_width=float(selection["wing_width"]),
            long_ce_secid=long_ce.secid,
            long_pe_secid=long_pe.secid,
            short_ce_secid=short_ce.secid,
            short_pe_secid=short_pe.secid,
            lot_size=lot_size,
            lots=self.settings.lots,
            entry_ts=now,
            long_ce_entry=long_ce.ask,
            long_pe_entry=long_pe.ask,
            short_ce_entry=short_ce.bid,
            short_pe_entry=short_pe.bid,
            net_debit_points=net_debit_points,
            max_profit_net=max_profit_net,
            max_net_pnl=initial_net,
            min_net_pnl=initial_net,
        )
        return self.position

    def mark(
        self,
        long_ce: LegQuote,
        long_pe: LegQuote,
        short_ce: LegQuote,
        short_pe: LegQuote,
        now: float,
    ) -> dict[str, float]:
        position = self.position
        if position is None:
            raise RuntimeError("No timed straddle position")
        quantity = position.lot_size * position.lots
        long_ce_pnl = (long_ce.bid - position.long_ce_entry) * quantity
        long_pe_pnl = (long_pe.bid - position.long_pe_entry) * quantity
        short_ce_pnl = (position.short_ce_entry - short_ce.ask) * quantity
        short_pe_pnl = (position.short_pe_entry - short_pe.ask) * quantity
        gross = long_ce_pnl + long_pe_pnl + short_ce_pnl + short_pe_pnl
        net = gross - self.settings.round_trip_cost
        position.max_net_pnl = max(position.max_net_pnl, net)
        position.min_net_pnl = min(position.min_net_pnl, net)
        return {
            "long_ce_pnl": long_ce_pnl,
            "long_pe_pnl": long_pe_pnl,
            "short_ce_pnl": short_ce_pnl,
            "short_pe_pnl": short_pe_pnl,
            "gross_pnl": gross,
            "net_pnl": net,
            "hold_sec": max(0.0, now - position.entry_ts),
        }

    def exit_reason(self, *legs: LegQuote, now: float, force_close: bool = False) -> str | None:
        mark = self.mark(*legs, now)
        if force_close:
            return "MARKET_FORCE_CLOSE"
        if mark["net_pnl"] >= self.settings.profit_target_net:
            return "NET_PROFIT_TARGET"
        if mark["hold_sec"] >= self.settings.hold_sec:
            return "TIMED_HOLD_TIMEOUT"
        return None

    def close(self, *legs: LegQuote, now: float, reason: str) -> dict[str, Any]:
        position = self.position
        if position is None:
            raise RuntimeError("No timed straddle position")
        long_ce, long_pe, short_ce, short_pe = legs
        mark = self.mark(*legs, now)
        summary = {
            **asdict(position),
            **mark,
            "long_ce_exit": long_ce.bid,
            "long_pe_exit": long_pe.bid,
            "short_ce_exit": short_ce.ask,
            "short_pe_exit": short_pe.ask,
            "long_entry_premium": position.long_ce_entry + position.long_pe_entry,
            "short_entry_premium": position.short_ce_entry + position.short_pe_entry,
            "fees": self.settings.round_trip_cost,
            "exit_ts": now,
            "exit_reason": reason,
            "paper": True,
        }
        self.position = None
        return summary


class TimedStraddleRuntime:
    def __init__(
        self,
        settings: TimedStraddleSettings,
        master: InstrumentMaster,
        selector: OptionChainSelector,
        quote_stream: FutureQuoteStream,
        sink: TradeSummarySink | None = None,
        clock: Callable[[], float] = time.time,
    ):
        self.settings = settings
        self.master = master
        self.selector = selector
        self.quote_stream = quote_stream
        self.sink = sink or TradeSummarySink(
            collection_name=os.getenv("TIMED_STRADDLE_MONGO_COLLECTION", "timed_straddle_experiments"),
            portfolio_collection_name=os.getenv("TIMED_STRADDLE_PORTFOLIO_COLLECTION", "timed_straddle_daily"),
        )
        self.clock = clock
        self.book = TimedStraddleBook(settings)
        self.quotes: dict[int, LegQuote] = {}
        self.selection: dict[str, Any] | None = None
        self.cycle_count = 0
        self.realized_net = 0.0
        self.wins = 0
        self.losses = 0
        self._lock = threading.RLock()
        self._last_heartbeat = 0.0
        self._selection_retry_at = 0.0
        self._selection_started_at = 0.0
        self._last_subscription_recovery_at = 0.0
        self._subscribed_ids: set[int] = set()

    def on_quote(self, secid: int, tag: str, ltp: float, depth: Any) -> None:
        features = dict(getattr(depth, "features", None) or {})
        bid_prices = list(getattr(depth, "bid_price", None) or [])
        ask_prices = list(getattr(depth, "ask_price", None) or [])
        bid = float(features.get("best_bid") or (bid_prices[0] if bid_prices else 0.0) or 0.0)
        ask = float(features.get("best_ask") or (ask_prices[0] if ask_prices else 0.0) or 0.0)
        side = "CE" if str(tag).upper().endswith("_CE") else "PE"
        with self._lock:
            self.quotes[int(secid)] = LegQuote(int(secid), side, float(ltp), bid, ask, self.clock())

    def _now_ist(self, now: float) -> datetime:
        return datetime.fromtimestamp(now, IST)

    def _can_start_cycle(self, now: float) -> bool:
        current = self._now_ist(now).time().replace(tzinfo=None)
        return (
            self.settings.market_start <= current < self.settings.entry_cutoff
            and self.cycle_count < self.settings.max_cycles
        )

    def _must_force_close(self, now: float) -> bool:
        return self._now_ist(now).time().replace(tzinfo=None) >= self.settings.force_close

    def _fresh_structure(self, now: float, *, allow_stale: bool = False) -> tuple[LegQuote, LegQuote, LegQuote, LegQuote] | None:
        if not self.selection:
            return None
        keys = ("long_ce_secid", "long_pe_secid", "short_ce_secid", "short_pe_secid")
        legs = tuple(self.quotes.get(int(self.selection[key])) for key in keys)
        if not all(legs) or not all(leg.valid for leg in legs):
            return None
        if not allow_stale and any(now - leg.ts > self.settings.quote_stale_sec for leg in legs):
            return None
        return legs

    def _select_pair(self, now: float) -> None:
        if now < self._selection_retry_at:
            return
        try:
            selection = self.selector.select_atm_reverse_iron_fly(self.settings.index, self.settings.wing_steps)
        except Exception:
            logger.exception("TIMED_STRADDLE_SELECTION_FAILED | index=%s", self.settings.index)
            self._selection_retry_at = now + 10.0
            return
        if not selection:
            logger.warning("TIMED_STRADDLE_SELECTION_EMPTY | index=%s", self.settings.index)
            self._selection_retry_at = now + 10.0
            return
        previous_ids = set(self._subscribed_ids)
        self.selection = selection
        self.quotes.clear()
        subscriptions = [
            (int(selection["long_ce_secid"]), f"{self.settings.index}_LONG_CE"),
            (int(selection["long_pe_secid"]), f"{self.settings.index}_LONG_PE"),
            (int(selection["short_ce_secid"]), f"{self.settings.index}_SHORT_CE"),
            (int(selection["short_pe_secid"]), f"{self.settings.index}_SHORT_PE"),
        ]
        self.quote_stream.replace_subscriptions(subscriptions, reason=f"timed_straddle_cycle_{self.cycle_count + 1}")
        new_ids = {secid for secid, _ in subscriptions}
        if previous_ids and previous_ids != new_ids and hasattr(self.quote_stream, "reconnect_for_subscriptions"):
            self.quote_stream.reconnect_for_subscriptions(subscriptions, reason="timed_straddle_contract_change")
        self._subscribed_ids = new_ids
        self._selection_started_at = now
        logger.info(
            "TIMED_STRADDLE_STRUCTURE_SELECTED | cycle=%s | index=%s | spot=%.2f | atm=%.2f | lower=%.2f | upper=%.2f | long_ce=%s | long_pe=%s | short_ce=%s | short_pe=%s | lot_size=%s",
            self.cycle_count + 1, self.settings.index, selection["underlying_ltp"], selection["strike"],
            selection["lower_strike"], selection["upper_strike"], selection["long_ce_secid"],
            selection["long_pe_secid"], selection["short_ce_secid"], selection["short_pe_secid"], selection["lot_size"],
        )

    def _recover_missing_quotes(self, now: float) -> None:
        if not self.selection or now - self._selection_started_at < 5.0:
            return
        if now - self._last_subscription_recovery_at < 10.0:
            return
        self._last_subscription_recovery_at = now
        subscriptions = [
            (int(self.selection["long_ce_secid"]), f"{self.settings.index}_LONG_CE"),
            (int(self.selection["long_pe_secid"]), f"{self.settings.index}_LONG_PE"),
            (int(self.selection["short_ce_secid"]), f"{self.settings.index}_SHORT_CE"),
            (int(self.selection["short_pe_secid"]), f"{self.settings.index}_SHORT_PE"),
        ]
        if hasattr(self.quote_stream, "reconnect_for_subscriptions"):
            self.quote_stream.reconnect_for_subscriptions(subscriptions, reason="timed_straddle_missing_quotes")
            logger.warning("TIMED_STRADDLE_QUOTE_RECOVERY | cycle=%s | missing_or_stale=true", self.cycle_count + 1)

    def step(self, now: float | None = None) -> None:
        now = self.clock() if now is None else now
        with self._lock:
            force_close = self.book.position is not None and self._must_force_close(now)
            structure = self._fresh_structure(now, allow_stale=force_close)
            if self.book.position is not None:
                if structure is None:
                    return
                reason = self.book.exit_reason(*structure, now=now, force_close=force_close)
                if reason:
                    summary = self.book.close(*structure, now=now, reason=reason)
                    self.realized_net += float(summary["net_pnl"])
                    self.wins += int(summary["net_pnl"] > 0)
                    self.losses += int(summary["net_pnl"] <= 0)
                    self.sink.record("timed_straddle", summary)
                    logger.info(
                        "TIMED_STRADDLE_EXIT | cycle=%s | reason=%s | hold=%.1fs | long_ce=%+.2f | long_pe=%+.2f | short_ce=%+.2f | short_pe=%+.2f | gross=%+.2f | fees=%.2f | net=%+.2f | daily_net=%+.2f",
                        summary["cycle"], reason, summary["hold_sec"], summary["long_ce_pnl"],
                        summary["long_pe_pnl"], summary["short_ce_pnl"], summary["short_pe_pnl"],
                        summary["gross_pnl"], summary["fees"], summary["net_pnl"], self.realized_net,
                    )
                    self.selection = None
                return

            if not self._can_start_cycle(now):
                return
            if self.selection is None:
                self._select_pair(now)
                return
            if structure is None:
                self._recover_missing_quotes(now)
                return
            long_ce, long_pe, short_ce, short_pe = structure
            lot_size = self.settings.lot_size_override or int(self.selection.get("lot_size", 1) or 1)
            quantity = lot_size * self.settings.lots
            debit = long_ce.ask + long_pe.ask - short_ce.bid - short_pe.bid
            max_profit_net = (float(self.selection["wing_width"]) - debit) * quantity - self.settings.round_trip_cost
            if max_profit_net < self.settings.profit_target_net:
                logger.warning(
                    "TIMED_STRADDLE_ENTRY_BLOCKED | reason=MAX_PROFIT_BELOW_TARGET | debit=%.2f | width=%.2f | max_profit_net=%+.2f | target=%.2f",
                    debit, self.selection["wing_width"], max_profit_net, self.settings.profit_target_net,
                )
                self.selection = None
                self._selection_retry_at = now + 10.0
                return
            self.cycle_count += 1
            position = self.book.open(
                cycle=self.cycle_count, selection=self.selection, long_ce=long_ce, long_pe=long_pe,
                short_ce=short_ce, short_pe=short_pe, now=now,
            )
            logger.info(
                "TIMED_STRADDLE_ENTRY | cycle=%s | strategy=REVERSE_IRON_FLY | atm=%.2f | lower=%.2f | upper=%.2f | long_ce_ask=%.2f | long_pe_ask=%.2f | short_ce_bid=%.2f | short_pe_bid=%.2f | debit=%.2f | max_profit_net=%+.2f | lot_size=%s | lots=%s | timeout=%.0fs | net_target=%.2f",
                position.cycle, position.strike, position.lower_strike, position.upper_strike,
                position.long_ce_entry, position.long_pe_entry, position.short_ce_entry,
                position.short_pe_entry, position.net_debit_points, position.max_profit_net,
                position.lot_size, position.lots, self.settings.hold_sec, self.settings.profit_target_net,
            )

    def _heartbeat(self, now: float) -> None:
        if now - self._last_heartbeat < self.settings.heartbeat_sec:
            return
        self._last_heartbeat = now
        structure = self._fresh_structure(now)
        mark = None
        if self.book.position and structure:
            mark = self.book.mark(*structure, now)
        payload = {
            "cycles_completed": self.wins + self.losses,
            "cycles_opened": self.cycle_count,
            "wins": self.wins,
            "losses": self.losses,
            "daily_net_pnl": self.realized_net + (float(mark["net_pnl"]) if mark else 0.0),
            "daily_realized_pnl": self.realized_net,
            "daily_unrealized_pnl": float(mark["net_pnl"]) if mark else 0.0,
            "open_positions": int(self.book.position is not None),
            "current_cycle": self.book.position.cycle if self.book.position else None,
        }
        self.sink.record_portfolio("timed_straddle", payload)
        logger.info(
            "TIMED_STRADDLE_HEALTH | opened=%s/%s | completed=%s | wins=%s | losses=%s | realized=%+.2f | unrealized=%+.2f | open=%s",
            self.cycle_count, self.settings.max_cycles, self.wins + self.losses, self.wins, self.losses,
            self.realized_net, payload["daily_unrealized_pnl"], payload["open_positions"],
        )

    def run(self) -> None:
        self.quote_stream.start()
        logger.info(
            "TIMED_STRADDLE_RUNTIME_ACTIVE | strategy=REVERSE_IRON_FLY | index=%s | wing_steps=%s | hold=%.0fs | lots=%s | modeled_cost=%.2f | cutoff=15:25 | force_close=15:30 | max_cycles=%s | paper=true",
            self.settings.index, self.settings.wing_steps, self.settings.hold_sec, self.settings.lots,
            self.settings.round_trip_cost, self.settings.max_cycles,
        )
        try:
            while True:
                now = self.clock()
                self.step(now)
                self._heartbeat(now)
                time.sleep(0.2)
        except KeyboardInterrupt:
            logger.info("TIMED_STRADDLE_RUNTIME_STOPPED | reason=keyboard_interrupt")
        finally:
            self.quote_stream.close()


def build_timed_straddle_runtime(settings: TimedStraddleSettings) -> TimedStraddleRuntime:
    from dhan_engine.application.market_data import FutureQuoteStream
    from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
    from dhan_engine.infrastructure.dhan.option_chain_selector import OptionChainSelector

    master = InstrumentMaster(settings.csv_file, debug=False)
    selector = OptionChainSelector(
        access_token=settings.access_token,
        client_id=settings.client_id,
        instrument_master=master,
        strike_step_map={settings.index: settings.strike_step},
        mode=2,
        debug=False,
    )
    runtime: TimedStraddleRuntime
    quote_stream = FutureQuoteStream(
        client_id=settings.client_id,
        token=settings.access_token,
        exchange_segment="NSE_FNO",
        on_quote=lambda secid, tag, ltp, depth: runtime.on_quote(secid, tag, ltp, depth),
        debug=False,
        shard_count=1,
    )
    runtime = TimedStraddleRuntime(settings, master, selector, quote_stream)
    return runtime
