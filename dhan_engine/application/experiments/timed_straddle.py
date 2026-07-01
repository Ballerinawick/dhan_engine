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
    lots: int = 1
    lot_size_override: int = 0
    hold_sec: float = 300.0
    profit_target_net: float = 100.0
    round_trip_cost: float = 80.0
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
            lots=max(1, _env_int("TIMED_STRADDLE_LOTS", 1)),
            lot_size_override=max(0, _env_int("TIMED_STRADDLE_LOT_SIZE", 0)),
            hold_sec=max(1.0, _env_float("TIMED_STRADDLE_HOLD_SEC", 300.0)),
            profit_target_net=_env_float("TIMED_STRADDLE_PROFIT_TARGET_NET", 100.0),
            round_trip_cost=max(0.0, _env_float("TIMED_STRADDLE_ROUND_TRIP_COST", 80.0)),
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
    ce_secid: int
    pe_secid: int
    lot_size: int
    lots: int
    entry_ts: float
    ce_entry: float
    pe_entry: float
    max_net_pnl: float
    min_net_pnl: float


class TimedStraddleBook:
    """Pure paired-position accounting using executable ask/bid prices."""

    def __init__(self, settings: TimedStraddleSettings):
        self.settings = settings
        self.position: TimedStraddlePosition | None = None

    def open(
        self,
        *,
        cycle: int,
        selection: dict[str, Any],
        ce: LegQuote,
        pe: LegQuote,
        now: float,
    ) -> TimedStraddlePosition:
        if self.position is not None:
            raise RuntimeError("Timed straddle position already open")
        if not ce.valid or not pe.valid:
            raise ValueError("Both CE and PE require valid executable quotes")
        lot_size = self.settings.lot_size_override or int(selection.get("lot_size", 1) or 1)
        initial_net = -self.settings.round_trip_cost
        self.position = TimedStraddlePosition(
            cycle=cycle,
            index=self.settings.index,
            strike=float(selection["strike"]),
            expiry=str(selection["expiry"]),
            ce_secid=ce.secid,
            pe_secid=pe.secid,
            lot_size=lot_size,
            lots=self.settings.lots,
            entry_ts=now,
            ce_entry=ce.ask,
            pe_entry=pe.ask,
            max_net_pnl=initial_net,
            min_net_pnl=initial_net,
        )
        return self.position

    def mark(self, ce: LegQuote, pe: LegQuote, now: float) -> dict[str, float]:
        position = self.position
        if position is None:
            raise RuntimeError("No timed straddle position")
        quantity = position.lot_size * position.lots
        ce_pnl = (ce.bid - position.ce_entry) * quantity
        pe_pnl = (pe.bid - position.pe_entry) * quantity
        gross = ce_pnl + pe_pnl
        net = gross - self.settings.round_trip_cost
        position.max_net_pnl = max(position.max_net_pnl, net)
        position.min_net_pnl = min(position.min_net_pnl, net)
        return {
            "ce_pnl": ce_pnl,
            "pe_pnl": pe_pnl,
            "gross_pnl": gross,
            "net_pnl": net,
            "hold_sec": max(0.0, now - position.entry_ts),
        }

    def exit_reason(self, ce: LegQuote, pe: LegQuote, now: float, force_close: bool = False) -> str | None:
        mark = self.mark(ce, pe, now)
        if force_close:
            return "MARKET_FORCE_CLOSE"
        if mark["net_pnl"] >= self.settings.profit_target_net:
            return "NET_PROFIT_TARGET"
        if mark["hold_sec"] >= self.settings.hold_sec:
            return "FIVE_MINUTE_TIMEOUT"
        return None

    def close(self, ce: LegQuote, pe: LegQuote, now: float, reason: str) -> dict[str, Any]:
        position = self.position
        if position is None:
            raise RuntimeError("No timed straddle position")
        mark = self.mark(ce, pe, now)
        summary = {
            **asdict(position),
            **mark,
            "ce_exit": ce.bid,
            "pe_exit": pe.bid,
            "combined_entry_premium": position.ce_entry + position.pe_entry,
            "combined_exit_premium": ce.bid + pe.bid,
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

    def _fresh_pair(self, now: float, *, allow_stale: bool = False) -> tuple[LegQuote, LegQuote] | None:
        if not self.selection:
            return None
        ce = self.quotes.get(int(self.selection["ce_secid"]))
        pe = self.quotes.get(int(self.selection["pe_secid"]))
        if not ce or not pe or not ce.valid or not pe.valid:
            return None
        if not allow_stale and (
            now - ce.ts > self.settings.quote_stale_sec or now - pe.ts > self.settings.quote_stale_sec
        ):
            return None
        return ce, pe

    def _select_pair(self, now: float) -> None:
        if now < self._selection_retry_at:
            return
        try:
            selection = self.selector.select_atm_pair(self.settings.index)
        except Exception:
            logger.exception("TIMED_STRADDLE_SELECTION_FAILED | index=%s", self.settings.index)
            self._selection_retry_at = now + 10.0
            return
        if not selection:
            logger.warning("TIMED_STRADDLE_SELECTION_EMPTY | index=%s", self.settings.index)
            self._selection_retry_at = now + 10.0
            return
        self.selection = selection
        self.quotes.clear()
        subscriptions = [
            (int(selection["ce_secid"]), f"{self.settings.index}_CE"),
            (int(selection["pe_secid"]), f"{self.settings.index}_PE"),
        ]
        self.quote_stream.replace_subscriptions(subscriptions, reason=f"timed_straddle_cycle_{self.cycle_count + 1}")
        logger.info(
            "TIMED_STRADDLE_PAIR_SELECTED | cycle=%s | index=%s | strike=%.2f | expiry=%s | ce=%s | pe=%s | lot_size=%s",
            self.cycle_count + 1, self.settings.index, selection["strike"], selection["expiry"],
            selection["ce_secid"], selection["pe_secid"], selection["lot_size"],
        )

    def step(self, now: float | None = None) -> None:
        now = self.clock() if now is None else now
        with self._lock:
            force_close = self.book.position is not None and self._must_force_close(now)
            pair = self._fresh_pair(now, allow_stale=force_close)
            if self.book.position is not None:
                if pair is None:
                    return
                ce, pe = pair
                reason = self.book.exit_reason(ce, pe, now, force_close=force_close)
                if reason:
                    summary = self.book.close(ce, pe, now, reason)
                    self.realized_net += float(summary["net_pnl"])
                    self.wins += int(summary["net_pnl"] > 0)
                    self.losses += int(summary["net_pnl"] <= 0)
                    self.sink.record("timed_straddle", summary)
                    logger.info(
                        "TIMED_STRADDLE_EXIT | cycle=%s | reason=%s | hold=%.1fs | ce_pnl=%+.2f | pe_pnl=%+.2f | gross=%+.2f | fees=%.2f | net=%+.2f | daily_net=%+.2f",
                        summary["cycle"], reason, summary["hold_sec"], summary["ce_pnl"], summary["pe_pnl"],
                        summary["gross_pnl"], summary["fees"], summary["net_pnl"], self.realized_net,
                    )
                    self.selection = None
                return

            if not self._can_start_cycle(now):
                return
            if self.selection is None:
                self._select_pair(now)
                return
            if pair is None:
                return
            ce, pe = pair
            self.cycle_count += 1
            position = self.book.open(cycle=self.cycle_count, selection=self.selection, ce=ce, pe=pe, now=now)
            logger.info(
                "TIMED_STRADDLE_ENTRY | cycle=%s | index=%s | strike=%.2f | ce_ask=%.2f | pe_ask=%.2f | combined=%.2f | lot_size=%s | lots=%s | timeout=%.0fs | net_target=%.2f",
                position.cycle, position.index, position.strike, position.ce_entry, position.pe_entry,
                position.ce_entry + position.pe_entry, position.lot_size, position.lots,
                self.settings.hold_sec, self.settings.profit_target_net,
            )

    def _heartbeat(self, now: float) -> None:
        if now - self._last_heartbeat < self.settings.heartbeat_sec:
            return
        self._last_heartbeat = now
        pair = self._fresh_pair(now)
        mark = None
        if self.book.position and pair:
            mark = self.book.mark(pair[0], pair[1], now)
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
            "TIMED_STRADDLE_RUNTIME_ACTIVE | index=%s | hold=%.0fs | cutoff=15:25 | force_close=15:30 | max_cycles=%s | paper=true",
            self.settings.index, self.settings.hold_sec, self.settings.max_cycles,
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
