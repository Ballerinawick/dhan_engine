import logging
import os
import threading
import time
from datetime import datetime
from typing import Dict, Optional
from zoneinfo import ZoneInfo
from collections import defaultdict

import requests

from dhan_engine.application.market_data import FutureQuoteStream, OptionDepthStream
from dhan_engine.config.settings import RuntimeSettings
from dhan_engine.domain.features.depth_micro_features import DepthMicroFeatureBuilder
from dhan_engine.domain.market.market_state_engine import MarketStateEngine
from dhan_engine.domain.market.turn_detection_engine import TurnDetectionEngine
from dhan_engine.domain.state import PairRuntimeState
from dhan_engine.domain.strategy.institutional_decision_engine import InstitutionalDecisionEngine
from dhan_engine.domain.strategy.institutional_trailing_exit_engine import InstitutionalTrailingExitEngine
from dhan_engine.domain.strategy.options_momentum_engine import OptionsMomentumEngine
from dhan_engine.domain.strategy.structure_exit_engine import StructureExitEngine
from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
from dhan_engine.infrastructure.dhan.option_chain_selector import OptionChainSelector
from dhan_engine.simulations.paper_trade_manager import PaperTradeManager


logger = logging.getLogger(__name__)


def download_master_csv(csv_url: str, save_path: str) -> None:
    response = requests.get(csv_url, timeout=30)
    response.raise_for_status()
    with open(save_path, "wb") as handle:
        handle.write(response.content)
    logger.info(
        "Master CSV refreshed | path=%s | size_kb=%.2f",
        os.path.abspath(save_path),
        os.path.getsize(save_path) / 1024.0,
    )


class TradingRuntimeCoordinator:
    """Coordinates market-data streams and strategy execution."""
    FLOW_REVERSAL_CONFIRM_TICKS = 3
    FLOW_REVERSAL_MIN_HOLD_SEC = 20
    FLOW_REVERSAL_MIN_LOSS_PCT = -0.20
    FLOW_REVERSAL_CONFIRM_WINDOW_SEC = 8
    FLOW_REVERSAL_CONFIRM_MIN_INTERVAL_SEC = 1.0
    FLOW_REVERSAL_DECISION_COOLDOWN_SEC = 1.0
    PREMATURE_EXIT_THRESHOLD_SEC = 20

    EXIT_REASON_PRIORITY = {
        "OPPOSITE_TURN_CONFIRMED": 10,
        "STRUCTURE_BREAKDOWN": 20,
        "TRAIL_HIT": 30,
        "MOMENTUM_EXIT": 35,
        "FLOW_REVERSAL_CONFIRMED": 40,
        "STRATEGY_EXIT": 50,
    }

    def __init__(
        self,
        *,
        settings: RuntimeSettings,
        master: InstrumentMaster,
        feature_builder: DepthMicroFeatureBuilder,
        momentum_engine: OptionsMomentumEngine,
        paper_trader: PaperTradeManager,
        decision_engine: InstitutionalDecisionEngine,
        trailing_exit_engine: InstitutionalTrailingExitEngine,
        structure_exit_engine: StructureExitEngine,
        market_engine: MarketStateEngine,
        turn_engine: TurnDetectionEngine,
        selector: OptionChainSelector,
        future_quote_stream: Optional[FutureQuoteStream],
        option_quote_stream: Optional[FutureQuoteStream],
        option_depth_stream: Optional[OptionDepthStream],
    ):
        self.settings = settings
        self.master = master
        self.feature_builder = feature_builder
        self.momentum_engine = momentum_engine
        self.paper_trader = paper_trader
        self.decision_engine = decision_engine
        self.trailing_exit_engine = trailing_exit_engine
        self.structure_exit_engine = structure_exit_engine
        self.market_engine = market_engine
        self.turn_engine = turn_engine
        self.selector = selector
        self.future_quote_stream = future_quote_stream
        self.option_quote_stream = option_quote_stream
        self.option_depth_stream = option_depth_stream

        self._lock = threading.RLock()
        self._future_ready = threading.Event()
        self._zero_book_counter: Dict[int, int] = {}
        self._zero_book_warned = set()
        self.ws_blocked_until = 0
        self.ws_retry_delay = 5
        self._future_ws_started = False
        self._future_ws_subscribed = False
        self.timezone = ZoneInfo("Asia/Kolkata")

        self.pairs = {index: PairRuntimeState(index=index) for index in self.settings.indexes}
        self.future_secids: Dict[str, int] = {}
        self.future_index_by_secid: Dict[int, str] = {}
        self.option_index_by_secid: Dict[int, str] = {}
        self.full_quote_secid_tag: Dict[int, str] = {}

        self.premium_flow = {
            "CE": {"ltp": 0.0, "prev": 0.0, "velocity": 0.0},
            "PE": {"ltp": 0.0, "prev": 0.0, "velocity": 0.0},
            "dominant": None,
            "last_update": 0.0,
        }
        self.last_reselection_ts = 0.0
        self.reselection_cooldown_sec = 30
        self.last_selected_underlying: Dict[str, float] = {}
        self.reselection_move_threshold = {
            "NIFTY": 40,
            "BANKNIFTY": 100,
            "FINNIFTY": 40,
        }
        self.flow_reversal_state: Dict[int, dict] = {}
        self._tick_exit_guard: Dict[int, dict] = {}
        self.metrics = {
            "exit_reason_counts": defaultdict(int),
            "hold_time_buckets": defaultdict(int),
            "premature_exits": 0,
            "total_exits": 0,
        }

    def run(self) -> None:
        logger.info("MODE: FUTURE_WS_STREAM + OPTION_WS_STREAM + OPTION_DEPTH_STREAM")

        self._configure_underlying_contracts()
        if self.future_quote_stream is None:
            raise RuntimeError("Future quote stream is not configured")
        subscriptions = [(secid, f"{index}_FUT") for index, secid in self.future_secids.items()]
        try:
            if not self._future_ws_started:
                self.future_quote_stream.start()
                self._future_ws_started = True

            if not self._future_ws_subscribed:
                print("🚀 SUBSCRIBING FUTURE:", subscriptions)
                self.future_quote_stream.subscribe(subscriptions)
                self._future_ws_subscribed = True
        except Exception as error:
            self._handle_ws_error(error)

        if self.option_quote_stream is None:
            raise RuntimeError("Option quote stream is not configured")
        try:
            self.option_quote_stream.start()
        except Exception as error:
            self._handle_ws_error(error)
        if self.option_depth_stream is None:
            raise RuntimeError("Option depth stream is not configured")
        try:
            self.option_depth_stream.start()
        except Exception as error:
            self._handle_ws_error(error)

        self._wait_for_underlyings()
        self._select_and_subscribe_option_pairs()
        # 🚫 DISABLED: causing Dhan 429 rate limit
        # self._start_optional_full_quote_sampler()
        logger.info("⚠️ FULL QUOTE SAMPLER DISABLED (429 PROTECTION ACTIVE)")
        self._heartbeat_loop()

    def market_open(self) -> bool:
        now = datetime.now(self.timezone)

        if now.weekday() >= 5:
            return False

        market_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_end = now.replace(hour=15, minute=30, second=0, microsecond=0)

        return market_start <= now <= market_end

    def _configure_underlying_contracts(self) -> None:
        for index in self.settings.indexes:
            future_contract = self.master.get_nearest_future(index)
            secid = int(future_contract["security_id"])
            self.future_secids[index] = secid
            self.future_index_by_secid[secid] = index
            self.pairs[index].future_id = secid
            logger.info(
                "Underlying future registered | index=%s | secid=%s | symbol=%s",
                index,
                secid,
                future_contract["symbol"],
            )

    def _wait_for_underlyings(self) -> None:
        logger.info("Waiting for future websocket LTP stream")
        while not self._all_underlyings_ready():
            self._future_ready.wait(timeout=self.settings.startup_wait_sec)
            if self._all_underlyings_ready():
                break
            logger.info("FUTURE_WS_STARTUP_RETRY")
            self._retry_future_ws_startup()
        logger.info("Future websocket LTP stream ready")

    def _retry_future_ws_startup(self) -> None:
        now = time.time()
        last_retry_time = getattr(self, "_last_retry_time", 0)

        if now - last_retry_time < self.ws_retry_delay:
            return

        if now < self.ws_blocked_until:
            remaining = int(self.ws_blocked_until - now)
            logger.info(
                "⏳ WS_BLOCK_ACTIVE | retry paused | remaining=%ss",
                remaining,
            )
            return

        self._last_retry_time = time.time()
        time.sleep(self.ws_retry_delay)

        try:
            if self.future_quote_stream is None:
                return

            if not self._future_ws_subscribed:
                subscriptions = [
                    (secid, f"{index}_FUT")
                    for index, secid in self.future_secids.items()
                ]
                self.future_quote_stream.subscribe(subscriptions)
                self._future_ws_subscribed = True

            logger.info(
                "🔁 WS_RETRY | subscribe_only | delay=%ss",
                self.ws_retry_delay,
            )

        except Exception as error:
            self._handle_ws_error(error)

    def _handle_ws_error(self, error: Exception) -> None:
        error_str = str(error)

        if "429" in error_str or "Too many requests" in error_str:
            self.ws_blocked_until = time.time() + 60
            self.ws_retry_delay = min(self.ws_retry_delay * 2, 60)
            logger.error(
                "🚫 WS_BLOCKED | 429 detected | blocking retries for 60s"
            )
        else:
            logger.exception("WS_RUNTIME_ERROR | error=%s", error)

        self._future_ws_subscribed = False

    def _handle_ws_connected(self) -> None:
        if self.ws_retry_delay != 5 or self.ws_blocked_until != 0:
            self.ws_retry_delay = 5
            self.ws_blocked_until = 0
            logger.info("✅ WS_CONNECTED | retry_reset")

    def _all_underlyings_ready(self) -> bool:
        with self._lock:
            return all(self.pairs[index].underlying_ltp for index in self.settings.indexes)

    def _select_and_subscribe_option_pairs(self) -> None:
        for index in self.settings.indexes:
            underlying_ltp = self.pairs[index].underlying_ltp
            selection = self.selector.select_best(index, underlying_ltp_override=underlying_ltp)
            if not selection:
                logger.warning("Option selection returned no contracts | index=%s", index)
                continue

            subscriptions = []
            pair = self.pairs[index]

            if "CE" in selection:
                ce_id = int(selection["CE"]["security_id"])
                pair.ce_id = ce_id
                self.option_index_by_secid[ce_id] = index
                subscriptions.append((ce_id, f"{index}_CE"))

            if "PE" in selection:
                pe_id = int(selection["PE"]["security_id"])
                pair.pe_id = pe_id
                self.option_index_by_secid[pe_id] = index
                subscriptions.append((pe_id, f"{index}_PE"))

            if pair.ce_id and pair.pe_id:
                logger.info("PAIR_REGISTERED | %s | CE=%s | PE=%s", index, pair.ce_id, pair.pe_id)

            if subscriptions:
                self.option_depth_stream.subscribe(subscriptions)
                self.option_quote_stream.subscribe(subscriptions)
                for secid, tag in subscriptions:
                    self.full_quote_secid_tag[secid] = tag

    def on_future_quote(self, secid: int, tag: str, ltp: float, depth) -> None:
        self._handle_ws_connected()
        with self._lock:
            index = self.future_index_by_secid.get(int(secid))
            if index is None:
                return
            self.pairs[index].update_underlying_quote(
                {
                    "secid": int(secid),
                    "tag": str(tag),
                    "ltp": float(ltp),
                    "source": "FUTURE_WS",
                    "ts": getattr(depth, "ts", time.time()),
                    "bid_price": list(getattr(depth, "bid_price", []) or []),
                    "bid_qty": list(getattr(depth, "bid_qty", []) or []),
                    "ask_price": list(getattr(depth, "ask_price", []) or []),
                    "ask_qty": list(getattr(depth, "ask_qty", []) or []),
                }
            )
            if self._all_underlyings_ready():
                self._future_ready.set()
            self._reselect_option_pair_if_needed(index)

    def _should_reselect_options(self, index: str, underlying_ltp: float) -> bool:
        last = self.last_selected_underlying.get(index)
        if last is None:
            self.last_selected_underlying[index] = float(underlying_ltp)
            return False

        threshold = self.reselection_move_threshold.get(index, 40)
        moved = abs(float(underlying_ltp) - float(last))

        now = time.time()
        if moved >= threshold and (now - self.last_reselection_ts) >= self.reselection_cooldown_sec:
            return True

        return False

    def _reselect_option_pair_if_needed(self, index: str) -> None:
        pair = self.pairs.get(index)
        if pair is None or pair.underlying_ltp is None:
            return

        if not self._should_reselect_options(index, pair.underlying_ltp):
            return

        active_ce = pair.ce_id in self.paper_trader.positions if pair.ce_id else False
        active_pe = pair.pe_id in self.paper_trader.positions if pair.pe_id else False
        if active_ce or active_pe:
            logger.info("RESELECT_SKIPPED_ACTIVE_POSITION | index=%s", index)
            return

        selection = self.selector.select_best(index, underlying_ltp_override=pair.underlying_ltp)
        if not selection:
            logger.warning("RESELECT_NO_SELECTION | index=%s", index)
            return

        old_ce = pair.ce_id
        old_pe = pair.pe_id

        new_ce = int(selection["CE"]["security_id"]) if "CE" in selection else None
        new_pe = int(selection["PE"]["security_id"]) if "PE" in selection else None

        if old_ce == new_ce and old_pe == new_pe:
            self.last_selected_underlying[index] = float(pair.underlying_ltp)
            self.last_reselection_ts = time.time()
            logger.info("RESELECT_NO_CHANGE | index=%s", index)
            return

        # 🧹 CLEAR OLD STATE (MANDATORY)
        pair.ce_depth = None
        pair.pe_depth = None
        pair.ce_ltp = None
        pair.pe_ltp = None
        pair.ready_logged = False

        if old_ce:
            self.option_index_by_secid.pop(old_ce, None)

        if old_pe:
            self.option_index_by_secid.pop(old_pe, None)

        subscriptions = []
        if new_ce:
            pair.ce_id = new_ce
            self.option_index_by_secid[new_ce] = index
            subscriptions.append((new_ce, f"{index}_CE"))
        if new_pe:
            pair.pe_id = new_pe
            self.option_index_by_secid[new_pe] = index
            subscriptions.append((new_pe, f"{index}_PE"))

        if subscriptions:
            if self.option_depth_stream is not None:
                self.option_depth_stream.subscribe(subscriptions)
            if self.option_quote_stream is not None:
                self.option_quote_stream.subscribe(subscriptions)
            for secid, mapped_tag in subscriptions:
                self.full_quote_secid_tag[secid] = mapped_tag

        self.last_selected_underlying[index] = float(pair.underlying_ltp)
        self.last_reselection_ts = time.time()

        logger.info(
            "RESELECT_DONE | index=%s | old_ce=%s | new_ce=%s | old_pe=%s | new_pe=%s | underlying=%.2f",
            index, old_ce, new_ce, old_pe, new_pe, float(pair.underlying_ltp)
        )
        logger.info(
            "🧹 STATE_RESET_DONE | index=%s | new_ce=%s | new_pe=%s",
            index, new_ce, new_pe
        )

    def _on_option_full_quote(self, secid: int, tag: str, ltp: float, depth) -> None:
        self._handle_ws_connected()
        with self._lock:
            index = self.option_index_by_secid.get(int(secid))
            if index is None:
                index = str(tag).split("_")[0].upper()
            pair = self.pairs.get(index)
            if pair is None:
                return
            pair.update_option_ltp(int(secid), float(ltp))

            existing = pair.ce_depth if int(secid) == pair.ce_id else pair.pe_depth if int(secid) == pair.pe_id else None
            if not existing:
                return
            raw = dict(existing)
            raw["ltp"] = float(ltp)
            raw["secid"] = int(secid)
            raw["tag"] = str(tag)
            raw["ts"] = time.time()
            self._process_option_update(index, pair, int(secid), str(tag), raw)

    def on_option_depth(self, secid: int, tag: str, bid, ask) -> None:
        if not self.market_open():
            return

        raw = self.feature_builder.build(secid, bid, ask)
        if not raw:
            return

        raw["secid"] = int(secid)
        raw["tag"] = str(tag)
        raw["ts"] = time.time()

        index = self.option_index_by_secid.get(int(secid))
        if index is None:
            index = str(tag).split("_")[0].upper()
        pair = self.pairs.get(index)
        if pair is None:
            return

        with self._lock:
            ws_ltp = pair.ce_ltp if secid == pair.ce_id else pair.pe_ltp if secid == pair.pe_id else None
            if ws_ltp and ws_ltp > 0:
                raw["ltp"] = float(ws_ltp)
            pair.update_option_depth(secid, raw)
            self._process_option_update(index, pair, secid, tag, raw)

    def _process_option_update(self, index: str, pair: PairRuntimeState, secid: int, tag: str, raw: dict) -> None:
        if not self.market_open():
            return

        side = "CE" if "CE" in tag else "PE" if "PE" in tag else None
        if side:
            pf = self.premium_flow[side]

            pf["prev"] = pf["ltp"]
            pf["ltp"] = raw["ltp"]

            if pf["prev"] > 0:
                pf["velocity"] = pf["ltp"] - pf["prev"]

            self.premium_flow["last_update"] = time.time()

        ce_vel = self.premium_flow["CE"]["velocity"]
        pe_vel = self.premium_flow["PE"]["velocity"]

        if ce_vel > 0 and pe_vel < 0:
            self.premium_flow["dominant"] = "CE"

        elif pe_vel > 0 and ce_vel < 0:
            self.premium_flow["dominant"] = "PE"

        self._track_zero_book(secid, tag, raw)
        self.paper_trader.on_tick(secid, raw["ltp"])

        if pair.is_ready() and not pair.ready_logged:
            pair.ready_logged = True
            logger.info("PAIR_STATE_READY | %s | CE+PE+UNDERLYING LIVE", index)

        if pair.is_ready():
            self._update_market_snapshot(pair, raw, secid, tag)

        self._process_exit_engines(pair, secid, tag, raw)

    def _update_market_snapshot(self, pair: PairRuntimeState, raw: dict, secid: int, tag: str) -> None:
        if not self.market_open():
            return

        ce_input, pe_input = pair.build_market_inputs()
        if ce_input is None or pe_input is None or pair.underlying_ltp is None:
            return

        print("SNAPSHOT_GATE →", {
            "index": pair.index,
            "underlying": pair.underlying_ltp,
            "ce_ready": ce_input is not None,
            "pe_ready": pe_input is not None,
            "flow_dom": self.premium_flow.get("dominant"),
        })

        snapshot = self.market_engine.update(pair.index, pair.underlying_ltp, ce_input, pe_input)
        if not snapshot:
            return

        snapshot["underlying_ltp"] = float(pair.underlying_ltp)
        pair.last_snapshot = snapshot

        turn_signal = self.turn_engine.update(snapshot)
        if not turn_signal:
            print("TURN_SIGNAL_NONE →", pair.index)
            return

        pair.last_turn_signal = turn_signal
        route_secid, route_tag, route_ltp = pair.route_for_signal(turn_signal.get("signal"), tag, raw["ltp"])
        print("TURN_SIGNAL →", turn_signal)
        print("ROUTED →", route_secid, route_tag, route_ltp)
        if route_secid is None:
            print("ROUTE_FAILED →", turn_signal)
            return
        self._maybe_exit_on_opposite_turn(
            pair=pair,
            turn_signal=turn_signal,
            routed_secid=route_secid,
            routed_ltp=route_ltp,
        )

        flow = self.premium_flow.get("dominant")

        if flow:
            if flow == "CE" and "PE" in route_tag:
                logger.info("⚠️ FLOW_SOFT_BLOCK | PE against CE dominance")

            elif flow == "PE" and "CE" in route_tag:
                logger.info("⚠️ FLOW_SOFT_BLOCK | CE against PE dominance")

        print("CALLING_DECISION_ENGINE →", route_tag, route_ltp)
        decision = self.decision_engine.on_signal(
            secid=route_secid,
            tag=route_tag,
            ltp=route_ltp,
            signal=turn_signal["signal"],
            momentum_engine=self.momentum_engine,
            paper_trader=self.paper_trader,
            snapshot=snapshot,
        )
        print("DECISION_RESULT →", decision)

    def _process_exit_engines(self, pair: PairRuntimeState, secid: int, tag: str, raw: dict) -> None:
        action = self.momentum_engine.on_tick(secid, raw)
        decision = {"exit_allowed": True}
        tick_bucket = int(time.time() * 10)
        self._tick_exit_guard[secid] = {"bucket": tick_bucket, "reason": None, "priority": None}

        if action == "EXIT":
            decision = self.decision_engine.on_signal(
                secid=secid,
                tag=tag,
                ltp=raw["ltp"],
                signal=action,
                momentum_engine=self.momentum_engine,
                paper_trader=self.paper_trader,
                snapshot=pair.last_snapshot,
            )
            if decision and decision.get("exit_allowed") is not False:
                reason = decision.get("exit_reason", self.momentum_engine.last_exit_reason.get(secid, "MOMENTUM_EXIT"))
                if self._attempt_exit_once(pair, secid, tag, raw["ltp"], reason):
                    return

        structure = self.structure_exit_engine.on_tick(
            secid=secid,
            tag=tag,
            ltp=raw["ltp"],
            paper_trader=self.paper_trader,
            decision_engine=self.decision_engine,
        )
        if structure and structure.get("exit"):
            reason = structure.get("reason", "STRUCTURE_BREAKDOWN")
            logger.info("EXIT_OVERRIDE | %s | reason=%s", tag, reason)
            if self._attempt_exit_once(pair, secid, tag, raw["ltp"], reason):
                return

        active = self.paper_trader.positions.get(secid)
        if not active:
            self.flow_reversal_state.pop(secid, None)

        trail = self.trailing_exit_engine.on_tick(
            secid=secid,
            tag=tag,
            ltp=raw["ltp"],
            paper_trader=self.paper_trader,
            momentum_engine=self.momentum_engine,
        )
        if trail and trail.get("exit"):
            logger.info("EXIT_OVERRIDE | %s | reason=%s", tag, trail["reason"])
            if self._attempt_exit_once(pair, secid, tag, raw["ltp"], trail["reason"]):
                return

        active = self.paper_trader.positions.get(secid)
        if active:
            entry = active.get("entry", 0)
            if entry > 0:
                flow = self.premium_flow.get("dominant")
                pnl_pct = ((raw["ltp"] - entry) / entry) * 100
                hold_sec = max(time.time() - float(active.get("entry_ts", time.time())), 0.0)
                self._handle_flow_reversal_exit(
                    pair=pair,
                    secid=secid,
                    tag=tag,
                    ltp=float(raw["ltp"]),
                    flow=flow,
                    pnl_pct=pnl_pct,
                    hold_sec=hold_sec,
                )

    def _force_exit(self, pair: PairRuntimeState, secid: int, tag: str, ltp: float, reason: str) -> bool:
        active = self.paper_trader.positions.get(secid)
        entry_ts = float(active.get("entry_ts", time.time())) if active else time.time()
        hold_sec = max(time.time() - entry_ts, 0.0)

        try:
            decision = self.decision_engine.on_signal(
                secid=secid,
                tag=tag,
                ltp=ltp,
                signal="EXIT",
                momentum_engine=self.momentum_engine,
                paper_trader=self.paper_trader,
                snapshot=pair.last_snapshot,
            )
            if decision and decision.get("exit_allowed") is False:
                return False
        except Exception:
            logger.exception("force_exit decision engine failure | secid=%s | reason=%s", secid, reason)

        self.paper_trader.on_exit(secid, ltp, reason=reason)
        self.flow_reversal_state.pop(secid, None)
        if hasattr(self.momentum_engine, "clear_trade"):
            self.momentum_engine.clear_trade(secid, reason)
        else:
            self.momentum_engine.active_trade.pop(secid, None)
        self._record_exit_metrics(reason=reason, hold_sec=hold_sec, secid=secid, tag=tag)
        return True

    def _attempt_exit_once(self, pair: PairRuntimeState, secid: int, tag: str, ltp: float, reason: str) -> bool:
        tick_bucket = int(time.time() * 10)
        reason_priority = self.EXIT_REASON_PRIORITY.get(reason, 999)
        guard = self._tick_exit_guard.get(secid)
        if not guard or guard.get("bucket") != tick_bucket:
            guard = {"bucket": tick_bucket, "reason": None, "priority": None}
        else:
            used_priority = guard.get("priority")
            if used_priority is not None and reason_priority >= used_priority:
                logger.info(
                    "EXIT_GUARD_SKIP | secid=%s | tag=%s | reason=%s | blocked_by=%s",
                    secid,
                    tag,
                    reason,
                    guard.get("reason"),
                )
                return False
        if secid not in self.paper_trader.positions:
            return False
        success = self._force_exit(pair, secid, tag, ltp, reason)
        if success:
            guard["reason"] = reason
            guard["priority"] = reason_priority
            self._tick_exit_guard[secid] = guard
        return success

    def _handle_flow_reversal_exit(
        self,
        *,
        pair: PairRuntimeState,
        secid: int,
        tag: str,
        ltp: float,
        flow: Optional[str],
        pnl_pct: float,
        hold_sec: float,
    ) -> None:
        opposite_flow = (
            ("CE" in tag and flow == "PE")
            or ("PE" in tag and flow == "CE")
        )
        if not opposite_flow:
            self.flow_reversal_state.pop(secid, None)
            return

        state = self.flow_reversal_state.get(secid, {})
        now = time.time()
        last_eval_ts = float(state.get("last_eval_ts", 0.0))
        if now - last_eval_ts < self.FLOW_REVERSAL_DECISION_COOLDOWN_SEC:
            return
        state["last_eval_ts"] = now

        if hold_sec < self.FLOW_REVERSAL_MIN_HOLD_SEC:
            self.flow_reversal_state[secid] = state
            return

        if pnl_pct > self.FLOW_REVERSAL_MIN_LOSS_PCT:
            state["count"] = 0
            state["first_ts"] = 0.0
            state["last_seen_ts"] = 0.0
            self.flow_reversal_state[secid] = state
            return

        count = int(state.get("count", 0))
        first_ts = float(state.get("first_ts", 0.0))
        last_seen_ts = float(state.get("last_seen_ts", 0.0))
        if not first_ts or now - first_ts > self.FLOW_REVERSAL_CONFIRM_WINDOW_SEC:
            count = 0
            first_ts = now

        if last_seen_ts and now - last_seen_ts < self.FLOW_REVERSAL_CONFIRM_MIN_INTERVAL_SEC:
            self.flow_reversal_state[secid] = {
                "count": count,
                "first_ts": first_ts,
                "last_seen_ts": last_seen_ts,
                "last_eval_ts": now,
            }
            return

        count += 1
        last_seen_ts = now
        self.flow_reversal_state[secid] = {
            "count": count,
            "first_ts": first_ts,
            "last_seen_ts": last_seen_ts,
            "last_eval_ts": now,
        }
        if count < self.FLOW_REVERSAL_CONFIRM_TICKS:
            logger.info(
                "FLOW_EXIT_GUARD_WAIT | %s | count=%s/%s | hold=%.1fs | pnl_pct=%.3f",
                tag,
                count,
                self.FLOW_REVERSAL_CONFIRM_TICKS,
                hold_sec,
                pnl_pct,
            )
            return

        logger.info(
            "🧠 FLOW_EXIT_CONFIRMED | %s | hold=%.1fs | pnl_pct=%.3f | count=%s",
            tag,
            hold_sec,
            pnl_pct,
            count,
        )
        self._attempt_exit_once(pair, secid, tag, ltp, "FLOW_REVERSAL_CONFIRMED")

    def _maybe_exit_on_opposite_turn(
        self,
        pair: PairRuntimeState,
        turn_signal: dict,
        routed_secid: int,
        routed_ltp: float,
    ) -> bool:
        signal_name = str(turn_signal.get("signal", ""))

        if "BULLISH" in signal_name:
            opposite_secid = pair.pe_id
            opposite_tag = f"{pair.index}_PE"
            opposite_ltp = pair._best_leg_ltp(pair.pe_depth, pair.pe_ltp, routed_ltp)

        elif "BEARISH" in signal_name:
            opposite_secid = pair.ce_id
            opposite_tag = f"{pair.index}_CE"
            opposite_ltp = pair._best_leg_ltp(pair.ce_depth, pair.ce_ltp, routed_ltp)

        else:
            return False

        # Check if opposite position exists
        active = self.paper_trader.positions.get(opposite_secid)
        if not active:
            return False

        # Minimum hold time protection (critical fix)
        hold_sec = time.time() - float(active.get("entry_ts", time.time()))
        if hold_sec < 20:
            logger.info(
                "OPPOSITE_TURN_BLOCKED_MIN_HOLD | tag=%s | hold=%.2fs | signal=%s",
                opposite_tag,
                hold_sec,
                signal_name,
            )
            return False

        entry = float(active.get("entry", 0))
        pnl_pct = ((float(opposite_ltp) - entry) / entry) * 100 if entry > 0 else 0

        # Rule 1: If trade is in small profit, DO NOT exit (let it run)
        if pnl_pct > 0 and pnl_pct < 0.30:
            logger.info(
                "PROFIT_PROTECTION_BLOCK | tag=%s | pnl_pct=%.3f | hold=%.2fs",
                opposite_tag,
                pnl_pct,
                hold_sec,
            )
            return False

        # Rule 2: If profit is strong, allow exit
        if pnl_pct >= 0.30:
            logger.info(
                "PROFIT_TARGET_EXIT_ALLOWED | tag=%s | pnl_pct=%.3f",
                opposite_tag,
                pnl_pct,
            )

        # Rule 3: If loss, allow exit (cut quickly)
        if pnl_pct < 0:
            logger.info(
                "LOSS_EXIT_ALLOWED | tag=%s | pnl_pct=%.3f",
                opposite_tag,
                pnl_pct,
            )

        logger.info(
            "OPPOSITE_TURN_EXIT | index=%s | signal=%s | closing=%s | hold=%.2fs",
            pair.index,
            signal_name,
            opposite_tag,
            hold_sec,
        )

        return self._attempt_exit_once(
            pair,
            opposite_secid,
            opposite_tag,
            float(opposite_ltp),
            "OPPOSITE_TURN_CONFIRMED",
        )

    def _record_exit_metrics(self, *, reason: str, hold_sec: float, secid: int, tag: str) -> None:
        reason_code = reason or "UNKNOWN"
        self.metrics["exit_reason_counts"][reason_code] += 1
        self.metrics["total_exits"] += 1
        if hold_sec < self.PREMATURE_EXIT_THRESHOLD_SEC:
            self.metrics["premature_exits"] += 1

        if hold_sec < 20:
            hold_bucket = "<20s"
        elif hold_sec < 60:
            hold_bucket = "20-60s"
        elif hold_sec < 180:
            hold_bucket = "1-3m"
        else:
            hold_bucket = ">=3m"
        self.metrics["hold_time_buckets"][hold_bucket] += 1

        premature_rate = (
            self.metrics["premature_exits"] / max(self.metrics["total_exits"], 1)
        ) * 100.0
        logger.info(
            "EXIT_METRICS | secid=%s | tag=%s | reason=%s | hold_sec=%.2f | premature_rate=%.1f%% | reason_counts=%s | hold_buckets=%s",
            secid,
            tag,
            reason_code,
            hold_sec,
            premature_rate,
            dict(self.metrics["exit_reason_counts"]),
            dict(self.metrics["hold_time_buckets"]),
        )

    def _track_zero_book(self, secid: int, tag: str, raw: dict) -> None:
        bid_price = float(raw.get("bid_price", 0.0) or 0.0)
        ask_price = float(raw.get("ask_price", 0.0) or 0.0)
        if bid_price == 0.0 and ask_price == 0.0:
            count = self._zero_book_counter.get(secid, 0) + 1
            self._zero_book_counter[secid] = count
            if count >= 20 and secid not in self._zero_book_warned:
                logger.warning(
                    "ZERO_BOOK | tag=%s | secid=%s | depth prices are 0 - check Dhan depth entitlement/segment/subscription",
                    tag,
                    secid,
                )
                self._zero_book_warned.add(secid)
        else:
            self._zero_book_counter[secid] = 0
            self._zero_book_warned.discard(secid)

    def _start_optional_full_quote_sampler(self) -> None:
        """
        Disabled to prevent API rate limit (429 Too Many Requests)
        FullMarketQuoteSampler is redundant because:
        - OptionDepthStream already provides real-time data
        - OptionQuoteStream already provides LTP
        """
        return

    def _heartbeat_loop(self) -> None:
        last_heartbeat = 0.0
        logger.info("LIVE: INSTITUTIONAL OPTIONS ENGINE ACTIVE")
        while True:
            if not self.market_open():
                logger.info("⏸️ MARKET_CLOSED | idle mode")
                time.sleep(30.0)
                continue

            now = time.time()
            if now - last_heartbeat >= self.settings.heartbeat_sec:
                last_heartbeat = now
                logger.info("%s ENGINE_RUNNING", datetime.now(self.timezone).strftime("%H:%M:%S"))
                logger.info(
                    "📊 FLOW | CE_vel=%.2f | PE_vel=%.2f | DOM=%s",
                    self.premium_flow["CE"]["velocity"],
                    self.premium_flow["PE"]["velocity"],
                    self.premium_flow["dominant"],
                )

            time.sleep(1.0)


def build_runtime(settings: RuntimeSettings) -> TradingRuntimeCoordinator:
    try:
        download_master_csv(settings.master_url, settings.csv_file)
    except Exception:
        logger.exception("CSV download failed. Continuing with existing file.")

    master = InstrumentMaster(settings.csv_file, debug=False)
    feature_builder = DepthMicroFeatureBuilder()
    momentum_engine = OptionsMomentumEngine()
    paper_trader = PaperTradeManager(capital=settings.capital)
    decision_engine = InstitutionalDecisionEngine(debug=True)
    trailing_exit_engine = InstitutionalTrailingExitEngine(debug=True)
    structure_exit_engine = StructureExitEngine(debug=True)
    market_engine = MarketStateEngine()
    turn_engine = TurnDetectionEngine()
    selector = OptionChainSelector(
        access_token=settings.credentials.access_token,
        client_id=settings.credentials.client_id,
        instrument_master=master,
        strike_step_map=settings.strike_step,
        mode=settings.selector_mode,
        max_steps_each_side=settings.selector_steps_each_side,
        debug=True,
    )
    coordinator = TradingRuntimeCoordinator(
        settings=settings,
        master=master,
        feature_builder=feature_builder,
        momentum_engine=momentum_engine,
        paper_trader=paper_trader,
        decision_engine=decision_engine,
        trailing_exit_engine=trailing_exit_engine,
        structure_exit_engine=structure_exit_engine,
        market_engine=market_engine,
        turn_engine=turn_engine,
        selector=selector,
        future_quote_stream=None,
        option_quote_stream=None,
        option_depth_stream=None,
    )
    coordinator.future_quote_stream = FutureQuoteStream(
        client_id=settings.credentials.client_id,
        token=settings.credentials.access_token,
        exchange_segment=settings.future_exchange_segment,
        on_quote=coordinator.on_future_quote,
        debug=settings.future_quote_stream_debug,
    )
    coordinator.option_quote_stream = FutureQuoteStream(
        client_id=settings.credentials.client_id,
        token=settings.credentials.access_token,
        exchange_segment=settings.option_exchange_segment,
        on_quote=coordinator._on_option_full_quote,
        debug=False,
    )
    coordinator.option_depth_stream = OptionDepthStream(
        client_id=settings.credentials.client_id,
        token=settings.credentials.access_token,
        exchange_segment=settings.option_exchange_segment,
        on_depth=coordinator.on_option_depth,
    )
    return coordinator
