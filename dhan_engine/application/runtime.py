import logging
import os
import threading
import time
from datetime import datetime
from typing import Dict, Optional

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
        self.ws_retry_delay = 1
        self._future_ws_started = False
        self._future_ws_subscribed = False

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
        now = datetime.now(self.settings.timezone)

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

        if now < self.ws_blocked_until:
            return

        time.sleep(self.ws_retry_delay)

        try:
            if self.future_quote_stream is None:
                return

            # ONLY re-subscribe if needed
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
        self._future_ws_subscribed = False

        if "429" in str(error):
            # 🚫 block all WS attempts for cooldown
            self.ws_blocked_until = time.time() + 120  # 2 min block

            # 🔁 exponential backoff
            self.ws_retry_delay = min(self.ws_retry_delay * 2, 60)

            logger.error(
                "🚫 WS BLOCKED | cooldown=120s | retry_delay=%ss",
                self.ws_retry_delay,
            )
        else:
            logger.exception("WS_RUNTIME_ERROR | error=%s", error)

    def _handle_ws_connected(self) -> None:
        if self.ws_retry_delay != 1 or self.ws_blocked_until != 0:
            self.ws_retry_delay = 1
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

        snapshot = self.market_engine.update(pair.index, pair.underlying_ltp, ce_input, pe_input)
        if not snapshot:
            return

        snapshot["underlying_ltp"] = float(pair.underlying_ltp)
        pair.last_snapshot = snapshot

        turn_signal = self.turn_engine.update(snapshot)
        if not turn_signal:
            return

        pair.last_turn_signal = turn_signal
        route_secid, route_tag, route_ltp = pair.route_for_signal(turn_signal.get("signal"), tag, raw["ltp"])
        if route_secid is None:
            return

        flow = self.premium_flow.get("dominant")

        if flow:
            if flow == "CE" and "PE" in route_tag:
                logger.info("🚫 FLOW_BLOCK | PE against CE dominance")
                return

            if flow == "PE" and "CE" in route_tag:
                logger.info("🚫 FLOW_BLOCK | CE against PE dominance")
                return

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

        # STRUCTURE EXIT ONLY WHEN NO ACTIVE MOMENTUM TRADE
        if secid not in self.momentum_engine.active_trade:

            structure = self.structure_exit_engine.on_tick(
                secid=secid,
                tag=tag,
                ltp=raw["ltp"],
                paper_trader=self.paper_trader,
                decision_engine=self.decision_engine,
            )

            if structure and structure.get("exit"):
                logger.info("EXIT_OVERRIDE | %s | reason=%s", tag, structure["reason"])
                self._force_exit(pair, secid, tag, raw["ltp"], structure["reason"])
                return

        flow = self.premium_flow.get("dominant")

        active = self.paper_trader.trades.get(secid)
        if active:
            entry = active.get("entry_price", 0)
            if entry > 0:

                pnl_pct = ((raw["ltp"] - entry) / entry) * 100

                if flow:
                    if "CE" in tag and flow == "PE" and pnl_pct < 0:
                        logger.info("🧠 FLOW_EXIT | CE losing dominance to PE")
                        self._force_exit(pair, secid, tag, raw["ltp"], "FLOW_REVERSAL")
                        return

                    if "PE" in tag and flow == "CE" and pnl_pct < 0:
                        logger.info("🧠 FLOW_EXIT | PE losing dominance to CE")
                        self._force_exit(pair, secid, tag, raw["ltp"], "FLOW_REVERSAL")
                        return

        trail = self.trailing_exit_engine.on_tick(
            secid=secid,
            tag=tag,
            ltp=raw["ltp"],
            paper_trader=self.paper_trader,
            momentum_engine=self.momentum_engine,
        )
        if trail and trail.get("exit"):
            logger.info("EXIT_OVERRIDE | %s | reason=%s", tag, trail["reason"])
            self._force_exit(pair, secid, tag, raw["ltp"], trail["reason"])
            return

        if action == "EXIT":
            if decision and decision.get("exit_allowed") is False:
                return
            reason = decision.get("exit_reason", self.momentum_engine.last_exit_reason.get(secid, action))
            self._force_exit(pair, secid, tag, raw["ltp"], reason)

    def _force_exit(self, pair: PairRuntimeState, secid: int, tag: str, ltp: float, reason: str) -> bool:
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
        if hasattr(self.momentum_engine, "clear_trade"):
            self.momentum_engine.clear_trade(secid, reason)
        else:
            self.momentum_engine.active_trade.pop(secid, None)
        return True

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
                logger.info("⏸️ MARKET_CLOSED | idle mode active")
                time.sleep(30.0)
                continue

            now = time.time()
            if now - last_heartbeat >= self.settings.heartbeat_sec:
                last_heartbeat = now
                logger.info("%s ENGINE_RUNNING", datetime.now(self.settings.timezone).strftime("%H:%M:%S"))
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
