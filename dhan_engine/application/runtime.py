import json
import logging
import os
import threading
import time
from datetime import datetime
from typing import Dict, Optional
from zoneinfo import ZoneInfo
from collections import defaultdict, deque

import requests

from dhan_engine.application.market_data import FutureQuoteStream, OptionDepthStream
from dhan_engine.application.market_health import PairFreshness, age_from_ts, format_age
from dhan_engine.application.risk_manager import (
    EntryGateConfig,
    ScaleInGateConfig,
    entry_expected_edge,
    evaluate_entry_quality,
    evaluate_scale_in_quality,
)
from dhan_engine.analytics.tri_wave_live_analyzer import TriWaveLiveAnalyzer
from dhan_engine.analytics.tri_wave_session_recorder import TriWaveSessionRecorder
from dhan_engine.config.settings import RuntimeSettings
from dhan_engine.domain.features.depth_micro_features import DepthMicroFeatureBuilder
from dhan_engine.domain.market.market_state_engine import MarketStateEngine
from dhan_engine.domain.market.turn_detection_engine import TurnDetectionEngine
from dhan_engine.domain.market.tri_wave_tick_brain import TriWaveTickBrain
from dhan_engine.domain.market.tri_wave_v2_brain import TriWaveV2Brain
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
    TRI_WAVE_ONLY_MODE = False
    TRI_WAVE_V2_ONLY_MODE = True
    TRI_WAVE_REENTRY_COOLDOWN_SEC = 90
    TRI_WAVE_LOSS_REENTRY_COOLDOWN_SEC = 180
    TRI_WAVE_PROFIT_REENTRY_COOLDOWN_SEC = 60
    TRI_WAVE_DAILY_PROFIT_LOCK = 5000
    TRI_WAVE_DAILY_MAX_LOSS = -3000
    TRI_WAVE_MAX_TRADES_PER_DAY = 30
    TRI_WAVE_PEAK_PROFIT_TRACKING = True
    TRI_WAVE_PROFIT_DECAY_FROM_PEAK = 1600

    EXIT_REASON_PRIORITY = {
        "TRI_WAVE_EMERGENCY_EXIT": 1,
        "TRI_WAVE_FLIP_EXIT": 5,
        "TRI_WAVE_EXIT": 6,
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
        self._option_streams_started = False
        self.timezone = ZoneInfo("Asia/Kolkata")

        self.pairs = {index: PairRuntimeState(index=index) for index in self.settings.indexes}
        self.future_secids: Dict[str, int] = {}
        self.future_index_by_secid: Dict[int, str] = {}
        self.option_index_by_secid: Dict[int, str] = {}
        self.full_quote_secid_tag: Dict[int, str] = {}
        self.latest_full_features_by_secid: Dict[int, dict] = {}
        self.latest_full_raw_by_secid: Dict[int, dict] = {}
        self.latest_depth_features_by_secid: Dict[int, dict] = {}
        self.option_last_tick_ts_by_secid = {}
        self.option_ltp_last_ts_by_secid = {}
        self.option_ltp_last_value_by_secid = {}
        self.option_ltp_source_by_secid = {}
        self._last_pair_stale_log_ts = defaultdict(float)
        self._last_pair_resubscribe_ts = defaultdict(float)
        self._last_entry_stale_log_ts = defaultdict(float)
        self._last_depth_ltp_fallback_log_ts = defaultdict(float)
        self._recovery_fresh_since_ts = defaultdict(float)
        self._last_entry_cooldown_log_ts = defaultdict(float)
        self._last_entry_quality_log_ts = defaultdict(float)
        self._last_option_chain_health_log_ts = defaultdict(float)
        self._last_no_entry_watch_ts = 0.0
        self.stale_position_exit_sec = float(os.getenv("STALE_POSITION_EXIT_SEC", "90") or 90)
        self.stale_position_check_sec = float(os.getenv("STALE_POSITION_CHECK_SEC", "5") or 5)
        self.entry_fresh_ltp_max_age_sec = float(os.getenv("ENTRY_FRESH_LTP_MAX_AGE_SEC", "20") or 20)
        self.entry_pair_fresh_ltp_max_age_sec = float(os.getenv("ENTRY_PAIR_FRESH_LTP_MAX_AGE_SEC", str(self.entry_fresh_ltp_max_age_sec)) or self.entry_fresh_ltp_max_age_sec)
        self.entry_depth_ltp_fallback_enabled = str(os.getenv("ENTRY_DEPTH_LTP_FALLBACK_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
        self.entry_depth_ltp_max_spread_pct = float(os.getenv("ENTRY_DEPTH_LTP_MAX_SPREAD_PCT", "2.5") or 2.5)
        self.entry_min_dynamic_edge = float(os.getenv("TRIWAVE_ENTRY_MIN_DYNAMIC_EDGE", "0.15") or 0.15)
        self.entry_min_support_score = float(os.getenv("TRIWAVE_ENTRY_MIN_SUPPORT_SCORE", "0.40") or 0.40)
        self.entry_max_risk_score = float(os.getenv("TRIWAVE_ENTRY_MAX_RISK_SCORE", "0.45") or 0.45)
        self.entry_min_expected_net_rupees = float(os.getenv("TRIWAVE_ENTRY_MIN_EXPECTED_NET_RUPEES", "80") or 80)
        self.entry_min_expected_move_pct = float(os.getenv("TRIWAVE_ENTRY_MIN_EXPECTED_MOVE_PCT", "0.75") or 0.75)
        self.scale_in_enabled = str(os.getenv("TRIWAVE_SCALE_IN_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
        self.scale_in_max_lots = int(os.getenv("TRIWAVE_SCALE_IN_MAX_LOTS", "2") or 2)
        self.scale_in_min_profit_pct = float(os.getenv("TRIWAVE_SCALE_IN_MIN_PROFIT_PCT", "0.80") or 0.80)
        self.scale_in_min_support_score = float(os.getenv("TRIWAVE_SCALE_IN_MIN_SUPPORT_SCORE", "0.70") or 0.70)
        self.scale_in_max_risk_score = float(os.getenv("TRIWAVE_SCALE_IN_MAX_RISK_SCORE", "0.25") or 0.25)
        self.scale_in_min_edge = float(os.getenv("TRIWAVE_SCALE_IN_MIN_EDGE", "0.45") or 0.45)
        self.scale_in_cooldown_sec = float(os.getenv("TRIWAVE_SCALE_IN_COOLDOWN_SEC", "30") or 30)
        self.scale_in_fresh_ltp_max_age_sec = float(os.getenv("TRIWAVE_SCALE_IN_FRESH_LTP_MAX_AGE_SEC", "5") or 5)
        self.entry_gate_config = EntryGateConfig(
            min_support_score=self.entry_min_support_score,
            max_risk_score=self.entry_max_risk_score,
            min_dynamic_edge=self.entry_min_dynamic_edge,
            min_expected_net_rupees=self.entry_min_expected_net_rupees,
            min_expected_move_pct=self.entry_min_expected_move_pct,
        )
        self.scale_in_gate_config = ScaleInGateConfig(
            enabled=self.scale_in_enabled,
            max_lots=self.scale_in_max_lots,
            min_profit_pct=self.scale_in_min_profit_pct,
            min_support_score=self.scale_in_min_support_score,
            max_risk_score=self.scale_in_max_risk_score,
            min_edge=self.scale_in_min_edge,
            cooldown_sec=self.scale_in_cooldown_sec,
            fresh_ltp_max_age_sec=self.scale_in_fresh_ltp_max_age_sec,
        )
        self.stale_exit_quarantine_sec = float(os.getenv("TRIWAVE_STALE_EXIT_QUARANTINE_SEC", "600") or 600)
        self.pair_stale_entry_quarantine_sec = float(os.getenv("TRIWAVE_PAIR_STALE_ENTRY_QUARANTINE_SEC", "120") or 120)
        self.pair_stale_recovery_clear_fresh_sec = float(os.getenv("TRIWAVE_PAIR_STALE_RECOVERY_CLEAR_FRESH_SEC", "3") or 3)
        self.pair_stale_resubscribe_sec = float(os.getenv("PAIR_STALE_RESUBSCRIBE_SEC", "45") or 45)
        self.pair_stale_resubscribe_cooldown_sec = float(os.getenv("PAIR_STALE_RESUBSCRIBE_COOLDOWN_SEC", "45") or 45)
        self.static_daily_option_pairs = str(os.getenv("OPTION_STATIC_DAILY_PAIRS", "1")).strip().lower() in {"1", "true", "yes", "on"}
        self.option_reselection_enabled = str(os.getenv("OPTION_RESELECTION_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "on"}
        self.option_selection_retry_sec = float(os.getenv("OPTION_SELECTION_RETRY_SEC", "20") or 20)
        # In-place subscription refresh is safer than replacing a client while
        # one of its callback workers may still be busy.
        self.premium_rebuild_enabled = str(os.getenv("PREMIUM_STREAM_REBUILD_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "on"}
        self.premium_rebuild_attempt_threshold = int(os.getenv("PREMIUM_STREAM_REBUILD_ATTEMPT_THRESHOLD", "2") or 2)
        self.premium_rebuild_window_sec = float(os.getenv("PREMIUM_STREAM_REBUILD_WINDOW_SEC", "300") or 300)
        self.premium_rebuild_cooldown_sec = float(os.getenv("PREMIUM_STREAM_REBUILD_COOLDOWN_SEC", "120") or 120)
        self.premium_rebuild_verify_sec = float(os.getenv("PREMIUM_STREAM_REBUILD_VERIFY_SEC", "12") or 12)
        self.premium_stream_down_quarantine_sec = float(os.getenv("PREMIUM_STREAM_DOWN_QUARANTINE_SEC", "60") or 60)
        self.no_entry_watch_sec = float(os.getenv("TRIWAVE_NO_ENTRY_WATCH_SEC", "60") or 60)
        self.verbose_tick_logs = str(os.getenv("TRIWAVE_VERBOSE_TICK_LOGS", "0")).strip().lower() in {"1", "true", "yes", "on"}
        self._last_stale_position_check_ts = 0.0
        self.portfolio_snapshot_interval_sec = float(os.getenv("TRIWAVE_PORTFOLIO_SNAPSHOT_SEC", "5") or 5)
        self._last_portfolio_snapshot_ts = 0.0
        pair_stale_reconnect_default = "0" if self.static_daily_option_pairs else "1"
        self.pair_stale_reconnect_enabled = str(os.getenv("PAIR_STALE_RECONNECT_ENABLED", pair_stale_reconnect_default)).strip().lower() in {"1", "true", "yes", "on"}
        self.active_position_stale_exit_sec = float(os.getenv("ACTIVE_POSITION_STALE_EXIT_SEC", "45") or 45)

        self.premium_flow = {
            "CE": {"ltp": 0.0, "prev": 0.0, "velocity": 0.0},
            "PE": {"ltp": 0.0, "prev": 0.0, "velocity": 0.0},
            "dominant": None,
            "last_update": 0.0,
        }
        self.last_reselection_ts = 0.0
        self.last_initial_selection_retry_ts = 0.0
        self.reselection_cooldown_sec = 30
        self.last_selected_underlying: Dict[str, float] = {}
        self.reselection_move_threshold = {
            "NIFTY": 40,
            "BANKNIFTY": 100,
            "FINNIFTY": 40,
        }
        self.flow_reversal_state: Dict[int, dict] = {}
        self._tick_exit_guard: Dict[int, dict] = {}
        self.tri_wave_brain = TriWaveTickBrain(debug=True)
        self.tri_wave_v2_brain = TriWaveV2Brain()
        self.tri_wave_recorder = TriWaveSessionRecorder(enabled=True, expiry_key=os.getenv("TRIWAVE_EXPIRY_KEY", "unknown"))
        self.tri_wave_live_analyzer = TriWaveLiveAnalyzer(self.tri_wave_recorder, interval_sec=300)
        self.tri_wave_v2_last_exit_ts: Dict[str, float] = {}
        self.tri_wave_v2_last_exit_reason: Dict[str, str] = {}
        self.tri_wave_v2_last_exit_net_pnl: Dict[str, float] = {}
        self.tri_wave_peak_realized_pnl: Dict[str, float] = defaultdict(float)
        self.tri_wave_trading_halted_for_day: Dict[str, bool] = defaultdict(bool)
        self.tri_wave_data_quarantine_until: Dict[str, float] = defaultdict(float)
        self.tri_wave_data_quarantine_reason: Dict[str, str] = defaultdict(str)
        self.premium_stale_attempt_ts: Dict[str, deque] = defaultdict(lambda: deque(maxlen=12))
        self.premium_rebuild_pending_until: Dict[str, float] = defaultdict(float)
        self.premium_rebuild_generation: int = 0
        self.last_premium_stream_rebuild_ts: float = 0.0
        self.feed_health_log_sec = float(os.getenv("TRIWAVE_FEED_HEALTH_LOG_SEC", "1") or 1)
        self.stale_entry_block_ms = float(os.getenv("TRIWAVE_STALE_ENTRY_BLOCK_MS", "1500") or 1500)
        self.feed_slow_feature_ms = float(os.getenv("TRIWAVE_FEATURE_SLOW_MS", "50") or 50)
        self.feed_slow_decision_ms = float(os.getenv("TRIWAVE_DECISION_SLOW_MS", "50") or 50)
        self._feed_health_last_log_mono = defaultdict(float)
        self._feed_health_current_sec = defaultdict(int)
        self._feed_health_counters = defaultdict(lambda: defaultdict(int))
        self._feed_health_last_queue_size = defaultdict(int)
        self._feed_health_queue_growth = defaultdict(int)
        self._engine_tick_age_ms_by_secid: Dict[int, float] = {}
        self._last_stale_entry_block_log_mono = defaultdict(float)
        self.first_tick_log_enabled = os.getenv("TRIWAVE_FIRST_TICK_LOG_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}
        self._first_index_tick_logged = set()

        self.metrics = {
            "exit_reason_counts": defaultdict(int),
            "hold_time_buckets": defaultdict(int),
            "premature_exits": 0,
            "total_exits": 0,
        }

    def run(self) -> None:
        logger.info("MODE: FUTURE_WS_STREAM + OPTION_WS_STREAM + OPTION_DEPTH_STREAM")
        logger.info(
            "THREAD_HEALTH | stage=startup | active_thread_count=%s | indexes=%s | stale_entry_block_ms=%.1f | static_daily_option_pairs=%s | option_reselection_enabled=%s | pair_stale_reconnect_enabled=%s",
            threading.active_count(),
            ",".join(self.settings.indexes),
            self.stale_entry_block_ms,
            self.static_daily_option_pairs,
            self.option_reselection_enabled,
            self.pair_stale_reconnect_enabled,
        )
        logger.info("TRI_WAVE_BRAIN_VERSION | production_dynamic_exit_v3 | owner_locked=True | legacy_exit_skip=True | weak_exit_removed=True | static_40s_disabled_for_triwave=True")
        if self.TRI_WAVE_V2_ONLY_MODE:
            logger.info("TRI_WAVE_V2_ONLY_MODE_ACTIVE")
            logger.info("LEGACY_SYSTEM_SKIPPED_BY_TRI_WAVE_V2_ONLY")

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

    @staticmethod
    def _diag_from_depth(depth) -> dict:
        return dict(getattr(depth, "diag", None) or {})

    def _feed_key(self, index: str, side: Optional[str], secid: int) -> str:
        return f"{index}:{side or 'UNKNOWN'}:{int(secid)}"

    def _bump_feed_counter(self, key: str, name: str, count: int = 1) -> None:
        sec = int(time.time())
        if self._feed_health_current_sec[key] != sec:
            self._feed_health_current_sec[key] = sec
            self._feed_health_counters[key] = defaultdict(int)
        self._feed_health_counters[key][name] += int(count)

    def _observe_feed_health(
        self,
        *,
        index: str,
        side: Optional[str],
        secid: int,
        source: str,
        diag: Optional[dict] = None,
        feature_duration_ms: Optional[float] = None,
        decision_duration_ms: Optional[float] = None,
        engine_start_mono: Optional[float] = None,
        count_engine_tick: bool = True,
        emit_log: bool = True,
    ) -> float:
        now_mono = time.monotonic()
        diag = dict(diag or {})
        raw_mono = float(diag.get("raw_received_mono") or now_mono)
        parsed_mono = float(diag.get("parsed_mono") or raw_mono)
        engine_mono = float(engine_start_mono or now_mono)
        raw_age_ms = max((now_mono - raw_mono) * 1000.0, 0.0)
        parsed_age_ms = max((now_mono - parsed_mono) * 1000.0, 0.0)
        feature_age_ms = max((now_mono - float(diag.get("feature_mono") or parsed_mono)) * 1000.0, 0.0)
        engine_age_ms = max((now_mono - engine_mono) * 1000.0, 0.0)
        engine_delay_from_raw_ms = max((engine_mono - raw_mono) * 1000.0, 0.0)
        key = self._feed_key(index, side, secid)
        queue_size = int(diag.get("queue_size") or 0)
        previous_queue_size = int(self._feed_health_last_queue_size.get(key, 0) or 0)
        self._feed_health_last_queue_size[key] = queue_size
        self._feed_health_queue_growth[key] += max(queue_size - previous_queue_size, 0)
        self._engine_tick_age_ms_by_secid[int(secid)] = raw_age_ms
        if count_engine_tick:
            self._bump_feed_counter(key, "engine_ticks")

        if emit_log and now_mono - self._feed_health_last_log_mono[key] >= self.feed_health_log_sec:
            self._feed_health_last_log_mono[key] = now_mono
            counters = dict(self._feed_health_counters.get(key, {}))
            logger.info(
                "FEED_HEALTH | index=%s | side=%s | secid=%s | source=%s | connection_id=%s | "
                "raw_age_ms=%.1f | parsed_age_ms=%.1f | feature_age_ms=%.1f | engine_age_ms=%.1f | "
                "engine_delay_from_raw_ms=%.1f | queue_size=%s | queue_max_size=%s | queue_backlog_growth=%s | "
                "ticks_received_per_sec=%s | ticks_parsed_per_sec=%s | ticks_processed_by_engine_per_sec=%s | "
                "feature_extraction_duration_ms=%.1f | decision_engine_duration_ms=%.1f | lock_wait_duration_ms=%.1f | "
                "exception_count=%s | dropped_tick_count=%s | stale_tick_count=%s | active_thread_count=%s",
                index,
                side,
                secid,
                source,
                diag.get("connection_id", "unknown"),
                raw_age_ms,
                parsed_age_ms,
                feature_age_ms,
                engine_age_ms,
                engine_delay_from_raw_ms,
                queue_size,
                diag.get("queue_max_size", "unknown"),
                self._feed_health_queue_growth[key],
                counters.get("raw_ticks", 0),
                counters.get("parsed_ticks", 0),
                counters.get("engine_ticks", 0),
                float(feature_duration_ms or diag.get("feature_duration_ms") or 0.0),
                float(decision_duration_ms or 0.0),
                float(diag.get("lock_wait_duration_ms") or 0.0),
                diag.get("exception_count", 0),
                diag.get("dropped_tick_count", 0),
                counters.get("stale_ticks", 0),
                threading.active_count(),
            )
            self._feed_health_queue_growth[key] = 0
        return raw_age_ms

    def _entry_data_is_fresh_for_engine(self, index: str, side: str, secid: Optional[int], ltp: float) -> bool:
        if not secid:
            return False
        age_ms = self._engine_tick_age_ms_by_secid.get(int(secid))
        if age_ms is None or age_ms <= self.stale_entry_block_ms:
            return True
        key = f"{index}:{side}:{secid}"
        now_mono = time.monotonic()
        self._bump_feed_counter(self._feed_key(index, side, int(secid)), "stale_ticks")
        if now_mono - self._last_stale_entry_block_log_mono[key] >= 1.0:
            self._last_stale_entry_block_log_mono[key] = now_mono
            logger.warning(
                "STALE_DATA_ENTRY_BLOCKED | index=%s | side=%s | secid=%s | ltp=%.2f | "
                "engine_tick_age_ms=%.1f | max_age_ms=%.1f",
                index,
                side,
                secid,
                float(ltp or 0.0),
                float(age_ms),
                self.stale_entry_block_ms,
            )
        return False

    def _all_underlyings_ready(self) -> bool:
        with self._lock:
            return all(self.pairs[index].underlying_ltp for index in self.settings.indexes)

    def _start_option_streams(self) -> None:
        if self._option_streams_started:
            return
        if self.option_quote_stream is None:
            raise RuntimeError("Option quote stream is not configured")
        if self.option_depth_stream is None:
            raise RuntimeError("Option depth stream is not configured")
        try:
            self.option_quote_stream.start()
        except Exception as error:
            self._handle_ws_error(error)
        try:
            self.option_depth_stream.start()
        except Exception as error:
            self._handle_ws_error(error)
        self._option_streams_started = True

    def _select_and_subscribe_option_pairs(self) -> None:
        all_subscriptions = []
        for index in self.settings.indexes:
            underlying_ltp = self.pairs[index].underlying_ltp
            try:
                selection = self.selector.select_best(index, underlying_ltp_override=underlying_ltp)
            except Exception as error:
                logger.error("TRI_WAVE_INITIAL_SELECTION_FAILED | index=%s | error=%s", index, error)
                continue
            if not selection:
                logger.warning("TRI_WAVE_INITIAL_SELECTION_NO_CONTRACT | index=%s | underlying=%.2f", index, float(underlying_ltp or 0.0))
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
                source = str(
                    (selection.get("CE", {}) or {}).get("selection_source")
                    or (selection.get("PE", {}) or {}).get("selection_source")
                    or "OPTION_CHAIN"
                )
                logger.info(
                    "TRI_WAVE_INITIAL_SELECTION_SOURCE | index=%s | source=%s | ce_id=%s | pe_id=%s",
                    index,
                    source,
                    pair.ce_id,
                    pair.pe_id,
                )
                logger.info("PAIR_REGISTERED | %s | CE=%s | PE=%s", index, pair.ce_id, pair.pe_id)

            if subscriptions:
                all_subscriptions.extend(subscriptions)
                for secid, tag in subscriptions:
                    self.full_quote_secid_tag[secid] = tag

        if not all_subscriptions:
            logger.warning("TRI_WAVE_INITIAL_SELECTION_EMPTY | option_streams=NOT_STARTED")
            return

        logger.info(
            "TRI_WAVE_OPTION_STREAM_START_ORDER | step=after_future_ltp_and_pair_selection | subscriptions=%s",
            len(all_subscriptions),
        )
        self._start_option_streams()
        if hasattr(self.option_quote_stream, "replace_subscriptions"):
            self.option_quote_stream.replace_subscriptions(all_subscriptions, reason="initial_option_pair_selection")
        else:
            self.option_quote_stream.subscribe(all_subscriptions)
        self.option_depth_stream.subscribe(all_subscriptions)

    def _retry_missing_option_selection_if_needed(self, trigger_index: str) -> None:
        if not self.market_open():
            return
        missing_indexes = [
            index for index, pair in self.pairs.items()
            if not (pair.ce_id and pair.pe_id) and float(pair.underlying_ltp or 0.0) > 0.0
        ]
        if not missing_indexes:
            return
        now = time.time()
        if now - self.last_initial_selection_retry_ts < self.option_selection_retry_sec:
            return
        self.last_initial_selection_retry_ts = now
        logger.info(
            "TRI_WAVE_OPTION_SELECTION_RETRY | trigger=%s | missing=%s | reason=missing_option_pair_after_future_live",
            trigger_index,
            ",".join(missing_indexes),
        )
        self._select_and_subscribe_option_pairs()

    @staticmethod
    def _json_safe(value):
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, dict):
            return {str(key): TradingRuntimeCoordinator._json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [TradingRuntimeCoordinator._json_safe(item) for item in value]
        return str(value)

    def _log_first_index_tick(self, *, stream: str, index: str, side: Optional[str], secid: int, tag: str, ltp: float, payload: dict) -> None:
        if not self.first_tick_log_enabled:
            return
        key = f"{stream}:{index}:{side or 'NA'}:{int(secid)}"
        if key in self._first_index_tick_logged:
            return
        self._first_index_tick_logged.add(key)
        merged = {
            "segment": "index",
            "stream": stream,
            "index": index,
            "side": side,
            "secid": int(secid),
            "tag": str(tag),
            "ltp": float(ltp),
        }
        merged.update(dict(payload or {}))
        logger.info(
            "TRI_WAVE_FIRST_FULL_TICK | stream=%s | index=%s | side=%s | secid=%s | payload=%s",
            stream,
            index,
            side or "NA",
            int(secid),
            json.dumps(self._json_safe(merged), sort_keys=True, separators=(",", ":")),
        )

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
            features = getattr(depth, "features", None) or {}
            raw_full = getattr(depth, "raw", None) or {}
            self.latest_full_features_by_secid[int(secid)] = dict(features)
            self.latest_full_raw_by_secid[int(secid)] = dict(raw_full)
            self._log_first_index_tick(
                stream="FUTURE_FULL",
                index=index,
                side=None,
                secid=int(secid),
                tag=str(tag),
                ltp=float(ltp),
                payload={
                    "depth": {
                        "bid_price": list(getattr(depth, "bid_price", []) or []),
                        "bid_qty": list(getattr(depth, "bid_qty", []) or []),
                        "ask_price": list(getattr(depth, "ask_price", []) or []),
                        "ask_qty": list(getattr(depth, "ask_qty", []) or []),
                        "ts": float(getattr(depth, "ts", time.time()) or time.time()),
                    },
                    "features": dict(features),
                    "raw": dict(raw_full),
                    "diag": dict(getattr(depth, "diag", None) or {}),
                },
            )
            if self.TRI_WAVE_V2_ONLY_MODE:
                self.tri_wave_recorder.record_tick(index=index, stream="FUT", secid=int(secid), ltp=float(ltp), features=dict(features or self.pairs[index].underlying_quote or {}))

            if self.TRI_WAVE_V2_ONLY_MODE:
                if self.verbose_tick_logs:
                    if time.time() - getattr(self, "_last_v2_fut_route_log", 0.0) >= 5.0:
                        self._last_v2_fut_route_log = time.time()
                        logger.info(
                            "TRI_WAVE_V2_FULL_FEATURE_ROUTE | stream=FUT | index=%s | secid=%s | ltp=%.2f | feature_keys=%s | recovery=%.2f | clean=%.2f | flow=%.2f | ofi=%.2f | volume_change=%s | oi_change=%s",
                            index, secid, float(ltp), sorted(features.keys()),
                            float(features.get("recovery_score", 0.0) or 0.0),
                            float(features.get("clean_trade_score", 0.0) or 0.0),
                            float(features.get("flow", 0.0) or 0.0),
                            float(features.get("ofi", 0.0) or 0.0),
                            features.get("volume_change_tick"),
                            features.get("oi_change_tick"),
                        )
                self.tri_wave_v2_brain.on_future_tick(index=index, secid=int(secid), ltp=float(ltp), features=features or self.pairs[index].underlying_quote)
            else:
                self.tri_wave_brain.on_future_tick(
                    index=index,
                    secid=int(secid),
                    ltp=float(ltp),
                    quote=self.pairs[index].underlying_quote,
                )
            if self._all_underlyings_ready():
                self._future_ready.set()
            self._retry_missing_option_selection_if_needed(index)
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
        if self.static_daily_option_pairs or not self.option_reselection_enabled:
            return

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

        try:
            selection = self.selector.select_best(index, underlying_ltp_override=pair.underlying_ltp)
        except Exception as error:
            logger.warning(
                "TRI_WAVE_RESELECT_FAILED_KEEPING_OLD_PAIR | index=%s | old_ce=%s | old_pe=%s | error=%s",
                index, pair.ce_id, pair.pe_id, error
            )
            return
        if not selection:
            logger.warning(
                "TRI_WAVE_RESELECT_NO_SELECTION_KEEPING_OLD_PAIR | index=%s | old_ce=%s | old_pe=%s | underlying=%.2f",
                index, pair.ce_id, pair.pe_id, float(pair.underlying_ltp or 0.0)
            )
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
            for secid, mapped_tag in subscriptions:
                self.full_quote_secid_tag[secid] = mapped_tag
            if self.option_quote_stream is not None:
                self._rebuild_option_quote_stream(index, reason=f"pair_reselected:{index}")

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

    def _mark_option_ltp_fresh(
        self,
        *,
        secid: int,
        ltp: float,
        source: str,
        pair: Optional[PairRuntimeState] = None,
        index: Optional[str] = None,
        side: Optional[str] = None,
        features: Optional[dict] = None,
        update_pair_ltp: bool = False,
    ) -> bool:
        try:
            secid = int(secid)
            ltp = float(ltp or 0.0)
        except (TypeError, ValueError):
            return False
        if ltp <= 0:
            return False

        source = str(source or "UNKNOWN").upper()
        if source.startswith("DEPTH") and self.entry_depth_ltp_fallback_enabled:
            features = features or {}
            bid = float(features.get("bid_price") or features.get("best_bid") or 0.0)
            ask = float(features.get("ask_price") or features.get("best_ask") or 0.0)
            if bid <= 0 or ask <= 0 or ask < bid:
                return False
            spread_pct = ((ask - bid) / max(ltp, 1e-9)) * 100.0
            if spread_pct > self.entry_depth_ltp_max_spread_pct:
                return False
        elif source.startswith("DEPTH"):
            return False

        now_ts = time.time()
        self.option_ltp_last_ts_by_secid[secid] = now_ts
        self.option_ltp_last_value_by_secid[secid] = ltp
        self.option_ltp_source_by_secid[secid] = source
        if update_pair_ltp and pair is not None:
            pair.update_option_ltp(secid, ltp)

        if source.startswith("DEPTH"):
            log_key = f"{index or ''}:{side or ''}:{secid}"
            if now_ts - self._last_depth_ltp_fallback_log_ts[log_key] >= 60:
                self._last_depth_ltp_fallback_log_ts[log_key] = now_ts
                logger.info(
                    "TRI_WAVE_DEPTH_LTP_FRESHNESS_FALLBACK | index=%s | side=%s | secid=%s | ltp=%.2f | source=%s",
                    index,
                    side,
                    secid,
                    ltp,
                    source,
                )
        return True

    def _on_option_full_quote(self, secid: int, tag: str, ltp: float, depth) -> None:
        self._handle_ws_connected()
        lock_wait_start_mono = time.monotonic()
        with self._lock:
            lock_wait_duration_ms = (time.monotonic() - lock_wait_start_mono) * 1000.0
            index = self.option_index_by_secid.get(int(secid))
            if index is None:
                index = str(tag).split("_")[0].upper()
            pair = self.pairs.get(index)
            if pair is None:
                return
            side = "CE" if int(secid) == pair.ce_id else "PE" if int(secid) == pair.pe_id else None
            if side is None:
                logger.info(
                    "TRI_WAVE_STALE_OPTION_PACKET_IGNORED | index=%s | secid=%s | tag=%s | current_ce=%s | current_pe=%s | source=FULL_QUOTE",
                    index,
                    secid,
                    tag,
                    pair.ce_id,
                    pair.pe_id,
                )
                return
            if float(ltp or 0.0) > 0:
                self._mark_option_ltp_fresh(
                    secid=int(secid),
                    ltp=float(ltp),
                    source="FULL_QUOTE",
                    pair=pair,
                    index=index,
                    side=side,
                    update_pair_ltp=True,
                )
                now_ts = time.time()
                quarantine_reason = str(self.tri_wave_data_quarantine_reason.get(index, ""))
                if self.premium_rebuild_pending_until.get(index) or quarantine_reason.startswith("PREMIUM_STREAM"):
                    self._verify_premium_stream_for_index(index, now_ts)

            features = dict(getattr(depth, "features", None) or {})
            raw_full = dict(getattr(depth, "raw", None) or {})
            diag = self._diag_from_depth(depth)
            diag["lock_wait_duration_ms"] = lock_wait_duration_ms
            self.latest_full_features_by_secid[int(secid)] = dict(features)
            self.latest_full_raw_by_secid[int(secid)] = dict(raw_full)

            existing = pair.ce_depth if int(secid) == pair.ce_id else pair.pe_depth if int(secid) == pair.pe_id else None
            merged = {}
            merged.update(features)
            for k, v in (existing or {}).items():
                merged.setdefault(k, v)
            merged["ltp"] = float(ltp)
            merged["secid"] = int(secid)
            merged["tag"] = str(tag)
            merged["ts"] = time.time()
            merged["feature_source"] = "FULL_QUOTE_PRIMARY"
            merged["_diag"] = dict(diag)
            side = "CE" if int(secid) == pair.ce_id else "PE" if int(secid) == pair.pe_id else None
            if side:
                self._log_first_index_tick(
                    stream="OPTION_FULL",
                    index=index,
                    side=side,
                    secid=int(secid),
                    tag=str(tag),
                    ltp=float(ltp),
                    payload={
                        "features": dict(features),
                        "raw": dict(raw_full),
                        "merged": dict(merged),
                        "diag": dict(diag),
                    },
                )
            if side:
                feed_key = self._feed_key(index, side, int(secid))
                self._bump_feed_counter(feed_key, "raw_ticks")
                self._bump_feed_counter(feed_key, "parsed_ticks")
            if self.TRI_WAVE_V2_ONLY_MODE and side:
                self.tri_wave_recorder.record_tick(index=index, stream=side, secid=int(secid), ltp=float(ltp), features=dict(merged))
            self._process_option_update(index, pair, int(secid), str(tag), merged)

    def on_option_depth(self, secid: int, tag: str, bid, ask) -> None:
        if not self.market_open():
            return

        raw_mono = time.monotonic()
        feature_start_mono = time.monotonic()
        raw = self.feature_builder.build(secid, bid, ask)
        feature_done_mono = time.monotonic()
        if not raw:
            return
        feature_duration_ms = (feature_done_mono - feature_start_mono) * 1000.0
        if feature_duration_ms > self.feed_slow_feature_ms:
            logger.warning(
                "FEATURE_EXTRACTION_SLOW | secid=%s | tag=%s | duration_ms=%.1f | threshold_ms=%.1f",
                secid,
                tag,
                feature_duration_ms,
                self.feed_slow_feature_ms,
            )

        secid = int(secid)
        self.latest_depth_features_by_secid[secid] = dict(raw)

        full = self.latest_full_features_by_secid.get(secid, {})
        merged = dict(raw)
        for k, v in (full or {}).items():
            if v is not None:
                merged[k] = v

        merged["secid"] = secid
        merged["tag"] = str(tag)
        merged["ts"] = time.time()
        merged["_diag"] = {
            "connection_id": "DEPTH",
            "raw_received_mono": raw_mono,
            "parsed_mono": feature_done_mono,
            "feature_mono": feature_done_mono,
            "feature_duration_ms": feature_duration_ms,
            "queue_size": 0,
            "queue_max_size": "unbounded",
        }

        index = self.option_index_by_secid.get(int(secid))
        if index is None:
            index = str(tag).split("_")[0].upper()
        pair = self.pairs.get(index)
        if pair is None:
            return

        lock_wait_start_mono = time.monotonic()
        with self._lock:
            lock_wait_duration_ms = (time.monotonic() - lock_wait_start_mono) * 1000.0
            side = "CE" if secid == pair.ce_id else "PE" if secid == pair.pe_id else None
            if side is None:
                logger.info(
                    "TRI_WAVE_STALE_OPTION_PACKET_IGNORED | index=%s | secid=%s | tag=%s | current_ce=%s | current_pe=%s | source=DEPTH",
                    index,
                    secid,
                    tag,
                    pair.ce_id,
                    pair.pe_id,
                )
                return
            feed_key = self._feed_key(index, side, secid)
            self._bump_feed_counter(feed_key, "raw_ticks")
            self._bump_feed_counter(feed_key, "parsed_ticks")
            merged["_diag"]["lock_wait_duration_ms"] = lock_wait_duration_ms
            now_ts = time.time()
            ws_ltp = pair.ce_ltp if secid == pair.ce_id else pair.pe_ltp if secid == pair.pe_id else None
            ws_ltp_ts = float(self.option_ltp_last_ts_by_secid.get(secid, 0.0) or 0.0)
            ws_ltp_source = str(self.option_ltp_source_by_secid.get(secid, "") or "")
            ws_ltp_is_fresh = (
                ws_ltp_source == "FULL_QUOTE"
                and ws_ltp_ts > 0
                and now_ts - ws_ltp_ts <= self.entry_pair_fresh_ltp_max_age_sec
            )
            if ws_ltp and ws_ltp > 0 and ws_ltp_is_fresh:
                merged["ltp"] = float(ws_ltp)
                merged["feature_source"] = "DEPTH_PLUS_FULL"
            else:
                merged["ltp"] = float(raw.get("ltp", 0.0) or 0.0)
                merged["feature_source"] = "DEPTH_LTP_FALLBACK" if self._mark_option_ltp_fresh(
                    secid=secid,
                    ltp=float(merged.get("ltp", 0.0) or 0.0),
                    source=str(raw.get("ltp_source") or "DEPTH_MICROPRICE"),
                    pair=pair,
                    index=index,
                    side=side,
                    features=merged,
                    update_pair_ltp=False,
                ) else "DEPTH_ONLY"
            if self.TRI_WAVE_V2_ONLY_MODE and side:
                self.tri_wave_recorder.record_tick(index=index, stream=side, secid=int(secid), ltp=float(merged.get("ltp", 0.0) or 0.0), features=dict(merged))
            self._log_first_index_tick(
                stream="OPTION_DEPTH_MERGED",
                index=index,
                side=side,
                secid=int(secid),
                tag=str(tag),
                ltp=float(merged.get("ltp", 0.0) or 0.0),
                payload={
                    "raw_depth": dict(raw),
                    "latest_full_features": dict(full or {}),
                    "merged": dict(merged),
                    "diag": dict(merged.get("_diag", {}) or {}),
                },
            )
            pair.update_option_depth(secid, merged)
            self._process_option_update(index, pair, secid, tag, merged)

    def _get_active_position_for_pair(self, pair: PairRuntimeState) -> Optional[dict]:
        if pair.ce_id and pair.ce_id in self.paper_trader.positions:
            p = self.paper_trader.positions[pair.ce_id]
            return {
                "side": "CE",
                "secid": pair.ce_id,
                "tag": f"{pair.index}_CE",
                "entry": float(p.get("entry", 0) or 0),
                "entry_ts": float(p.get("entry_ts", time.time())),
                "ltp": float(p.get("ltp", p.get("entry", 0)) or 0),
                "pnl_pct": ((float(p.get("ltp", p.get("entry", 0)) or 0) - float(p.get("entry", 0) or 0)) / max(float(p.get("entry", 0) or 0), 1e-9)) * 100.0,
            }
        if pair.pe_id and pair.pe_id in self.paper_trader.positions:
            p = self.paper_trader.positions[pair.pe_id]
            return {
                "side": "PE",
                "secid": pair.pe_id,
                "tag": f"{pair.index}_PE",
                "entry": float(p.get("entry", 0) or 0),
                "entry_ts": float(p.get("entry_ts", time.time())),
                "ltp": float(p.get("ltp", p.get("entry", 0)) or 0),
                "pnl_pct": ((float(p.get("ltp", p.get("entry", 0)) or 0) - float(p.get("entry", 0) or 0)) / max(float(p.get("entry", 0) or 0), 1e-9)) * 100.0,
            }
        return None

    def _is_tri_wave_position(self, secid: int) -> bool:
        position = self.paper_trader.positions.get(secid, {})
        if position.get("strategy_owner") == "TRI_WAVE":
            return True

        active_trade = self.momentum_engine.active_trade.get(secid, {}) if hasattr(self.momentum_engine, "active_trade") else {}
        if active_trade.get("strategy_owner") == "TRI_WAVE":
            return True

        reason = str(position.get("entry_reason") or position.get("reason") or "")
        source = str(position.get("entry_reason_source") or "")
        return reason.startswith("TRI_WAVE_") or source.startswith("TRI_WAVE")

    def _register_momentum_trade_from_entry(self, secid: int, ltp: float, raw: Optional[dict] = None, tri_wave_metadata: Optional[dict] = None) -> None:
        raw = raw or {}
        trade = {
            "type": "TRI_WAVE",
            "side": "LONG",
            "entry": float(ltp),
            "ts": time.time(),
            "best_price": float(ltp),
            "worst_price": float(ltp),
            "mfe": 0.0,
            "mae": 0.0,
            "locked_price": None,
            "breakeven_armed": False,
            "profit_lock_armed": False,
            "entry_spread": float(raw.get("spread", 0) or 0),
        }
        trade.update(dict(tri_wave_metadata or {}))
        if hasattr(self.momentum_engine, "register_trade"):
            self.momentum_engine.register_trade(secid, trade)
        else:
            self.momentum_engine.active_trade[secid] = trade

    def _is_option_ltp_fresh(self, index: str, side: str, secid: Optional[int]) -> bool:
        if not secid or self.entry_fresh_ltp_max_age_sec <= 0:
            return bool(secid)
        now = time.time()
        ltp_ts = float(self.option_ltp_last_ts_by_secid.get(int(secid), 0.0) or 0.0)
        ltp_value = float(self.option_ltp_last_value_by_secid.get(int(secid), 0.0) or 0.0)
        age = now - ltp_ts if ltp_ts else None
        if ltp_ts and age is not None and age <= self.entry_fresh_ltp_max_age_sec and ltp_value > 0:
            return True
        log_key = f"{index}:{side}:{secid}"
        if now - self._last_entry_stale_log_ts[log_key] >= 10:
            self._last_entry_stale_log_ts[log_key] = now
            logger.warning(
                "TRI_WAVE_ENTRY_STALE_LTP_BLOCK | index=%s | side=%s | secid=%s | ltp_age=%s | ltp=%.2f | max_age=%.2f",
                index,
                side,
                secid,
                f"{age:.2f}" if age is not None else "missing",
                ltp_value,
                self.entry_fresh_ltp_max_age_sec,
            )
        return False

    def _lot_size_for_index(self, index: str) -> int:
        lot_sizes = getattr(self.paper_trader, "LOT_SIZES", {}) or {}
        return int(lot_sizes.get(index, lot_sizes.get(str(index).upper(), 1)) or 1)

    def _round_trip_fee(self) -> float:
        return float(getattr(self.paper_trader, "ROUND_TRIP_FEE", 60.0) or 60.0)

    def _side_state_stats(self, index: str, side: str) -> dict:
        try:
            stream = self.tri_wave_v2_brain.streams.get(index, {}).get(side)
            return dict(getattr(stream, "stats", {}) or {})
        except Exception:
            return {}

    def _entry_expected_edge(self, index: str, side: str, ltp: float) -> dict:
        stats = self._side_state_stats(index, side)
        return entry_expected_edge(
            stats=stats,
            ltp=float(ltp),
            lot_size=self._lot_size_for_index(index),
            fee=self._round_trip_fee(),
            min_expected_move_pct=self.entry_gate_config.min_expected_move_pct,
        )

    def _set_tri_wave_data_quarantine(self, index: str, reason: str, seconds: float) -> None:
        if seconds <= 0:
            return
        until = time.time() + float(seconds)
        self.tri_wave_data_quarantine_until[index] = max(float(self.tri_wave_data_quarantine_until.get(index, 0.0) or 0.0), until)
        self.tri_wave_data_quarantine_reason[index] = str(reason)

    def _replace_tri_wave_data_quarantine(self, index: str, reason: str, seconds: float) -> None:
        until = time.time() + max(float(seconds), 0.0)
        self.tri_wave_data_quarantine_until[index] = until
        self.tri_wave_data_quarantine_reason[index] = str(reason)

    def _clear_tri_wave_data_quarantine(self, index: str, reason_prefix: str) -> None:
        reason = str(self.tri_wave_data_quarantine_reason.get(index, ""))
        if reason.startswith(reason_prefix):
            self.tri_wave_data_quarantine_until[index] = 0.0
            self.tri_wave_data_quarantine_reason[index] = ""
            self._recovery_fresh_since_ts[index] = 0.0

    def _maybe_clear_pair_stale_recovery_quarantine(
        self,
        index: str,
        pair: PairRuntimeState,
        now: Optional[float] = None,
    ) -> bool:
        now = time.time() if now is None else now
        reason = str(self.tri_wave_data_quarantine_reason.get(index, ""))
        quarantine_until = float(self.tri_wave_data_quarantine_until.get(index, 0.0) or 0.0)
        if reason != "PAIR_STALE_RECOVERY" or quarantine_until <= now:
            self._recovery_fresh_since_ts[index] = 0.0
            return False

        freshness = PairFreshness(
            ce_age=self._option_ltp_age(pair.ce_id, now),
            pe_age=self._option_ltp_age(pair.pe_id, now),
            max_age_sec=self.entry_pair_fresh_ltp_max_age_sec,
        )
        if not freshness.is_fresh:
            self._recovery_fresh_since_ts[index] = 0.0
            return False

        fresh_since = float(self._recovery_fresh_since_ts.get(index, 0.0) or 0.0)
        if fresh_since <= 0:
            self._recovery_fresh_since_ts[index] = now
            return False
        if now - fresh_since < self.pair_stale_recovery_clear_fresh_sec:
            return False

        self._clear_tri_wave_data_quarantine(index, "PAIR_STALE_RECOVERY")
        logger.info(
            "TRI_WAVE_PAIR_STALE_RECOVERY_CLEARED | index=%s | ce_age=%s | pe_age=%s | fresh_for=%.1fs",
            index,
            self._fmt_age(freshness.ce_age),
            self._fmt_age(freshness.pe_age),
            now - fresh_since,
        )
        return True

    def _tri_wave_entry_quality_gate(self, pair: PairRuntimeState, side: str, secid: Optional[int], ltp: float, signal) -> bool:
        index = pair.index
        now = time.time()
        self._maybe_clear_pair_stale_recovery_quarantine(index, pair, now)
        quarantine_until = float(self.tri_wave_data_quarantine_until.get(index, 0.0) or 0.0)
        if quarantine_until > now:
            self._log_tri_wave_entry_quality_block(
                index,
                side,
                "DATA_QUARANTINE",
                secid=secid,
                ltp=ltp,
                remaining=quarantine_until - now,
                quarantine_reason=self.tri_wave_data_quarantine_reason.get(index, "UNKNOWN"),
            )
            return False

        if not self._entry_data_is_fresh_for_engine(index, side, secid, ltp):
            return False

        if not self._is_option_ltp_fresh(index, side, secid):
            return False

        freshness = PairFreshness(
            ce_age=self._option_ltp_age(pair.ce_id, now),
            pe_age=self._option_ltp_age(pair.pe_id, now),
            max_age_sec=self.entry_pair_fresh_ltp_max_age_sec,
        )
        if not freshness.is_fresh:
            self._log_tri_wave_entry_quality_block(
                index,
                side,
                "PAIR_LTP_NOT_FRESH",
                secid=secid,
                ltp=ltp,
                ce_age=self._fmt_age(freshness.ce_age),
                pe_age=self._fmt_age(freshness.pe_age),
                max_age=self.entry_pair_fresh_ltp_max_age_sec,
            )
            return False

        stats = self._side_state_stats(index, side)
        phase = str(stats.get("phase") or "")
        if not phase:
            try:
                stream = self.tri_wave_v2_brain.streams.get(index, {}).get(side)
                phase = str(getattr(stream, "phase", "UNKNOWN"))
            except Exception:
                phase = "UNKNOWN"

        decision = evaluate_entry_quality(
            stats=stats,
            ltp=float(ltp),
            lot_size=self._lot_size_for_index(index),
            fee=self._round_trip_fee(),
            phase=phase,
            config=self.entry_gate_config,
        )
        if not decision.allowed:
            self._log_tri_wave_entry_quality_block(
                index,
                side,
                decision.reason,
                secid=secid,
                ltp=ltp,
                **(decision.fields or {}),
            )
            return False

        return True

    def _log_tri_wave_entry_quality_block(self, index: str, side: str, reason: str, **fields) -> None:
        now = time.time()
        key = f"{index}:{side}:{reason}"
        if now - self._last_entry_quality_log_ts[key] < 10:
            return
        self._last_entry_quality_log_ts[key] = now
        details = " | ".join(f"{name}={value}" for name, value in fields.items())
        logger.info(
            "TRI_WAVE_ENTRY_QUALITY_BLOCK | index=%s | side=%s | reason=%s%s%s",
            index,
            side,
            reason,
            " | " if details else "",
            details,
        )
        self._log_tri_wave_route_blocked(index, f"BUY_{side}", reason, **fields)

    def _log_tri_wave_route_blocked(self, index: str, action: str, reason: str, **fields) -> None:
        now = time.time()
        key = f"{index}:{action}:{reason}"
        if now - self._last_entry_quality_log_ts[key] < 10:
            return
        self._last_entry_quality_log_ts[key] = now
        details = " | ".join(f"{name}={value}" for name, value in fields.items())
        logger.info(
            "TRI_WAVE_ROUTE_BLOCKED | index=%s | action=%s | reason=%s%s%s",
            index,
            action,
            reason,
            " | " if details else "",
            details,
        )

    def _try_tri_wave_scale_in(self, pair: PairRuntimeState, side: str, secid: Optional[int], ltp: float, signal, raw: dict, tri_meta: dict) -> bool:
        if not self.scale_in_enabled or not secid or secid not in self.paper_trader.positions:
            return False

        position = self.paper_trader.positions.get(secid, {})
        now = time.time()
        ltp_ts = float(self.option_ltp_last_ts_by_secid.get(int(secid), 0.0) or 0.0)
        ltp_age = now - ltp_ts if ltp_ts else None

        stats = self._side_state_stats(pair.index, side)
        decision = evaluate_scale_in_quality(
            position=position,
            ltp=float(ltp),
            stats=stats,
            ltp_age=ltp_age,
            now_ts=now,
            config=self.scale_in_gate_config,
        )
        if not decision.allowed:
            fields = dict(decision.fields or {})
            if "ltp_age" in fields:
                fields["ltp_age"] = self._fmt_age(fields["ltp_age"])
            self._log_tri_wave_entry_quality_block(
                pair.index,
                side,
                decision.reason,
                secid=secid,
                **fields,
            )
            return False

        tag = f"{pair.index}_{side}"
        accepted = self.paper_trader.on_entry(
            int(secid),
            tag,
            "LONG",
            float(ltp),
            lots=1,
            reason=f"TRI_WAVE_SCALE_IN:{signal.reason}",
            metadata=tri_meta,
        )
        if accepted:
            logger.info(
                "TRI_WAVE_SCALE_IN_COMMITTED | %s | ltp=%.2f | lots=%s | pnl_pct=%.2f | support=%.2f | risk=%.2f | edge=%.2f | reason=%s",
                tag,
                float(ltp),
                int(self.paper_trader.positions.get(int(secid), {}).get("lots", position.get("lots", 0))),
                float((decision.fields or {}).get("pnl_pct", 0.0) or 0.0),
                float((decision.fields or {}).get("support", 0.0) or 0.0),
                float((decision.fields or {}).get("risk", 0.0) or 0.0),
                float((decision.fields or {}).get("edge", 0.0) or 0.0),
                signal.reason,
            )
            self._record_tri_wave_portfolio_snapshot(pair.index)
        return bool(accepted)

    def _execute_tri_wave_signal(self, pair: PairRuntimeState, signal, raw: dict) -> bool:
        action = signal.action
        if action == "NO_TRADE":
            return False
        if action in {"BUY_CE", "BUY_PE", "FLIP_TO_CE", "FLIP_TO_PE"}:
            if self._should_block_tri_wave_entry(pair.index, action):
                return False

        logger.info(
            "TRI_WAVE_ROUTE | index=%s | action=%s | side=%s | conf=%.2f | reason=%s",
            pair.index, action, signal.side, signal.confidence, signal.reason
        )

        tri_meta = {
            "strategy_owner": "TRI_WAVE_V2" if self.TRI_WAVE_V2_ONLY_MODE else "TRI_WAVE",
            "entry_reason_source": "TRI_WAVE",
            "tri_wave_action": action,
            "tri_wave_confidence": signal.confidence,
            "tri_wave_reason": signal.reason,
        }

        if action == "BUY_CE":
            secid = pair.ce_id
            tag = f"{pair.index}_CE"
            ltp = pair._best_leg_ltp(pair.ce_depth, pair.ce_ltp, raw.get("ltp", 0))
            if secid in self.paper_trader.positions:
                return self._try_tri_wave_scale_in(pair, "CE", secid, float(ltp), signal, raw, tri_meta)
            if not self._tri_wave_entry_quality_gate(pair, "CE", secid, float(ltp), signal):
                return False
            accepted = self.paper_trader.on_entry(secid, tag, "LONG", float(ltp), lots=1, reason=signal.reason, metadata=tri_meta)
            if accepted:
                self._register_momentum_trade_from_entry(secid, float(ltp), pair.ce_depth or raw, tri_wave_metadata=tri_meta)
                if self.TRI_WAVE_V2_ONLY_MODE:
                    self.tri_wave_v2_brain.reset_trade_state(pair.index, "CE", float(ltp))
                elif hasattr(self.tri_wave_brain, "reset_trade_state"):
                    self.tri_wave_brain.reset_trade_state(pair.index, "CE", float(ltp))
                    logger.info("TRI_WAVE_TRADE_STATE_RESET | index=%s | side=%s | entry=%.2f", pair.index, "CE", float(ltp))
                logger.info("TRI_WAVE_ENTRY_COMMITTED | %s | ltp=%.2f | reason=%s", tag, float(ltp), signal.reason)
                self._record_tri_wave_portfolio_snapshot(pair.index)
            else:
                self._log_tri_wave_route_blocked(
                    pair.index,
                    action,
                    "PAPER_ENTRY_REJECTED",
                    secid=secid,
                    ltp=round(float(ltp), 2),
                    open_positions=len(self.paper_trader.positions),
                )
            return bool(accepted)

        if action == "BUY_PE":
            secid = pair.pe_id
            tag = f"{pair.index}_PE"
            ltp = pair._best_leg_ltp(pair.pe_depth, pair.pe_ltp, raw.get("ltp", 0))
            if secid in self.paper_trader.positions:
                return self._try_tri_wave_scale_in(pair, "PE", secid, float(ltp), signal, raw, tri_meta)
            if not self._tri_wave_entry_quality_gate(pair, "PE", secid, float(ltp), signal):
                return False
            accepted = self.paper_trader.on_entry(secid, tag, "LONG", float(ltp), lots=1, reason=signal.reason, metadata=tri_meta)
            if accepted:
                self._register_momentum_trade_from_entry(secid, float(ltp), pair.pe_depth or raw, tri_wave_metadata=tri_meta)
                if self.TRI_WAVE_V2_ONLY_MODE:
                    self.tri_wave_v2_brain.reset_trade_state(pair.index, "PE", float(ltp))
                elif hasattr(self.tri_wave_brain, "reset_trade_state"):
                    self.tri_wave_brain.reset_trade_state(pair.index, "PE", float(ltp))
                    logger.info("TRI_WAVE_TRADE_STATE_RESET | index=%s | side=%s | entry=%.2f", pair.index, "PE", float(ltp))
                logger.info("TRI_WAVE_ENTRY_COMMITTED | %s | ltp=%.2f | reason=%s", tag, float(ltp), signal.reason)
                self._record_tri_wave_portfolio_snapshot(pair.index)
            else:
                self._log_tri_wave_route_blocked(
                    pair.index,
                    action,
                    "PAPER_ENTRY_REJECTED",
                    secid=secid,
                    ltp=round(float(ltp), 2),
                    open_positions=len(self.paper_trader.positions),
                )
            return bool(accepted)

        if action == "EXIT_CE":
            secid = pair.ce_id
            ltp = pair._best_leg_ltp(pair.ce_depth, pair.ce_ltp, raw.get("ltp", 0))
            return bool(secid and self._tri_wave_direct_exit(pair, secid, float(ltp), signal.reason))

        if action == "EXIT_PE":
            secid = pair.pe_id
            ltp = pair._best_leg_ltp(pair.pe_depth, pair.pe_ltp, raw.get("ltp", 0))
            return bool(secid and self._tri_wave_direct_exit(pair, secid, float(ltp), signal.reason))

        if action == "FLIP_TO_CE":
            old_secid = pair.pe_id
            old_tag = f"{pair.index}_PE"
            old_ltp = pair._best_leg_ltp(pair.pe_depth, pair.pe_ltp, raw.get("ltp", 0))
            if old_secid and old_secid in self.paper_trader.positions and not self._attempt_exit_once(pair, old_secid, old_tag, float(old_ltp), f"TRI_WAVE_FLIP_EXIT:{signal.reason}"):
                return False
            new_ltp = pair._best_leg_ltp(pair.ce_depth, pair.ce_ltp, raw.get("ltp", 0))
            if self.paper_trader.has_open_position() or not self._tri_wave_entry_quality_gate(pair, "CE", pair.ce_id, float(new_ltp), signal):
                return False
            accepted = self.paper_trader.on_entry(pair.ce_id, f"{pair.index}_CE", "LONG", float(new_ltp), lots=1, reason=f"TRI_WAVE_FLIP_ENTRY:{signal.reason}", metadata=tri_meta)
            if accepted:
                self._register_momentum_trade_from_entry(pair.ce_id, float(new_ltp), pair.ce_depth or raw, tri_wave_metadata=tri_meta)
            return bool(accepted)

        if action == "FLIP_TO_PE":
            old_secid = pair.ce_id
            old_tag = f"{pair.index}_CE"
            old_ltp = pair._best_leg_ltp(pair.ce_depth, pair.ce_ltp, raw.get("ltp", 0))
            if old_secid and old_secid in self.paper_trader.positions and not self._attempt_exit_once(pair, old_secid, old_tag, float(old_ltp), f"TRI_WAVE_FLIP_EXIT:{signal.reason}"):
                return False
            new_ltp = pair._best_leg_ltp(pair.pe_depth, pair.pe_ltp, raw.get("ltp", 0))
            if self.paper_trader.has_open_position() or not self._tri_wave_entry_quality_gate(pair, "PE", pair.pe_id, float(new_ltp), signal):
                return False
            accepted = self.paper_trader.on_entry(pair.pe_id, f"{pair.index}_PE", "LONG", float(new_ltp), lots=1, reason=f"TRI_WAVE_FLIP_ENTRY:{signal.reason}", metadata=tri_meta)
            if accepted:
                self._register_momentum_trade_from_entry(pair.pe_id, float(new_ltp), pair.pe_depth or raw, tri_wave_metadata=tri_meta)
            return bool(accepted)

        return False

    def _tri_wave_direct_exit(self, pair: PairRuntimeState, secid: int, ltp: float, reason: str) -> bool:
        active = self.paper_trader.positions.get(secid)
        if not active:
            return False
        tag = active.get("tag", f"{pair.index}_{'CE' if secid == pair.ce_id else 'PE'}")
        final_reason = reason if str(reason).startswith("TRI_WAVE_V2_EXIT:") else f"TRI_WAVE_EXIT:{reason}"
        hold_sec = max(time.time() - float(active.get("entry_ts", time.time())), 0.0)
        self.paper_trader.on_exit(secid, float(ltp), reason=final_reason)
        try:
            summary = dict(getattr(self.paper_trader, "last_trade_summary", {}) or {})
            if summary:
                summary.setdefault("index", pair.index)
                summary.setdefault("secid", secid)
                summary.setdefault("exit_reason", final_reason)
                self.tri_wave_recorder.record_trade(summary)
        except Exception:
            logger.exception("TRI_WAVE_RECORDER_TRADE_ERROR | index=%s | secid=%s", pair.index, secid)
        self.flow_reversal_state.pop(secid, None)
        if hasattr(self.momentum_engine, "clear_trade"):
            self.momentum_engine.clear_trade(secid, final_reason)
        else:
            self.momentum_engine.active_trade.pop(secid, None)
        self._record_exit_metrics(reason=final_reason, hold_sec=hold_sec, secid=secid, tag=str(tag))
        self._record_tri_wave_exit(pair.index, final_reason)
        self._record_tri_wave_portfolio_snapshot(pair.index)
        if self.TRI_WAVE_V2_ONLY_MODE:
            self.tri_wave_v2_brain.clear_trade_state(pair.index)
        return True

    def _record_tri_wave_portfolio_snapshot(self, index: str) -> None:
        try:
            now_ts = time.time()
            positions = []
            unrealized = 0.0
            premium_deployed = 0.0
            for secid, pos in (getattr(self.paper_trader, "positions", {}) or {}).items():
                entry = float(pos.get("entry", 0.0) or 0.0)
                ltp = float(pos.get("ltp", entry) or entry)
                qty = int(pos.get("qty", 0) or 0)
                side = str(pos.get("side", "LONG"))
                pnl = (ltp - entry) * qty if side == "LONG" else (entry - ltp) * qty
                premium_deployed += entry * qty
                unrealized += pnl
                entry_ts = float(pos.get("entry_ts", now_ts) or now_ts)
                positions.append({
                    "secid": int(secid),
                    "tag": pos.get("tag"),
                    "side": side,
                    "lots": pos.get("lots"),
                    "qty": qty,
                    "entry": entry,
                    "ltp": ltp,
                    "pnl": pnl,
                    "pnl_pct": ((ltp - entry) / entry) * 100 if entry > 0 else 0.0,
                    "entry_ts": entry_ts,
                    "last_tick_ts": float(pos.get("last_tick_ts", entry_ts) or entry_ts),
                    "hold_sec": max(now_ts - entry_ts, 0.0),
                    "entry_reason": pos.get("entry_reason"),
                    "strategy_owner": pos.get("strategy_owner"),
                })
            realized = float(getattr(self.paper_trader, "realized_pnl", 0.0) or 0.0)
            daily_realized = realized - float(getattr(self.paper_trader, "daily_realized_start", 0.0) or 0.0)
            snapshot = {
                "index": index,
                "capital": getattr(self.paper_trader, "capital", None),
                "initial_capital": getattr(self.paper_trader, "initial_capital", None),
                "cash": getattr(self.paper_trader, "cash", None),
                "realized_pnl": getattr(self.paper_trader, "realized_pnl", None),
                "unrealized_pnl": unrealized,
                "net_pnl": realized + unrealized,
                "daily_realized_pnl": daily_realized,
                "daily_unrealized_pnl": unrealized,
                "daily_net_pnl": daily_realized + unrealized,
                "premium_deployed": premium_deployed,
                "fees_paid": getattr(self.paper_trader, "fees_paid_today", getattr(self.paper_trader, "fees_paid", None)),
                "total_fees": getattr(self.paper_trader, "total_fees", None),
                "opened_today": getattr(self.paper_trader, "opened_today", None),
                "closed_today": getattr(self.paper_trader, "closed_today", None),
                "open_positions": len(getattr(self.paper_trader, "positions", {}) or {}),
                "positions": positions,
                "runtime": "tri_wave_paper",
                "strategy": "tri_wave_v2",
            }
            self.tri_wave_recorder.record_portfolio(snapshot)
            self.paper_trader.trade_summary_sink.record_portfolio("index", snapshot)
        except Exception:
            logger.exception("TRI_WAVE_RECORDER_PORTFOLIO_ERROR | index=%s", index)

    def _maybe_record_tri_wave_portfolio_snapshot(self, index: str) -> None:
        now_ts = time.time()
        if now_ts - self._last_portfolio_snapshot_ts < self.portfolio_snapshot_interval_sec:
            return
        if not getattr(self.paper_trader, "positions", None):
            return
        self._last_portfolio_snapshot_ts = now_ts
        self._record_tri_wave_portfolio_snapshot(index)

    def _record_tri_wave_exit(self, index: str, reason: str) -> None:
        self.tri_wave_v2_last_exit_ts[index] = time.time()
        self.tri_wave_v2_last_exit_reason[index] = str(reason)
        net_pnl = float((self.paper_trader.last_trade_summary or {}).get("net_pnl", 0.0) or 0.0)
        self.tri_wave_v2_last_exit_net_pnl[index] = net_pnl
        if "STALE_MARKET_DATA" in str(reason).upper():
            self._set_tri_wave_data_quarantine(index, "STALE_EXIT", self.stale_exit_quarantine_sec)

    def _should_block_tri_wave_entry(self, index: str, action: str) -> bool:
        now_ts = time.time()
        self.paper_trader._maybe_reset_daily_counts(now_ts)
        current_pnl = float(getattr(self.paper_trader, "realized_pnl", 0.0) or 0.0)
        opened_today = int(getattr(self.paper_trader, "opened_today", 0) or 0)

        if self.TRI_WAVE_PEAK_PROFIT_TRACKING:
            peak = float(self.tri_wave_peak_realized_pnl.get(index, 0.0))
            if current_pnl > peak:
                peak = current_pnl
                self.tri_wave_peak_realized_pnl[index] = peak
            if peak >= 700 and current_pnl <= (peak - self.TRI_WAVE_PROFIT_DECAY_FROM_PEAK):
                self.tri_wave_trading_halted_for_day[index] = True
                logger.info(
                    "TRI_WAVE_PROFIT_DECAY_BLOCK | peak_pnl=%.2f | current_pnl=%.2f | decay=%.2f",
                    peak, current_pnl, peak - current_pnl
                )

        guard_reason = None
        if self.tri_wave_trading_halted_for_day.get(index):
            guard_reason = "PROFIT_DECAY_LOCK"
        elif current_pnl >= self.TRI_WAVE_DAILY_PROFIT_LOCK:
            guard_reason = "DAILY_PROFIT_LOCK"
            self.tri_wave_trading_halted_for_day[index] = True
        elif current_pnl <= self.TRI_WAVE_DAILY_MAX_LOSS:
            guard_reason = "DAILY_MAX_LOSS"
            self.tri_wave_trading_halted_for_day[index] = True
        elif opened_today >= self.TRI_WAVE_MAX_TRADES_PER_DAY:
            guard_reason = "MAX_TRADES_PER_DAY"
            self.tri_wave_trading_halted_for_day[index] = True

        if guard_reason:
            logger.info(
                "TRI_WAVE_DAILY_GUARD_BLOCK | index=%s | reason=%s | net_pnl_after_fees=%.2f | opened_today=%s",
                index, guard_reason, current_pnl, opened_today
            )
            self._log_tri_wave_route_blocked(
                index,
                action,
                guard_reason,
                net_pnl_after_fees=round(current_pnl, 2),
                opened_today=opened_today,
            )
            return True

        fees_paid = float(getattr(self.paper_trader, "fees_paid_today", 0.0) or getattr(self.paper_trader, "fees_paid", 0.0) or 0.0)
        if (fees_paid >= 900 and current_pnl <= 0) or (opened_today >= 15 and current_pnl <= 0):
            logger.info(
                "TRI_WAVE_V2_FEE_GUARD_BLOCK | fees_paid=%.2f | opened_today=%s | net_pnl_after_fees=%.2f",
                fees_paid, opened_today, current_pnl
            )
            self._log_tri_wave_route_blocked(
                index,
                action,
                "FEE_GUARD",
                fees_paid=round(fees_paid, 2),
                opened_today=opened_today,
                net_pnl_after_fees=round(current_pnl, 2),
            )
            return True

        last_exit_ts = self.tri_wave_v2_last_exit_ts.get(index)
        if last_exit_ts:
            elapsed = now_ts - float(last_exit_ts)
            last_reason = str(self.tri_wave_v2_last_exit_reason.get(index, "UNKNOWN"))
            last_net = float(self.tri_wave_v2_last_exit_net_pnl.get(index, 0.0))
            required = self.TRI_WAVE_REENTRY_COOLDOWN_SEC
            if last_net > 0:
                required = self.TRI_WAVE_PROFIT_REENTRY_COOLDOWN_SEC
            elif last_net <= 0:
                required = self.TRI_WAVE_LOSS_REENTRY_COOLDOWN_SEC
            reason_u = last_reason.upper()
            if last_net <= 0 and ("PROFIT" in reason_u or "GIVEBACK" in reason_u or "EXHAUSTION" in reason_u):
                required = max(required, 120)
            if elapsed < required:
                if now_ts - self._last_entry_cooldown_log_ts[index] >= 10:
                    self._last_entry_cooldown_log_ts[index] = now_ts
                    logger.info(
                        "TRI_WAVE_V2_ENTRY_COOLDOWN_BLOCK | index=%s | elapsed=%.2f | required=%.2f | last_exit_reason=%s | last_net_pnl=%.2f",
                        index, elapsed, required, last_reason, last_net
                    )
                    self._log_tri_wave_route_blocked(
                        index,
                        action,
                        "ENTRY_COOLDOWN",
                        elapsed=round(elapsed, 2),
                        required=round(required, 2),
                        last_exit_reason=last_reason,
                        last_net_pnl=round(last_net, 2),
                    )
                return True
        return False

    def _tri_wave_cooldown_status(self, index: str, now_ts: Optional[float] = None) -> dict:
        now_ts = time.time() if now_ts is None else now_ts
        last_exit_ts = self.tri_wave_v2_last_exit_ts.get(index)
        if not last_exit_ts:
            return {"active": False, "remaining": 0.0, "required": 0.0, "last_reason": "NONE", "last_net": 0.0}

        elapsed = now_ts - float(last_exit_ts)
        last_reason = str(self.tri_wave_v2_last_exit_reason.get(index, "UNKNOWN"))
        last_net = float(self.tri_wave_v2_last_exit_net_pnl.get(index, 0.0))
        required = self.TRI_WAVE_REENTRY_COOLDOWN_SEC
        if last_net > 0:
            required = self.TRI_WAVE_PROFIT_REENTRY_COOLDOWN_SEC
        elif last_net <= 0:
            required = self.TRI_WAVE_LOSS_REENTRY_COOLDOWN_SEC
        reason_u = last_reason.upper()
        if last_net <= 0 and ("PROFIT" in reason_u or "GIVEBACK" in reason_u or "EXHAUSTION" in reason_u):
            required = max(required, 120)

        remaining = max(0.0, required - elapsed)
        return {
            "active": remaining > 0.0,
            "remaining": remaining,
            "required": float(required),
            "last_reason": last_reason,
            "last_net": last_net,
        }

    def _tri_wave_daily_guard_status(self, index: str, current_pnl: float, opened_today: int) -> str:
        if self.tri_wave_trading_halted_for_day.get(index):
            return "PROFIT_DECAY_LOCK"
        if current_pnl >= self.TRI_WAVE_DAILY_PROFIT_LOCK:
            return "DAILY_PROFIT_LOCK"
        if current_pnl <= self.TRI_WAVE_DAILY_MAX_LOSS:
            return "DAILY_MAX_LOSS"
        if opened_today >= self.TRI_WAVE_MAX_TRADES_PER_DAY:
            return "MAX_TRADES_PER_DAY"

        fees_paid = float(getattr(self.paper_trader, "fees_paid_today", 0.0) or getattr(self.paper_trader, "fees_paid", 0.0) or 0.0)
        if (fees_paid >= 900 and current_pnl <= 0) or (opened_today >= 15 and current_pnl <= 0):
            return "FEE_GUARD"
        return "OK"

    def _option_ltp_age(self, secid: Optional[int], now_ts: Optional[float] = None) -> Optional[float]:
        if not secid:
            return None
        now_ts = time.time() if now_ts is None else now_ts
        return age_from_ts(float(self.option_ltp_last_ts_by_secid.get(int(secid), 0.0) or 0.0), now_ts)

    def _fmt_age(self, value: Optional[float]) -> str:
        return format_age(value)

    def _log_no_entry_watch_if_needed(self, now_ts: Optional[float] = None) -> None:
        if self.no_entry_watch_sec <= 0:
            return
        now_ts = time.time() if now_ts is None else now_ts
        if now_ts - self._last_no_entry_watch_ts < self.no_entry_watch_sec:
            return

        positions = getattr(self.paper_trader, "positions", {}) or {}
        if positions:
            return

        self._last_no_entry_watch_ts = now_ts
        try:
            self.paper_trader._maybe_reset_daily_counts(now_ts)
        except Exception:
            logger.exception("TRI_WAVE_NO_ENTRY_WATCH_DAILY_RESET_ERROR")

        current_pnl = float(getattr(self.paper_trader, "realized_pnl", 0.0) or 0.0)
        opened_today = int(getattr(self.paper_trader, "opened_today", 0) or 0)
        closed_today = int(getattr(self.paper_trader, "closed_today", 0) or 0)
        profile_states = []

        for index, pair in self.pairs.items():
            self._maybe_clear_pair_stale_recovery_quarantine(index, pair, now_ts)
            ce_age = self._option_ltp_age(pair.ce_id, now_ts)
            pe_age = self._option_ltp_age(pair.pe_id, now_ts)
            cooldown = self._tri_wave_cooldown_status(index, now_ts)
            guard = self._tri_wave_daily_guard_status(index, current_pnl, opened_today)
            quarantine_remaining = max(0.0, float(self.tri_wave_data_quarantine_until.get(index, 0.0) or 0.0) - now_ts)
            missing = []
            if not pair.underlying_ltp:
                missing.append("underlying")
            if not pair.ce_id:
                missing.append("ce_id")
            if not pair.pe_id:
                missing.append("pe_id")
            if not pair.ce_depth:
                missing.append("ce_depth")
            if not pair.pe_depth:
                missing.append("pe_depth")

            stale_sides = []
            if pair.ce_id and (ce_age is None or ce_age > self.entry_fresh_ltp_max_age_sec):
                stale_sides.append("CE")
            if pair.pe_id and (pe_age is None or pe_age > self.entry_fresh_ltp_max_age_sec):
                stale_sides.append("PE")

            if guard != "OK":
                state = f"guard={guard}"
            elif quarantine_remaining > 0:
                state = f"data_quarantine={quarantine_remaining:.1f}s:{self.tri_wave_data_quarantine_reason.get(index, 'UNKNOWN')}"
            elif cooldown["active"]:
                state = f"cooldown={cooldown['remaining']:.1f}s"
            elif missing:
                state = "missing=" + ",".join(missing)
            elif stale_sides:
                state = "stale_ltp=" + ",".join(stale_sides)
                logger.warning(
                    "TRI_WAVE_NO_ENTRY_STALE_RECOVERY_TRIGGER | index=%s | stale_sides=%s | ce_ltp_age=%s | pe_ltp_age=%s",
                    index,
                    ",".join(stale_sides),
                    self._fmt_age(ce_age),
                    self._fmt_age(pe_age),
                )
                self._recover_stale_option_pair(index, pair, now_ts)
            else:
                state = "waiting_for_signal"

            profile_states.append(
                (
                    f"{index}:state={state},ready={pair.is_ready()},ce={pair.ce_id},pe={pair.pe_id},"
                    f"ce_ltp_age={self._fmt_age(ce_age)},pe_ltp_age={self._fmt_age(pe_age)},"
                    f"last_exit={cooldown['last_reason']},last_net={cooldown['last_net']:.2f}"
                )
            )

        logger.info(
            "TRI_WAVE_NO_ENTRY_WATCH | open_positions=0 | realized_pnl=%.2f | opened_today=%s | closed_today=%s | profiles=%s",
            current_pnl,
            opened_today,
            closed_today,
            " ; ".join(profile_states),
        )

    def _process_option_update(self, index: str, pair: PairRuntimeState, secid: int, tag: str, raw: dict) -> None:
        if not self.market_open():
            return
        engine_start_mono = time.monotonic()
        diag = dict(raw.get("_diag") or {})
        feature_duration_ms = float(diag.get("feature_duration_ms") or 0.0)
        self.option_last_tick_ts_by_secid[int(secid)] = time.time()
        self._log_pair_stale_if_needed(index, pair)

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
        self._maybe_record_tri_wave_portfolio_snapshot(pair.index)

        if pair.is_ready() and not pair.ready_logged:
            pair.ready_logged = True
            logger.info("PAIR_STATE_READY | %s | CE+PE+UNDERLYING LIVE", index)

        if side:
            if self.TRI_WAVE_V2_ONLY_MODE:
                log_key = f"{index}:{side}"
                if self.verbose_tick_logs and time.time() - getattr(self, "_last_v2_opt_route_log", {}).get(log_key, 0.0) >= 5.0:
                    self._last_v2_opt_route_log = getattr(self, "_last_v2_opt_route_log", {})
                    self._last_v2_opt_route_log[log_key] = time.time()
                    logger.info(
                        "TRI_WAVE_V2_OPTION_FEATURE_ROUTE | index=%s | side=%s | secid=%s | source=%s | ltp=%.2f | keys=%s | recovery=%.2f | clean=%.2f | exhaustion=%.2f | flow=%.2f | ofi=%.2f | depth_imb=%.2f | market_q_imb=%.2f | volume_change=%s | oi_change=%s",
                        index, side, secid, raw.get("feature_source", "UNKNOWN"), float(raw.get("ltp", 0.0) or 0.0),
                        sorted(raw.keys()), float(raw.get("recovery_score", 0.0) or 0.0), float(raw.get("clean_trade_score", 0.0) or 0.0),
                        float(raw.get("exhaustion_score", 0.0) or 0.0), float(raw.get("flow", 0.0) or 0.0), float(raw.get("ofi", 0.0) or 0.0),
                        float(raw.get("depth_imbalance_5", 0.0) or 0.0), float(raw.get("market_queue_imbalance", 0.0) or 0.0),
                        raw.get("volume_change_tick"), raw.get("oi_change_tick")
                    )
                self.tri_wave_v2_brain.on_option_tick(index=index, side=side, secid=int(secid), ltp=float(raw["ltp"]), features=raw)
            else:
                self.tri_wave_brain.on_option_tick(
                    index=index,
                    side=side,
                    secid=int(secid),
                    ltp=float(raw["ltp"]),
                    features=raw,
                )

        if pair.is_ready():
            active_position = self._get_active_position_for_pair(pair)
            self._observe_feed_health(
                index=index,
                side=side,
                secid=int(secid),
                source=str(raw.get("feature_source") or "UNKNOWN"),
                diag=diag,
                feature_duration_ms=feature_duration_ms,
                decision_duration_ms=0.0,
                engine_start_mono=engine_start_mono,
                count_engine_tick=False,
                emit_log=False,
            )
            decision_start_mono = time.monotonic()
            tri_signal = self.tri_wave_v2_brain.evaluate(pair.index, active_position=active_position) if self.TRI_WAVE_V2_ONLY_MODE else self.tri_wave_brain.evaluate(pair.index, active_position=active_position)
            decision_duration_ms = (time.monotonic() - decision_start_mono) * 1000.0
            if decision_duration_ms > self.feed_slow_decision_ms:
                logger.warning(
                    "DECISION_ENGINE_SLOW | index=%s | side=%s | secid=%s | duration_ms=%.1f | threshold_ms=%.1f",
                    index,
                    side,
                    secid,
                    decision_duration_ms,
                    self.feed_slow_decision_ms,
                )
            self._observe_feed_health(
                index=index,
                side=side,
                secid=int(secid),
                source=str(raw.get("feature_source") or "UNKNOWN"),
                diag=diag,
                feature_duration_ms=feature_duration_ms,
                decision_duration_ms=decision_duration_ms,
                engine_start_mono=engine_start_mono,
            )
            if self.TRI_WAVE_V2_ONLY_MODE:
                try:
                    snapshot = self.tri_wave_v2_brain.get_state_snapshot(pair.index, active_position=active_position)
                    self.tri_wave_recorder.record_state(pair.index, snapshot)
                    self.tri_wave_recorder.record_signal(pair.index, tri_signal)
                    try:
                        self.tri_wave_live_analyzer.maybe_analyze(self.paper_trader)
                    except Exception:
                        logger.exception("TRI_WAVE_LIVE_ANALYZER_ERROR")
                except Exception:
                    logger.exception("TRI_WAVE_RECORDER_SIGNAL_STATE_ERROR | index=%s", pair.index)
            if self.TRI_WAVE_ONLY_MODE or self.TRI_WAVE_V2_ONLY_MODE:
                if tri_signal and tri_signal.action != "NO_TRADE":
                    executed = self._execute_tri_wave_signal(pair, tri_signal, raw)
                    if executed:
                        logger.info(
                            "TRI_WAVE_ONLY_EXECUTED | index=%s | action=%s | reason=%s",
                            pair.index, tri_signal.action, tri_signal.reason
                        )
                if self.verbose_tick_logs:
                    logger.info(
                        "LEGACY_SYSTEM_SKIPPED_BY_TRI_WAVE_V2_ONLY | index=%s | secid=%s | tag=%s",
                        pair.index, secid, tag
                    )
                return
            if tri_signal and tri_signal.action != "NO_TRADE":
                executed = self._execute_tri_wave_signal(pair, tri_signal, raw)
                if executed:
                    logger.info(
                        "TRI_WAVE_EXECUTED | index=%s | action=%s | reason=%s",
                        pair.index, tri_signal.action, tri_signal.reason
                    )
                    self._process_exit_engines(pair, secid, tag, raw)
                    return
            self._update_market_snapshot(pair, raw, secid, tag)

    def _log_pair_stale_if_needed(self, index: str, pair: PairRuntimeState) -> None:
        now = time.time()
        if now - self._last_pair_stale_log_ts[index] < 60:
            return

        ce_age = None
        pe_age = None
        ce_ltp_age = None
        pe_ltp_age = None
        if pair.ce_id:
            ts = self.option_last_tick_ts_by_secid.get(pair.ce_id)
            ce_age = now - ts if ts else None
            ltp_ts = self.option_ltp_last_ts_by_secid.get(pair.ce_id)
            ce_ltp_age = now - ltp_ts if ltp_ts else None
        if pair.pe_id:
            ts = self.option_last_tick_ts_by_secid.get(pair.pe_id)
            pe_age = now - ts if ts else None
            ltp_ts = self.option_ltp_last_ts_by_secid.get(pair.pe_id)
            pe_ltp_age = now - ltp_ts if ltp_ts else None

        if (
            (ce_age is not None and ce_age > self.pair_stale_resubscribe_sec)
            or (pe_age is not None and pe_age > self.pair_stale_resubscribe_sec)
            or (ce_ltp_age is not None and ce_ltp_age > self.pair_stale_resubscribe_sec)
            or (pe_ltp_age is not None and pe_ltp_age > self.pair_stale_resubscribe_sec)
        ):
            self._last_pair_stale_log_ts[index] = now
            logger.warning(
                "TRI_WAVE_PAIR_STALE | index=%s | ce_age=%s | pe_age=%s | ce_ltp_age=%s | pe_ltp_age=%s | ce_id=%s | pe_id=%s",
                index, ce_age, pe_age, ce_ltp_age, pe_ltp_age, pair.ce_id, pair.pe_id
            )
            self._exit_active_position_on_stale_leg(index, pair, now, ce_ltp_age, pe_ltp_age)
            self._recover_stale_option_pair(index, pair, now)

    def _exit_active_position_on_stale_leg(
        self,
        index: str,
        pair: PairRuntimeState,
        now: float,
        ce_ltp_age: Optional[float],
        pe_ltp_age: Optional[float],
    ) -> bool:
        if self.active_position_stale_exit_sec <= 0:
            return False
        active = self._get_active_position_for_pair(pair)
        if not active:
            return False

        side = str(active.get("side") or "")
        secid = int(active.get("secid") or 0)
        ltp_age = ce_ltp_age if side == "CE" else pe_ltp_age if side == "PE" else None
        if ltp_age is None or ltp_age < self.active_position_stale_exit_sec:
            return False

        position = self.paper_trader.positions.get(secid, {})
        ltp = float(position.get("ltp", active.get("ltp", active.get("entry", 0.0))) or 0.0)
        if ltp <= 0:
            ltp = pair._best_leg_ltp(
                pair.ce_depth if side == "CE" else pair.pe_depth,
                pair.ce_ltp if side == "CE" else pair.pe_ltp,
                active.get("entry", 0.0),
            )
        logger.warning(
            "TRI_WAVE_ACTIVE_STALE_LEG_EXIT | index=%s | side=%s | secid=%s | ltp_age=%.2f | max_age=%.2f | ltp=%.2f | note=active_position_leg_stopped_updating",
            index,
            side,
            secid,
            float(ltp_age),
            self.active_position_stale_exit_sec,
            float(ltp),
        )
        return self._tri_wave_direct_exit(
            pair,
            secid,
            float(ltp),
            "TRI_WAVE_V2_EXIT:ACTIVE_SIDE_STALE_MARKET_DATA",
        )

    def _current_option_subscriptions(self) -> list[tuple[int, str]]:
        subscriptions = []
        seen = set()
        for index, pair in self.pairs.items():
            if pair.ce_id:
                item = (int(pair.ce_id), f"{index}_CE")
                if item[0] not in seen:
                    subscriptions.append(item)
                    seen.add(item[0])
            if pair.pe_id:
                item = (int(pair.pe_id), f"{index}_PE")
                if item[0] not in seen:
                    subscriptions.append(item)
                    seen.add(item[0])
        return subscriptions

    def _make_option_quote_stream(self) -> FutureQuoteStream:
        return FutureQuoteStream(
            client_id=self.settings.credentials.client_id,
            token=self.settings.credentials.access_token,
            exchange_segment=self.settings.option_exchange_segment,
            on_quote=self._on_option_full_quote,
            debug=False,
        )

    def _record_premium_stale_recovery_attempt(self, index: str, now: float) -> int:
        attempts = self.premium_stale_attempt_ts[index]
        attempts.append(float(now))
        while attempts and now - float(attempts[0]) > self.premium_rebuild_window_sec:
            attempts.popleft()
        return len(attempts)

    def _should_rebuild_premium_stream(self, index: str, now: float) -> bool:
        if not self.premium_rebuild_enabled:
            return False
        attempt_count = self._record_premium_stale_recovery_attempt(index, now)
        if attempt_count < self.premium_rebuild_attempt_threshold:
            return False
        if now - self.last_premium_stream_rebuild_ts < self.premium_rebuild_cooldown_sec:
            logger.warning(
                "PREMIUM_STREAM_REBUILD_SKIPPED | index=%s | attempts=%s | cooldown_remaining=%.1f",
                index,
                attempt_count,
                self.premium_rebuild_cooldown_sec - (now - self.last_premium_stream_rebuild_ts),
            )
            return False
        return True

    def _rebuild_option_quote_stream(self, trigger_index: str, reason: str, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        subscriptions = self._current_option_subscriptions()
        if not subscriptions:
            logger.warning("PREMIUM_STREAM_REBUILD_SKIPPED | reason=no_subscriptions | trigger_index=%s", trigger_index)
            return False

        logger.warning(
            "PREMIUM_STREAM_REBUILD_START | trigger_index=%s | reason=%s | subscriptions=%s | generation=%s",
            trigger_index,
            reason,
            subscriptions,
            self.premium_rebuild_generation + 1,
        )
        old_stream = self.option_quote_stream
        try:
            if old_stream is not None and hasattr(old_stream, "close"):
                old_closed = old_stream.close()
                if old_closed is False:
                    logger.error(
                        "PREMIUM_STREAM_REBUILD_ABORTED | trigger_index=%s | reason=old_stream_not_stopped",
                        trigger_index,
                    )
                    return False
        except Exception:
            logger.exception("PREMIUM_STREAM_OLD_CLOSE_FAILED | trigger_index=%s", trigger_index)
            return False

        try:
            new_stream = self._make_option_quote_stream()
            self.option_quote_stream = new_stream
            self._option_streams_started = True
            if hasattr(new_stream, "replace_subscriptions"):
                new_stream.replace_subscriptions(subscriptions, reason=f"premium_rebuild:{reason}")
            else:
                new_stream.subscribe(subscriptions)
            # Register the complete desired set before the websocket thread can
            # open, so its first on_open always sends every current contract.
            new_stream.start()
        except Exception:
            logger.exception("PREMIUM_STREAM_REBUILD_FAILED | trigger_index=%s | reason=%s", trigger_index, reason)
            self.option_quote_stream = old_stream
            self._option_streams_started = bool(old_stream)
            return False

        self.premium_rebuild_generation += 1
        self.last_premium_stream_rebuild_ts = now
        verify_until = now + self.premium_rebuild_verify_sec
        for profile in self.settings.indexes:
            self.premium_rebuild_pending_until[profile] = verify_until
            self._replace_tri_wave_data_quarantine(profile, "PREMIUM_STREAM_REBUILD_VERIFY", self.premium_rebuild_verify_sec)
        logger.warning(
            "PREMIUM_STREAM_REBUILD_DONE | generation=%s | verify_sec=%.1f | subscriptions=%s",
            self.premium_rebuild_generation,
            self.premium_rebuild_verify_sec,
            len(subscriptions),
        )
        return True

    def _premium_rebuild_verify_remaining(self, now: Optional[float] = None) -> float:
        now = time.time() if now is None else now
        pending_until = max((float(until) for until in self.premium_rebuild_pending_until.values()), default=0.0)
        cooldown_until = float(self.last_premium_stream_rebuild_ts or 0.0) + float(self.premium_rebuild_verify_sec or 0.0)
        return max(pending_until, cooldown_until) - now

    def _verify_premium_stream_for_index(self, index: str, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        pair = self.pairs.get(index)
        if pair is None:
            return False
        freshness = PairFreshness(
            ce_age=self._option_ltp_age(pair.ce_id, now),
            pe_age=self._option_ltp_age(pair.pe_id, now),
            max_age_sec=max(self.entry_pair_fresh_ltp_max_age_sec, self.premium_rebuild_verify_sec),
        )
        ce_tick_ts = float(self.option_ltp_last_ts_by_secid.get(int(pair.ce_id or 0), 0.0) or 0.0)
        pe_tick_ts = float(self.option_ltp_last_ts_by_secid.get(int(pair.pe_id or 0), 0.0) or 0.0)
        has_post_rebuild_ticks = (
            ce_tick_ts >= float(self.last_premium_stream_rebuild_ts or 0.0)
            and pe_tick_ts >= float(self.last_premium_stream_rebuild_ts or 0.0)
        )
        if freshness.is_fresh and has_post_rebuild_ticks:
            self.premium_rebuild_pending_until.pop(index, None)
            self.premium_stale_attempt_ts[index].clear()
            self._clear_tri_wave_data_quarantine(index, "PREMIUM_STREAM")
            logger.warning(
                "PREMIUM_STREAM_REBUILD_VERIFIED | index=%s | ce_age=%s | pe_age=%s | generation=%s",
                index,
                self._fmt_age(freshness.ce_age),
                self._fmt_age(freshness.pe_age),
                self.premium_rebuild_generation,
            )
            return True
        return False

    def _check_premium_stream_rebuild_verification(self, now: Optional[float] = None) -> None:
        now = time.time() if now is None else now
        for index, until in list(self.premium_rebuild_pending_until.items()):
            if self._verify_premium_stream_for_index(index, now):
                continue
            if until and now >= float(until):
                self.premium_rebuild_pending_until.pop(index, None)
                self._replace_tri_wave_data_quarantine(index, "PREMIUM_STREAM_DOWN", self.premium_stream_down_quarantine_sec)
                pair = self.pairs.get(index)
                ce_age = self._option_ltp_age(pair.ce_id, now) if pair else None
                pe_age = self._option_ltp_age(pair.pe_id, now) if pair else None
                logger.error(
                    "PREMIUM_STREAM_REBUILD_VERIFY_FAILED | index=%s | ce_age=%s | pe_age=%s | quarantine_sec=%.1f | generation=%s",
                    index,
                    self._fmt_age(ce_age),
                    self._fmt_age(pe_age),
                    self.premium_stream_down_quarantine_sec,
                    self.premium_rebuild_generation,
                )

    def _recover_stale_option_pair(self, index: str, pair: PairRuntimeState, now: Optional[float] = None) -> None:
        if self.pair_stale_resubscribe_sec <= 0:
            return
        now = time.time() if now is None else now
        if now - self._last_pair_resubscribe_ts[index] < self.pair_stale_resubscribe_cooldown_sec:
            return

        subscriptions = []
        if pair.ce_id:
            subscriptions.append((int(pair.ce_id), f"{index}_CE"))
        if pair.pe_id:
            subscriptions.append((int(pair.pe_id), f"{index}_PE"))
        if not subscriptions:
            return

        verify_remaining = self._premium_rebuild_verify_remaining(now)
        if verify_remaining > 0:
            self._replace_tri_wave_data_quarantine(index, "PREMIUM_STREAM_REBUILD_VERIFY", verify_remaining)
            logger.warning(
                "PREMIUM_STREAM_RECOVERY_SKIPPED | index=%s | reason=rebuild_in_progress_or_verify | generation=%s | verify_remaining=%.1f | subscriptions=%s",
                index,
                self.premium_rebuild_generation,
                verify_remaining,
                subscriptions,
            )
            return

        if self.static_daily_option_pairs:
            self._last_pair_resubscribe_ts[index] = now
            if self._should_rebuild_premium_stream(index, now):
                logger.warning(
                    "TRI_WAVE_PAIR_STALE_STATIC_ESCALATION | index=%s | action=rebuild_option_stream | subscriptions=%s | note=same_contracts",
                    index,
                    subscriptions,
                )
                self._rebuild_option_quote_stream(
                    index,
                    reason=f"static_pair_repeated_stale:{index}",
                    now=now,
                )
                return
            # First repair Dhan's server-side subscription state in place. This
            # uses official Full unsubscribe/subscribe request codes 22/21 and
            # avoids temporarily exceeding the account websocket limit.
            self._set_tri_wave_data_quarantine(index, "PAIR_STALE_RECOVERY", self.pair_stale_entry_quarantine_sec)
            logger.warning(
                "TRI_WAVE_PAIR_STALE_STATIC_RECOVERY | index=%s | action=refresh_all_option_subscriptions | subscriptions=%s | note=no_reselection",
                index,
                self._current_option_subscriptions(),
            )
            try:
                if self.option_quote_stream is None:
                    logger.error(
                        "TRI_WAVE_PAIR_STALE_STATIC_RECOVERY_FAILED | index=%s | reason=option_stream_missing",
                        index,
                    )
                    return
                all_subscriptions = self._current_option_subscriptions()
                if hasattr(self.option_quote_stream, "refresh_subscriptions"):
                    refreshed = self.option_quote_stream.refresh_subscriptions(
                        all_subscriptions,
                        reason=f"static_pair_stale:{index}",
                    )
                    if refreshed:
                        logger.warning(
                            "PREMIUM_SUBSCRIPTION_REFRESH_SENT | index=%s | subscriptions=%s",
                            index,
                            all_subscriptions,
                        )
                        return
                if not hasattr(self.option_quote_stream, "reconnect_for_subscriptions"):
                    logger.error(
                        "TRI_WAVE_PAIR_STALE_STATIC_RECOVERY_FAILED | index=%s | reason=reconnect_not_supported",
                        index,
                    )
                    return
                self.option_quote_stream.reconnect_for_subscriptions(
                    all_subscriptions,
                    reason=f"static_pair_refresh_failed:{index}",
                )
            except Exception:
                logger.exception("TRI_WAVE_PAIR_STALE_STATIC_RECOVERY_FAILED | index=%s", index)
            return

        self._last_pair_resubscribe_ts[index] = now
        if self._should_rebuild_premium_stream(index, now):
            self._rebuild_option_quote_stream(index, reason=f"repeated_pair_stale:{index}", now=now)
            return

        self._set_tri_wave_data_quarantine(index, "PAIR_STALE_RECOVERY", self.pair_stale_entry_quarantine_sec)
        action = "resubscribe_and_reconnect_option_shard" if self.pair_stale_reconnect_enabled else "resubscribe_only"
        logger.warning(
            "TRI_WAVE_PAIR_STALE_RECOVERY | index=%s | action=%s | subscriptions=%s",
            index,
            action,
            subscriptions,
        )
        try:
            if self.option_quote_stream is not None:
                if self.pair_stale_reconnect_enabled and hasattr(self.option_quote_stream, "reconnect_for_subscriptions"):
                    self.option_quote_stream.reconnect_for_subscriptions(subscriptions, reason=f"pair_stale:{index}")
        except Exception:
            logger.exception("TRI_WAVE_PAIR_STALE_RECOVERY_FAILED | index=%s", index)

    def _exit_stale_positions_if_needed(self) -> None:
        if self.stale_position_exit_sec <= 0:
            return
        now = time.time()
        if now - self._last_stale_position_check_ts < self.stale_position_check_sec:
            return
        self._last_stale_position_check_ts = now

        for index, pair in list(self.pairs.items()):
            for side, secid in (("CE", pair.ce_id), ("PE", pair.pe_id)):
                if not secid or secid not in self.paper_trader.positions:
                    continue
                position = self.paper_trader.positions.get(secid, {})
                ltp_ts = float(self.option_ltp_last_ts_by_secid.get(int(secid), 0.0) or 0.0)
                entry_ts = float(position.get("entry_ts", now) or now)
                age = now - (ltp_ts or entry_ts)
                if age < self.stale_position_exit_sec:
                    continue
                ltp = float(position.get("ltp", position.get("entry", 0.0)) or 0.0)
                if ltp <= 0:
                    ltp = pair._best_leg_ltp(
                        pair.ce_depth if side == "CE" else pair.pe_depth,
                        pair.ce_ltp if side == "CE" else pair.pe_ltp,
                        position.get("entry", 0.0),
                    )
                logger.warning(
                    "TRI_WAVE_STALE_POSITION_EXIT | index=%s | side=%s | secid=%s | ltp_age=%.2f | ltp=%.2f",
                    index,
                    side,
                    secid,
                    age,
                    float(ltp),
                )
                self._tri_wave_direct_exit(
                    pair,
                    int(secid),
                    float(ltp),
                    "TRI_WAVE_V2_EXIT:STALE_MARKET_DATA",
                )

    def _log_option_chain_health_if_needed(self, index: str, pair: PairRuntimeState) -> None:
        now = time.time()
        if now - self._last_option_chain_health_log_ts[index] < 300:
            return
        self._last_option_chain_health_log_ts[index] = now

        health = {}
        try:
            if hasattr(self.selector, "get_health"):
                health = self.selector.get_health(index)
        except Exception as exc:
            health = {"error": str(exc)}

        logger.info(
            "TRI_WAVE_OPTION_CHAIN_HEALTH | index=%s | cache_age=%s | last_fetch_ok=%s | retries=%s | current_ce=%s | current_pe=%s | last_error=%s",
            index,
            health.get("cache_age"),
            health.get("last_fetch_ok"),
            health.get("last_fetch_retries"),
            pair.ce_id,
            pair.pe_id,
            health.get("last_fetch_error") or health.get("error"),
        )

    def _update_market_snapshot(self, pair: PairRuntimeState, raw: dict, secid: int, tag: str) -> None:
        if getattr(self, "TRI_WAVE_ONLY_MODE", False) or getattr(self, "TRI_WAVE_V2_ONLY_MODE", False):
            logger.info(
                "LEGACY_MARKET_SNAPSHOT_SKIPPED_TRI_WAVE_ONLY | index=%s | secid=%s | tag=%s",
                pair.index, secid, tag
            )
            return
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
        if turn_signal:
            snapshot["confidence"] = turn_signal.get("confidence", 0.0)
        if not turn_signal:
            print("TURN_SIGNAL_NONE →", pair.index)
            return

        latest_tri = self.tri_wave_brain.get_latest_state(pair.index) if hasattr(self, "tri_wave_brain") else {}
        if latest_tri.get("sync_ready") is True:
            old_signal = str(turn_signal.get("signal", ""))
            tri_bias = latest_tri.get("bias")
            if old_signal == "BULLISH_CONTINUATION" and tri_bias != "CE":
                logger.info(
                    "OLD_SIGNAL_BLOCKED_BY_TRIWAVE | index=%s | signal=%s | tri_bias=%s | tri_reason=%s",
                    pair.index, old_signal, tri_bias, latest_tri.get("reason")
                )
                return
            if old_signal == "BEARISH_CONTINUATION" and tri_bias != "PE":
                logger.info(
                    "OLD_SIGNAL_BLOCKED_BY_TRIWAVE | index=%s | signal=%s | tri_bias=%s | tri_reason=%s",
                    pair.index, old_signal, tri_bias, latest_tri.get("reason")
                )
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
        if getattr(self, "TRI_WAVE_ONLY_MODE", False) or getattr(self, "TRI_WAVE_V2_ONLY_MODE", False):
            logger.info(
                "LEGACY_EXIT_ENGINE_SKIPPED_TRI_WAVE_ONLY | secid=%s | tag=%s",
                secid, tag
            )
            return
        action = self.momentum_engine.on_tick(secid, raw)
        decision = {"exit_allowed": True}
        tick_bucket = int(time.time() * 10)
        self._tick_exit_guard[secid] = {"bucket": tick_bucket, "reason": None, "priority": None}

        if self._is_tri_wave_position(secid):
            logger.info(
                "TRI_WAVE_OWNED_POSITION | secid=%s | tag=%s | legacy_exit_engines=SKIPPED",
                secid,
                tag,
            )
            return

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
        if active:
            entry = float(active.get("entry", 0))
            entry_ts = float(active.get("entry_ts", time.time()))
            hold_sec = time.time() - entry_ts
            pnl_pct = ((ltp - entry) / entry) * 100 if entry > 0 else 0

            logger.info("GLOBAL_HOLD_CHECK | %.2f sec | pnl=%.2f%%", hold_sec, pnl_pct)
            logger.info("EXIT_GATE | tag=%s | reason=%s | hold=%.2f | pnl_pct=%.2f", tag, reason, hold_sec, pnl_pct)

            if self._is_tri_wave_position(opposite_secid):
                logger.info(
                    "TRI_WAVE_FORCE_EXIT_BYPASS_STATIC_HOLD | secid=%s | reason=%s | hold_sec=%.2f",
                    opposite_secid,
                    "OPPOSITE_TURN_CONFIRMED",
                    hold_sec,
                )
            elif hold_sec < 40:
                if pnl_pct < -8:
                    logger.info("EMERGENCY_EXIT_ALLOWED | pnl_pct=%.2f", pnl_pct)
                else:
                    logger.info(
                        "EXIT_BLOCKED_REASON | GLOBAL_HOLD_PROTECTION | hold_sec=%.2f",
                        hold_sec
                    )
                    return False

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
        if reason.startswith("TRI_WAVE_FLIP_EXIT"):
            reason_priority = self.EXIT_REASON_PRIORITY.get("TRI_WAVE_FLIP_EXIT", 5)
        elif reason.startswith("TRI_WAVE_EXIT"):
            reason_priority = self.EXIT_REASON_PRIORITY.get("TRI_WAVE_EXIT", 6)
        elif reason.startswith("TRI_WAVE_EMERGENCY_EXIT") or reason.startswith("TRI_WAVE_EXIT:EMERGENCY_LOSS"):
            reason_priority = self.EXIT_REASON_PRIORITY.get("TRI_WAVE_EMERGENCY_EXIT", 1)
        else:
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
        if hold_sec < 40 and not self._is_tri_wave_position(secid):
            logger.info(
                "EXIT_BLOCKED_REASON | FLOW_HOLD_PROTECTION | hold_sec=%.2f",
                hold_sec
            )
            return
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

        active = self.paper_trader.positions.get(opposite_secid)
        if active:
            entry_ts = float(active.get("entry_ts", time.time()))
            hold_sec = time.time() - entry_ts

            logger.info("HOLD_TIME_CHECK | %.2f sec", hold_sec)

            if self._is_tri_wave_position(opposite_secid):
                logger.info(
                    "TRI_WAVE_FORCE_EXIT_BYPASS_STATIC_HOLD | secid=%s | reason=%s | hold_sec=%.2f",
                    opposite_secid,
                    "OPPOSITE_TURN_CONFIRMED",
                    hold_sec,
                )
            elif hold_sec < 40:
                logger.info(
                    "EXIT_BLOCKED_REASON | HOLD_PROTECTION_ACTIVE | hold_sec=%.2f",
                    hold_sec
                )
                return False

        flow = self.premium_flow.get("dominant")

        if "CE" in opposite_tag and flow != "PE":
            logger.info("EXIT_BLOCKED_REASON | NO_FLOW_CONFIRMATION_CE")
            return False

        if "PE" in opposite_tag and flow != "CE":
            logger.info("EXIT_BLOCKED_REASON | NO_FLOW_CONFIRMATION_PE")
            return False

        entry = float(active.get("entry", 0))
        pnl_pct = ((float(opposite_ltp) - entry) / entry) * 100 if entry > 0 else 0

        peak = active.get("peak_pnl_pct", pnl_pct)
        if pnl_pct > peak:
            active["peak_pnl_pct"] = pnl_pct
            peak = pnl_pct
        elif "peak_pnl_pct" not in active:
            active["peak_pnl_pct"] = peak

        drawdown = peak - pnl_pct

        # RULE 1: If profit is increasing → HOLD (DO NOT EXIT)
        if pnl_pct > 0 and pnl_pct >= peak:
            logger.info(
                "HOLD_PROFIT_GROWING | tag=%s | pnl_pct=%.3f | peak=%.3f",
                opposite_tag,
                pnl_pct,
                peak,
            )
            return False

        # RULE 2: If profit dropped from peak → EXIT
        if peak > 0.30 and drawdown > 0.20:
            logger.info(
                "TRAILING_EXIT | tag=%s | pnl_pct=%.3f | peak=%.3f | drawdown=%.3f",
                opposite_tag,
                pnl_pct,
                peak,
                drawdown,
            )
            return self._attempt_exit_once(
                pair,
                opposite_secid,
                opposite_tag,
                float(opposite_ltp),
                "TRAIL_PROFIT_EXIT",
            )

        # RULE 3: If loss → EXIT immediately
        if pnl_pct < -0.30:
            logger.info(
                "LOSS_EXIT | tag=%s | pnl_pct=%.3f",
                opposite_tag,
                pnl_pct,
            )
            return self._attempt_exit_once(
                pair,
                opposite_secid,
                opposite_tag,
                float(opposite_ltp),
                "LOSS_EXIT",
            )

        # RULE 4: Otherwise HOLD (ignore weak opposite signals)
        logger.info(
            "HOLD_WEAK_SIGNAL | tag=%s | pnl_pct=%.3f",
            opposite_tag,
            pnl_pct,
        )
        return False

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
        close_summary_logged_for_date = None
        logger.info("LIVE: INSTITUTIONAL OPTIONS ENGINE ACTIVE")
        while True:
            if not self.market_open():
                now_dt = datetime.now(self.timezone)
                if now_dt.hour > 15 or (now_dt.hour == 15 and now_dt.minute >= 30):
                    session_date = now_dt.strftime("%Y-%m-%d")
                    if close_summary_logged_for_date != session_date:
                        close_summary_logged_for_date = session_date
                        logger.info(
                            "TRI_WAVE_MARKET_CLOSE_SUMMARY | date=%s | dir=%s | ticks=%s | states=%s | signals=%s | trades=%s | portfolio=%s",
                            session_date,
                            self.tri_wave_recorder.session_dir,
                            self.tri_wave_recorder.ticks_written,
                            self.tri_wave_recorder.states_written,
                            self.tri_wave_recorder.signals_written,
                            self.tri_wave_recorder.trades_written,
                            self.tri_wave_recorder.portfolio_written,
                        )
                        logger.info(
                            "TRI_WAVE_MARKET_CLOSE_ANALYZE_CMD | cmd=python scripts/analyze_triwave_session.py --date %s --expiry %s",
                            session_date,
                            getattr(self.tri_wave_recorder, "expiry_key", "unknown"),
                        )
                logger.info("⏸️ MARKET_CLOSED | idle mode")
                time.sleep(30.0)
                continue

            now = time.time()
            self._check_premium_stream_rebuild_verification(now)
            self._exit_stale_positions_if_needed()
            if now - last_heartbeat >= self.settings.heartbeat_sec:
                last_heartbeat = now
                logger.info("%s ENGINE_RUNNING", datetime.now(self.timezone).strftime("%H:%M:%S"))
                logger.info(
                    "📊 FLOW | CE_vel=%.2f | PE_vel=%.2f | DOM=%s",
                    self.premium_flow["CE"]["velocity"],
                    self.premium_flow["PE"]["velocity"],
                    self.premium_flow["dominant"],
                )
                for index, pair in self.pairs.items():
                    if pair.ce_id or pair.pe_id:
                        self._log_pair_stale_if_needed(index, pair)
                        self._log_option_chain_health_if_needed(index, pair)
                self._log_no_entry_watch_if_needed(now)

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
