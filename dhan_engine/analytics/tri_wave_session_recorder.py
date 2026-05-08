import json
import logging
import os
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


class TriWaveSessionRecorder:
    def __init__(self, enabled: bool = True, base_dir: str | None = None, timezone: str = "Asia/Kolkata", expiry_key: str = "unknown"):
        self.enabled = bool(enabled)
        self.base_dir = str(base_dir or os.getenv("TRIWAVE_SESSION_BASE_DIR", "data/triwave_sessions"))
        self.expiry_key = str(expiry_key or "unknown")
        self.timezone = ZoneInfo(timezone)
        self._lock = threading.RLock()
        self.ticks_written = 0
        self.states_written = 0
        self.signals_written = 0
        self.trades_written = 0
        self.portfolio_written = 0
        self._last_heartbeat_ts = 0.0
        self.heartbeat_interval_sec = 30.0
        self.session_date = datetime.now(self.timezone).strftime("%Y-%m-%d")
        self.session_dir = os.path.join(self.base_dir, self.session_date, f"expiry={self.expiry_key}")
        persistent_hint = "VOLUME_OR_EXTERNAL" if self.base_dir.startswith("/data") or self.base_dir.startswith("/mnt") else "EPHEMERAL_CONTAINER"
        logger.info("TRI_WAVE_SESSION_RECORDER_ACTIVE | dir=%s | persistent_hint=%s", self.session_dir, persistent_hint)
        if self.expiry_key == "unknown":
            logger.info("TRI_WAVE_RECORDER_EXPIRY_UNKNOWN")
        self._handles = {}
        if self.enabled:
            try:
                os.makedirs(self.session_dir, exist_ok=True)
                for name in ["ticks.jsonl", "states.jsonl", "signals.jsonl", "trades.jsonl", "portfolio.jsonl"]:
                    self._handles[name] = open(os.path.join(self.session_dir, name), "a", encoding="utf-8", buffering=1)
            except Exception:
                logger.warning("TRI_WAVE_SESSION_RECORDER_INIT_FAILED", exc_info=True)
                self.enabled = False

    def _to_safe(self, value):
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, dict):
            return {str(k): self._to_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._to_safe(v) for v in value]
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    def _maybe_log_heartbeat(self):
        now = time.time()
        if now - self._last_heartbeat_ts < self.heartbeat_interval_sec:
            return
        self._last_heartbeat_ts = now
        logger.info(
            "TRI_WAVE_RECORDER_HEARTBEAT | dir=%s | ticks=%s | states=%s | signals=%s | trades=%s | portfolio=%s",
            self.session_dir,
            self.ticks_written,
            self.states_written,
            self.signals_written,
            self.trades_written,
            self.portfolio_written,
        )

    def _write(self, filename: str, row: dict) -> bool:
        if not self.enabled:
            return False
        try:
            with self._lock:
                handle = self._handles.get(filename)
                if handle is None:
                    os.makedirs(self.session_dir, exist_ok=True)
                    handle = open(os.path.join(self.session_dir, filename), "a", encoding="utf-8", buffering=1)
                    self._handles[filename] = handle
                handle.write(json.dumps(self._to_safe(row), ensure_ascii=False) + "\n")
                handle.flush()
                return True
        except Exception:
            logger.warning("TRI_WAVE_SESSION_RECORDER_WRITE_FAILED | file=%s", filename, exc_info=True)
            return False

    def record_tick(self, index: str, stream: str, secid: int, ltp: float, features: dict, raw: dict | None = None, route_source: str | None = None):
        now = time.time()
        if self._write("ticks.jsonl", {
            "ts": now,
            "time": datetime.fromtimestamp(now, self.timezone).strftime("%H:%M:%S"),
            "index": index,
            "stream": stream,
            "secid": int(secid),
            "ltp": float(ltp),
            "feature_source": (features or {}).get("feature_source"),
            "features": dict(features or {}),
            "raw": raw if raw is not None else None,
            "route_source": route_source,
        }):
            self.ticks_written += 1
            self._maybe_log_heartbeat()

    def record_state(self, index: str, snapshot: dict):
        now = time.time()
        if self._write("states.jsonl", {"ts": now, "time": datetime.fromtimestamp(now, self.timezone).strftime("%H:%M:%S"), "index": index, "snapshot": snapshot or {}}):
            self.states_written += 1
            self._maybe_log_heartbeat()

    def record_signal(self, index: str, signal):
        now = time.time()
        data = signal if isinstance(signal, dict) else getattr(signal, "__dict__", {"value": str(signal)})
        if self._write("signals.jsonl", {"ts": now, "time": datetime.fromtimestamp(now, self.timezone).strftime("%H:%M:%S"), "index": index, "signal": data}):
            self.signals_written += 1
            self._maybe_log_heartbeat()

    def record_trade(self, trade: dict):
        now = time.time()
        if self._write("trades.jsonl", {"ts": now, "time": datetime.fromtimestamp(now, self.timezone).strftime("%H:%M:%S"), "trade": trade or {}}):
            self.trades_written += 1
            self._maybe_log_heartbeat()

    def record_portfolio(self, portfolio: dict):
        now = time.time()
        if self._write("portfolio.jsonl", {"ts": now, "time": datetime.fromtimestamp(now, self.timezone).strftime("%H:%M:%S"), "portfolio": portfolio or {}}):
            self.portfolio_written += 1
            self._maybe_log_heartbeat()
