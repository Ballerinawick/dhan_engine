import json
import logging
import os
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


class TriWaveSessionRecorder:
    def __init__(self, enabled: bool = True, base_dir: str = "data/triwave_sessions", timezone: str = "Asia/Kolkata"):
        self.enabled = bool(enabled)
        self.base_dir = str(base_dir)
        self.timezone = ZoneInfo(timezone)
        self._lock = threading.RLock()
        self.session_date = datetime.now(self.timezone).strftime("%Y-%m-%d")
        self.session_dir = os.path.join(self.base_dir, self.session_date)
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

    def _write(self, filename: str, row: dict) -> None:
        if not self.enabled:
            return
        try:
            with self._lock:
                handle = self._handles.get(filename)
                if handle is None:
                    os.makedirs(self.session_dir, exist_ok=True)
                    handle = open(os.path.join(self.session_dir, filename), "a", encoding="utf-8", buffering=1)
                    self._handles[filename] = handle
                handle.write(json.dumps(self._to_safe(row), ensure_ascii=False) + "\n")
                handle.flush()
        except Exception:
            logger.warning("TRI_WAVE_SESSION_RECORDER_WRITE_FAILED | file=%s", filename, exc_info=True)

    def record_tick(self, index: str, stream: str, secid: int, ltp: float, features: dict):
        now = time.time()
        self._write("ticks.jsonl", {
            "ts": now,
            "time": datetime.fromtimestamp(now, self.timezone).strftime("%H:%M:%S"),
            "index": index,
            "stream": stream,
            "secid": int(secid),
            "ltp": float(ltp),
            "feature_source": (features or {}).get("feature_source"),
            "features": dict(features or {}),
        })

    def record_state(self, index: str, snapshot: dict):
        now = time.time()
        self._write("states.jsonl", {"ts": now, "time": datetime.fromtimestamp(now, self.timezone).strftime("%H:%M:%S"), "index": index, "snapshot": snapshot or {}})

    def record_signal(self, index: str, signal):
        now = time.time()
        data = signal if isinstance(signal, dict) else getattr(signal, "__dict__", {"value": str(signal)})
        self._write("signals.jsonl", {"ts": now, "time": datetime.fromtimestamp(now, self.timezone).strftime("%H:%M:%S"), "index": index, "signal": data})

    def record_trade(self, trade: dict):
        now = time.time()
        self._write("trades.jsonl", {"ts": now, "time": datetime.fromtimestamp(now, self.timezone).strftime("%H:%M:%S"), "trade": trade or {}})

    def record_portfolio(self, portfolio: dict):
        now = time.time()
        self._write("portfolio.jsonl", {"ts": now, "time": datetime.fromtimestamp(now, self.timezone).strftime("%H:%M:%S"), "portfolio": portfolio or {}})
