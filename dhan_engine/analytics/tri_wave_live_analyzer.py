import json
import logging
import os
import time
from datetime import datetime
from statistics import mean
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


class TriWaveLiveAnalyzer:
    def __init__(self, recorder, interval_sec: int = 300):
        self.recorder = recorder
        self.interval_sec = int(interval_sec)
        self._last_run_ts = 0.0
        self._tz = ZoneInfo("Asia/Kolkata")

    def _safe_load_recent(self, filename: str, window_start_ts: float):
        path = os.path.join(self.recorder.session_dir, filename)
        if not os.path.exists(path):
            return []
        rows = []
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    row = json.loads(line)
                    if float(row.get("ts", 0) or 0) >= window_start_ts:
                        rows.append(row)
        except Exception:
            logger.exception("TRI_WAVE_LIVE_ANALYSIS_READ_ERROR | file=%s", filename)
        return rows

    def maybe_analyze(self, paper_trader=None):
        now = time.time()
        if now - self._last_run_ts < self.interval_sec:
            return
        self._last_run_ts = now
        window_start = now - self.interval_sec

        recent_trades = self._safe_load_recent("trades.jsonl", window_start)
        trades = [r.get("trade", {}) for r in recent_trades]
        pnls = [float((t or {}).get("net_pnl", 0.0) or 0.0) for t in trades]
        holds = [float((t or {}).get("hold_sec", 0.0) or 0.0) for t in trades if (t or {}).get("hold_sec") is not None]
        wins = sum(1 for p in pnls if p > 0)

        active_positions = len(getattr(paper_trader, "positions", {}) or {}) if paper_trader else None
        fees_paid = float(getattr(paper_trader, "fees_paid", 0.0) or 0.0) if paper_trader else 0.0
        exit_reason_counts = {}
        for t in trades:
            reason = str((t or {}).get("exit_reason", "UNKNOWN") or "UNKNOWN")
            exit_reason_counts[reason] = exit_reason_counts.get(reason, 0) + 1

        total_recent_signals = len(self._safe_load_recent("signals.jsonl", window_start))
        churn_ratio = (len(trades) / total_recent_signals) if total_recent_signals > 0 else 0.0
        net = sum(pnls)
        win_rate = (wins / len(pnls) * 100.0) if pnls else 0.0
        avg_hold = mean(holds) if holds else 0.0
        trend = "improving" if net > 0 else "decaying" if net < 0 else "flat"

        analysis_row = {
            "ts": now,
            "time": datetime.fromtimestamp(now, self._tz).strftime("%H:%M:%S"),
            "window": "5m",
            "window_sec": self.interval_sec,
            "trades": len(trades),
            "net_pnl": net,
            "win_rate": win_rate,
            "avg_hold_sec": avg_hold,
            "open_positions": active_positions,
            "fees_paid_total": fees_paid,
            "churn_ratio": churn_ratio,
            "exit_reason_counts": exit_reason_counts,
            "trend": trend,
        }
        self.recorder._write("live_analysis.jsonl", analysis_row)
        logger.info(
            "TRI_WAVE_LIVE_ANALYSIS | window=5m | trades=%s | net=%.2f | win_rate=%.1f | avg_hold=%.1f | early_exit=%s | late_entry=%s | bad_entry=%s | comment=%s",
            len(trades),
            net,
            win_rate,
            avg_hold,
            exit_reason_counts.get("EARLY_EXIT", 0),
            0,
            0,
            trend,
        )
