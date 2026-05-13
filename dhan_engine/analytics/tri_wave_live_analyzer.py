import json
import logging
import os
import time
from collections import Counter
from datetime import datetime
from statistics import mean
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


class TriWaveLiveAnalyzer:
    TURN_LOOKBACK_SEC = 1800
    TURN_MIN_MOVE_PCT = 0.80
    TURN_CONFIRM_SEC = 20
    ENTRY_TOLERANCE_SEC = 30
    EXIT_TOLERANCE_SEC = 30
    LIVE_TURN_ANALYSIS_FILE = "live_turn_analysis.jsonl"

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

    def _load_all_jsonl(self, filename: str) -> list[dict]:
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
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(row, dict):
                        rows.append(row)
        except Exception:
            logger.exception("TRI_WAVE_LIVE_ANALYSIS_READ_ERROR | file=%s", filename)
        return rows

    @staticmethod
    def _to_float(value, default=None):
        try:
            if value is None or value == "":
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    def _hms_to_ts_today(self, raw_hms):
        if not raw_hms:
            return None
        try:
            hmst = datetime.strptime(str(raw_hms), "%H:%M:%S").time()
            dt = datetime.now(self._tz).replace(hour=hmst.hour, minute=hmst.minute, second=hmst.second, microsecond=0)
            return dt.timestamp()
        except ValueError:
            return None

    def _normalize_trade(self, row):
        tr = row.get("trade") if isinstance(row, dict) and isinstance(row.get("trade"), dict) else row
        if not isinstance(tr, dict):
            return None
        side = str(tr.get("side") or tr.get("Side") or "").upper()
        if not side:
            tag = str(tr.get("tag") or tr.get("Tag") or tr.get("symbol") or tr.get("Symbol") or "").upper()
            side = "CE" if "CE" in tag else "PE" if "PE" in tag else ""

        entry_ts = self._to_float(tr.get("entry_ts")) or self._to_float(tr.get("EntryTs")) or self._to_float(tr.get("entry_timestamp"))
        exit_ts = self._to_float(tr.get("exit_ts")) or self._to_float(tr.get("ExitTs")) or self._to_float(tr.get("exit_timestamp"))
        entry_time = tr.get("entry_time") or tr.get("EntryTime")
        exit_time = tr.get("exit_time") or tr.get("ExitTime")
        if entry_ts is None:
            entry_ts = self._hms_to_ts_today(entry_time)
        if exit_ts is None:
            exit_ts = self._hms_to_ts_today(exit_time)

        entry = self._to_float(tr.get("entry"), None)
        if entry is None:
            entry = self._to_float(tr.get("Entry"), None)
        if entry is None:
            entry = self._to_float(tr.get("entry_price"), None)
        if entry is None:
            entry = self._to_float(tr.get("EntryPrice"), 0.0)

        exitp = self._to_float(tr.get("exit"), None)
        if exitp is None:
            exitp = self._to_float(tr.get("Exit"), None)
        if exitp is None:
            exitp = self._to_float(tr.get("exit_price"), None)
        if exitp is None:
            exitp = self._to_float(tr.get("ExitPrice"), 0.0)

        net_pnl = self._to_float(tr.get("net_pnl"), None)
        if net_pnl is None:
            net_pnl = self._to_float(tr.get("NetPnL"), None)
        if net_pnl is None:
            net_pnl = self._to_float(tr.get("net"), None)
        if net_pnl is None:
            net_pnl = self._to_float(tr.get("Net"), 0.0)

        gross_pnl = self._to_float(tr.get("gross_pnl"), None)
        if gross_pnl is None:
            gross_pnl = self._to_float(tr.get("GrossPnL"), 0.0)
        hold_sec = self._to_float(tr.get("hold_sec"), None)
        if hold_sec is None:
            hold_sec = self._to_float(tr.get("HoldSec"), 0.0)
        reason = (
            tr.get("entry_reason")
            or tr.get("EntryReason")
            or tr.get("exit_reason")
            or tr.get("ExitReason")
            or ""
        )
        return {
            "side": side,
            "entry_ts": entry_ts,
            "exit_ts": exit_ts,
            "entry_time": entry_time,
            "exit_time": exit_time,
            "entry": entry,
            "exit": exitp,
            "net_pnl": net_pnl,
            "gross_pnl": gross_pnl,
            "hold_sec": hold_sec,
            "reason": reason,
        }

    def _window(self, rows, start_ts, end_ts):
        return [r for r in rows if start_ts <= (r.get("ts") or 0) <= end_ts]

    def _detect_turns_raw(self, ticks: list[dict], stream: str) -> list[dict]:
        turns = []
        n = len(ticks)
        if n < 5:
            return turns
        for i in range(1, n - 1):
            pivot = ticks[i]
            pivot_ts = pivot["ts"]
            pivot_price = pivot["ltp"]
            before = self._window(ticks, pivot_ts - self.TURN_CONFIRM_SEC, pivot_ts)
            after = self._window(ticks, pivot_ts, pivot_ts + self.TURN_CONFIRM_SEC)
            if len(before) < 2 or len(after) < 2:
                continue

            before_prices = [x["ltp"] for x in before]
            after_prices = [x["ltp"] for x in after]
            min_before, max_before = min(before_prices), max(before_prices)
            min_after, max_after = min(after_prices), max(after_prices)

            is_local_high = pivot_price >= max(before_prices + after_prices)
            is_local_low = pivot_price <= min(before_prices + after_prices)

            if is_local_high:
                rise_before_pct = ((pivot_price - min_before) / max(min_before, 1e-9)) * 100.0
                drop_after_pct = ((pivot_price - min_after) / max(pivot_price, 1e-9)) * 100.0
                if rise_before_pct >= self.TURN_MIN_MOVE_PCT and drop_after_pct >= self.TURN_MIN_MOVE_PCT:
                    turns.append({
                        "stream": stream,
                        "turn_type": "HIGH",
                        "ts": pivot_ts,
                        "time": datetime.fromtimestamp(pivot_ts, self._tz).strftime("%H:%M:%S"),
                        "price": pivot_price,
                        "move_before_pct": rise_before_pct,
                        "move_after_pct": drop_after_pct,
                        "strength": min(rise_before_pct, drop_after_pct),
                    })
            elif is_local_low:
                drop_before_pct = ((max_before - pivot_price) / max(max_before, 1e-9)) * 100.0
                rise_after_pct = ((max_after - pivot_price) / max(pivot_price, 1e-9)) * 100.0
                if drop_before_pct >= self.TURN_MIN_MOVE_PCT and rise_after_pct >= self.TURN_MIN_MOVE_PCT:
                    turns.append({
                        "stream": stream,
                        "turn_type": "LOW",
                        "ts": pivot_ts,
                        "time": datetime.fromtimestamp(pivot_ts, self._tz).strftime("%H:%M:%S"),
                        "price": pivot_price,
                        "move_before_pct": drop_before_pct,
                        "move_after_pct": rise_after_pct,
                        "strength": min(drop_before_pct, rise_after_pct),
                    })
        return turns

    def _filter_turns(self, turns: list[dict]) -> list[dict]:
        if not turns:
            return []
        min_gap_sec = 20
        alt_gap_sec = 10
        filtered = []
        for turn in sorted(turns, key=lambda x: x["ts"]):
            move_after_points = (turn["price"] * turn["move_after_pct"]) / 100.0
            min_points = max(0.75, turn["price"] * 0.003)
            if turn.get("move_after_pct", 0.0) < self.TURN_MIN_MOVE_PCT or abs(move_after_points) < min_points:
                continue
            if filtered:
                prev = filtered[-1]
                gap = turn["ts"] - prev["ts"]
                if turn["turn_type"] == prev["turn_type"] and gap < min_gap_sec:
                    if turn.get("strength", 0.0) > prev.get("strength", 0.0):
                        filtered[-1] = turn
                    continue
                if turn["turn_type"] != prev["turn_type"] and gap < alt_gap_sec:
                    if turn.get("strength", 0.0) > prev.get("strength", 0.0):
                        filtered[-1] = turn
                    continue
            filtered.append(turn)
        return filtered

    def _detect_turns(self, ticks: list[dict], stream: str):
        raw_turns = self._detect_turns_raw(ticks, stream)
        return raw_turns, self._filter_turns(raw_turns)

    @staticmethod
    def _nearest_turn(turns, target_ts, turn_type, max_delta_sec=60):
        if target_ts is None:
            return None
        candidates = [t for t in turns if t["turn_type"] == turn_type and abs(t["ts"] - target_ts) <= max_delta_sec]
        if not candidates:
            return None
        return min(candidates, key=lambda t: abs(t["ts"] - target_ts))

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

        try:
            lookback_start = now - self.TURN_LOOKBACK_SEC
            ticks_rows = self._safe_load_recent("ticks.jsonl", lookback_start)
            self._safe_load_recent("states.jsonl", lookback_start)
            self._safe_load_recent("signals.jsonl", lookback_start)
            raw_trade_rows = self._load_all_jsonl("trades.jsonl")

            ticks_by_stream = {"FUT": [], "CE": [], "PE": []}
            for row in ticks_rows:
                tick = row.get("tick") if isinstance(row, dict) and isinstance(row.get("tick"), dict) else row
                if not isinstance(tick, dict):
                    continue
                stream = str(tick.get("stream", row.get("stream", ""))).upper()
                ts = self._to_float(tick.get("ts", row.get("ts")))
                ltp = self._to_float(tick.get("ltp", row.get("ltp")))
                if stream not in ticks_by_stream or ts is None or ltp is None:
                    continue
                ticks_by_stream[stream].append({
                    "ts": ts,
                    "time": tick.get("time", row.get("time")),
                    "stream": stream,
                    "ltp": ltp,
                    "features": tick.get("features") or row.get("features") or {},
                })
            for k in ticks_by_stream:
                ticks_by_stream[k].sort(key=lambda x: x["ts"])

            turns_raw_by_stream = {}
            turns_by_stream = {}
            for k, v in ticks_by_stream.items():
                raw_turns, filtered_turns = self._detect_turns(v, k)
                turns_raw_by_stream[k] = raw_turns
                turns_by_stream[k] = filtered_turns
            entry_quality_counts = Counter()
            exit_quality_counts = Counter()
            trade_reviews = []
            matched_entries = 0
            matched_exits = 0

            norm_trades = [self._normalize_trade(r) for r in raw_trade_rows]
            norm_trades = [t for t in norm_trades if isinstance(t, dict) and t.get("side") in {"CE", "PE"}]
            closed_trades = [
                t for t in norm_trades
                if t.get("entry_ts")
                and t.get("exit_ts")
                and t["exit_ts"] >= lookback_start
            ]

            logger.info(
                "TRI_WAVE_TURN_ANALYZER_DEBUG | raw_trades=%s | normalized_trades=%s | recent_trades=%s | ce_ticks=%s | pe_ticks=%s | fut_ticks=%s | ce_turns_raw=%s | ce_turns_filtered=%s | pe_turns_raw=%s | pe_turns_filtered=%s | fut_turns_raw=%s | fut_turns_filtered=%s",
                len(raw_trade_rows),
                len(norm_trades),
                len(closed_trades),
                len(ticks_by_stream["CE"]),
                len(ticks_by_stream["PE"]),
                len(ticks_by_stream["FUT"]),
                len(turns_raw_by_stream["CE"]),
                len(turns_by_stream["CE"]),
                len(turns_raw_by_stream["PE"]),
                len(turns_by_stream["PE"]),
                len(turns_raw_by_stream["FUT"]),
                len(turns_by_stream["FUT"]),
            )

            for tr in closed_trades:
                side = tr["side"]
                entry_ts = tr["entry_ts"]
                exit_ts = tr["exit_ts"]
                stream_ticks = ticks_by_stream.get(side, [])
                side_turns = turns_by_stream.get(side, [])

                entry_turn = self._nearest_turn(side_turns, entry_ts, "LOW", max_delta_sec=90)
                exit_turn = self._nearest_turn(side_turns, exit_ts, "HIGH", max_delta_sec=90)

                if entry_turn:
                    matched_entries += 1
                    entry_delay = entry_ts - entry_turn["ts"]
                    if abs(entry_delay) <= self.ENTRY_TOLERANCE_SEC:
                        entry_quality = "TIMELY_ENTRY"
                    elif entry_delay > self.ENTRY_TOLERANCE_SEC:
                        entry_quality = "LATE_ENTRY"
                    else:
                        entry_quality = "EARLY_ENTRY"
                else:
                    entry_delay = None
                    entry_quality = "NO_TURN_ENTRY"

                if exit_turn:
                    matched_exits += 1
                    exit_delay = exit_ts - exit_turn["ts"]
                    if abs(exit_delay) <= self.EXIT_TOLERANCE_SEC:
                        exit_quality = "TIMELY_EXIT"
                    elif exit_delay > self.EXIT_TOLERANCE_SEC:
                        exit_quality = "LATE_EXIT"
                    else:
                        exit_quality = "EARLY_EXIT"
                else:
                    exit_delay = None
                    exit_quality = "NO_TURN_EXIT"

                entry_quality_counts[entry_quality] += 1
                exit_quality_counts[exit_quality] += 1

                after_entry = self._window(stream_ticks, entry_ts, min(entry_ts + 120, now))
                after_exit = self._window(stream_ticks, exit_ts, min(exit_ts + 120, now))
                entry_price = tr.get("entry") or 0.0
                exit_price = tr.get("exit") or 0.0
                favorable = max((x["ltp"] - entry_price for x in after_entry), default=0.0)
                adverse = min((x["ltp"] - entry_price for x in after_entry), default=0.0)
                missed_profit = max((x["ltp"] - exit_price for x in after_exit), default=0.0)
                adverse_protection = max((exit_price - x["ltp"] for x in after_exit), default=0.0)

                comment = "entry_exit_unknown"
                if entry_quality == "LATE_ENTRY":
                    comment = "Entry was late after premium low turn"
                elif entry_quality == "TIMELY_ENTRY":
                    comment = "Entry was close to real premium low turn"
                elif entry_quality == "EARLY_ENTRY":
                    comment = "Entry was early before premium low turn"
                if exit_quality == "TIMELY_EXIT":
                    comment += "; exit was timely near premium exhaustion"
                elif exit_quality == "EARLY_EXIT":
                    comment += "; exit was early before premium high"
                elif exit_quality == "LATE_EXIT":
                    comment += "; exit was late after premium high"

                trade_reviews.append({
                    "side": side,
                    "entry_time": tr.get("entry_time") or datetime.fromtimestamp(entry_ts, self._tz).strftime("%H:%M:%S"),
                    "exit_time": tr.get("exit_time") or datetime.fromtimestamp(exit_ts, self._tz).strftime("%H:%M:%S"),
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "net_pnl": tr.get("net_pnl", 0.0),
                    "entry_quality": entry_quality,
                    "entry_delay_sec": entry_delay,
                    "exit_quality": exit_quality,
                    "exit_delay_sec": exit_delay,
                    "favorable_move_after_entry": favorable,
                    "adverse_move_after_entry": adverse,
                    "missed_profit_after_exit_120s": missed_profit,
                    "adverse_protection_after_exit_120s": adverse_protection,
                    "comment": comment,
                })

            missed_entry_turns = 0
            missed_exit_turns = 0
            for side in ["CE", "PE"]:
                side_turns = turns_by_stream.get(side, [])
                lows = [t for t in side_turns if t["turn_type"] == "LOW"]
                highs = [t for t in side_turns if t["turn_type"] == "HIGH"]
                for low in lows:
                    if not any(t["side"] == side and t.get("entry_ts") and abs(t["entry_ts"] - low["ts"]) <= 60 for t in norm_trades):
                        missed_entry_turns += 1
                for high in highs:
                    active = [t for t in closed_trades if t["side"] == side and t["entry_ts"] <= high["ts"] <= t["exit_ts"]]
                    if active and not any(abs(t["exit_ts"] - high["ts"]) <= 60 for t in active):
                        missed_exit_turns += 1

            ce_turns = turns_by_stream["CE"]
            pe_turns = turns_by_stream["PE"]
            fut_turns = turns_by_stream["FUT"]
            turn_row = {
                "ts": now,
                "time": datetime.fromtimestamp(now, self._tz).strftime("%H:%M:%S"),
                "window": "30m",
                "turns": {
                    "CE": {"total": len(ce_turns), "low": sum(1 for t in ce_turns if t["turn_type"] == "LOW"), "high": sum(1 for t in ce_turns if t["turn_type"] == "HIGH")},
                    "PE": {"total": len(pe_turns), "low": sum(1 for t in pe_turns if t["turn_type"] == "LOW"), "high": sum(1 for t in pe_turns if t["turn_type"] == "HIGH")},
                    "FUT": {"total": len(fut_turns), "low": sum(1 for t in fut_turns if t["turn_type"] == "LOW"), "high": sum(1 for t in fut_turns if t["turn_type"] == "HIGH")},
                },
                "trades_analyzed": len(closed_trades),
                "matched_entries": matched_entries,
                "matched_exits": matched_exits,
                "missed_entry_turns": missed_entry_turns,
                "missed_exit_turns": missed_exit_turns,
                "entry_quality_counts": dict(entry_quality_counts),
                "exit_quality_counts": dict(exit_quality_counts),
                "trade_reviews": trade_reviews,
            }
            self.recorder._write(self.LIVE_TURN_ANALYSIS_FILE, turn_row)
            with open(os.path.join(self.recorder.session_dir, "latest_live_turn_summary.json"), "w", encoding="utf-8") as f:
                json.dump(turn_row, f, ensure_ascii=False)

            if (len(ticks_by_stream["CE"]) < 10 and len(ticks_by_stream["PE"]) < 10) or (len(closed_trades) == 0 and len(ticks_rows) == 0):
                comment = "insufficient_data"
            elif len(closed_trades) > 0 and matched_entries == 0 and matched_exits == 0:
                comment = "no_turn_match"
            elif missed_entry_turns >= 3:
                comment = "many_missed_turns"
            elif entry_quality_counts.get("LATE_ENTRY", 0) > entry_quality_counts.get("TIMELY_ENTRY", 0):
                comment = "entries_late"
            elif entry_quality_counts.get("TIMELY_ENTRY", 0) >= entry_quality_counts.get("LATE_ENTRY", 0):
                comment = "entries_timely"
            elif exit_quality_counts.get("EARLY_EXIT", 0) > exit_quality_counts.get("TIMELY_EXIT", 0):
                comment = "exits_early"
            elif exit_reason_counts.get("RISK", 0) > 0:
                comment = "risk_exits_working"
            else:
                comment = "entries_timely"

            logger.info(
                "TRI_WAVE_LIVE_TURN_ANALYSIS | window=30m | ce_turns=%s pe_turns=%s fut_turns=%s | trades=%s | matched_entries=%s matched_exits=%s | missed_entry_turns=%s missed_exit_turns=%s | entry_quality=%s | exit_quality=%s | comment=%s",
                len(ce_turns),
                len(pe_turns),
                len(fut_turns),
                len(closed_trades),
                matched_entries,
                matched_exits,
                missed_entry_turns,
                missed_exit_turns,
                dict(entry_quality_counts),
                dict(exit_quality_counts),
                comment,
            )
        except Exception as exc:
            logger.exception("TRI_WAVE_LIVE_TURN_ANALYSIS_ERROR | error=%s", exc)
