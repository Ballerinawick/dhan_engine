import csv
import json
import os
from collections import Counter
from datetime import date as date_cls
from datetime import datetime
from statistics import mean
from zoneinfo import ZoneInfo


class TriWaveReplayAnalyzer:
    def __init__(self, session_dir: str):
        self.session_dir = session_dir
        self.report = {}
        self.skipped_bad_json_rows = 0
        self.skipped_bad_trade_rows = 0
        self.session_date = self._extract_session_date(session_dir)
        self.ist = ZoneInfo("Asia/Kolkata")
        self.ticks_by_stream = {"FUT": [], "CE": [], "PE": []}

    def _extract_session_date(self, path: str):
        for part in path.split(os.sep):
            try:
                return date_cls.fromisoformat(part)
            except ValueError:
                continue
        return None

    def _to_float(self, v, default=None):
        try:
            if v is None or v == "":
                return default
            return float(v)
        except (TypeError, ValueError):
            return default

    def load_jsonl(self, filename):
        path = os.path.join(self.session_dir, filename)
        if not os.path.exists(path):
            return []
        out = []
        with open(path, "r", encoding="utf-8") as f:
            for ln in f:
                if not ln.strip():
                    continue
                try:
                    out.append(json.loads(ln))
                except json.JSONDecodeError:
                    self.skipped_bad_json_rows += 1
        return out

    def _parse_ts(self, raw):
        ts = self._to_float(raw)
        if ts is not None and ts > 10_000:
            return ts
        return None

    def _hms_to_ts(self, raw_hms):
        if not raw_hms or not self.session_date:
            return None
        try:
            t = datetime.strptime(str(raw_hms), "%H:%M:%S").time()
            dt = datetime.combine(self.session_date, t, tzinfo=self.ist)
            return dt.timestamp()
        except ValueError:
            return None

    def _normalize_trade(self, row):
        tr = row.get("trade") if isinstance(row, dict) and isinstance(row.get("trade"), dict) else row
        if not isinstance(tr, dict):
            return None
        side = str(tr.get("side") or "").upper()
        if not side:
            tag = str(tr.get("tag", ""))
            side = "CE" if "CE" in tag else "PE" if "PE" in tag else ""

        entry_ts = self._parse_ts(tr.get("entry_ts")) or self._parse_ts(tr.get("EntryTs"))
        exit_ts = self._parse_ts(tr.get("exit_ts")) or self._parse_ts(tr.get("ExitTs"))
        entry_time = tr.get("entry_time") or tr.get("EntryTime")
        exit_time = tr.get("exit_time") or tr.get("ExitTime")
        if entry_ts is None:
            entry_ts = self._hms_to_ts(entry_time)
        if exit_ts is None:
            exit_ts = self._hms_to_ts(exit_time)
        if entry_ts is None or exit_ts is None:
            return None

        entry = self._to_float(tr.get("entry", tr.get("entry_price")), 0.0)
        exitp = self._to_float(tr.get("exit", tr.get("exit_price")), 0.0)
        gross_pnl = self._to_float(tr.get("gross_pnl"), 0.0)
        fee = self._to_float(tr.get("fee", tr.get("fees")), 0.0)
        net_pnl = self._to_float(tr.get("net_pnl"), gross_pnl - fee)
        hold_sec = self._to_float(tr.get("hold_sec"), max(0.0, exit_ts - entry_ts))
        return {
            "side": side,
            "entry_ts": entry_ts,
            "exit_ts": exit_ts,
            "entry_time": entry_time or datetime.fromtimestamp(entry_ts, self.ist).strftime("%H:%M:%S"),
            "exit_time": exit_time or datetime.fromtimestamp(exit_ts, self.ist).strftime("%H:%M:%S"),
            "entry": entry,
            "exit": exitp,
            "gross_pnl": gross_pnl,
            "fee": fee,
            "net_pnl": net_pnl,
            "hold_sec": hold_sec,
            "entry_reason": tr.get("entry_reason", ""),
            "exit_reason": tr.get("exit_reason", ""),
            "raw_trade": tr,
        }

    def _window(self, rows, start_ts, end_ts):
        return [r for r in rows if start_ts <= r.get("ts", 0) <= end_ts]

    def _nearest_state(self, ts, states):
        cand = [s for s in states if abs((s.get("ts") or 0) - ts) <= 2]
        if not cand:
            return None
        return min(cand, key=lambda s: abs((s.get("ts") or 0) - ts))

    def _entry_class(self, favorable_30, adverse_30, mfe, entry_pos, side, trade_ticks, entry):
        if not trade_ticks:
            return "UNKNOWN_ENTRY"
        if favorable_30 > adverse_30 and mfe > 0 and adverse_30 <= max(0.40, entry * 0.005):
            return "PERFECT_ENTRY"
        if adverse_30 > favorable_30 and mfe > 0:
            return "EARLY_ENTRY"
        if entry_pos > 0.75 and mfe <= max(0.75, entry * 0.01):
            return "LATE_ENTRY"
        if mfe <= 0 or adverse_30 > favorable_30:
            return "BAD_ENTRY"
        return "UNKNOWN_ENTRY"

    def _exit_class(self, post120, net_pnl, missed_120, adverse_120, giveback, entry, exitp):
        if not post120:
            return "UNKNOWN_EXIT"
        if net_pnl < 0 and missed_120 >= max(1.00, exitp * 0.015):
            return "BAD_EXIT"
        if net_pnl < 0 and adverse_120 >= max(1.00, exitp * 0.015):
            return "GOOD_RISK_EXIT"
        if missed_120 >= max(1.00, exitp * 0.015):
            return "EARLY_EXIT"
        if giveback >= max(1.00, entry * 0.015):
            return "LATE_EXIT"
        if (adverse_120 >= max(1.0, exitp * 0.01) and missed_120 < max(1.0, exitp * 0.01)) or (
            net_pnl > 0 and missed_120 < max(1.0, exitp * 0.01)
        ):
            return "PERFECT_EXIT"
        return "PERFECT_EXIT"

    def analyze(self):
        ticks = self.load_jsonl("ticks.jsonl")
        states = self.load_jsonl("states.jsonl")
        signals = self.load_jsonl("signals.jsonl")
        trades_rows = self.load_jsonl("trades.jsonl")
        portfolio = self.load_jsonl("portfolio.jsonl")

        for row in ticks:
            tick = row.get("tick") if isinstance(row, dict) and isinstance(row.get("tick"), dict) else row
            if not isinstance(tick, dict):
                continue
            stream = str(tick.get("stream", row.get("stream", ""))).upper()
            ts = self._parse_ts(tick.get("ts", row.get("ts")))
            ltp = self._to_float(tick.get("ltp", row.get("ltp")))
            if stream not in self.ticks_by_stream or ts is None or ltp is None:
                continue
            self.ticks_by_stream[stream].append({
                "ts": ts,
                "time": tick.get("time", row.get("time")),
                "stream": stream,
                "secid": tick.get("secid", row.get("secid")),
                "ltp": ltp,
                "features": tick.get("features") or row.get("features") or {},
            })

        for k in self.ticks_by_stream:
            self.ticks_by_stream[k].sort(key=lambda x: x["ts"])

        norm_states = []
        for s in states:
            ts = self._parse_ts((s.get("state") or {}).get("ts", s.get("ts")) if isinstance(s, dict) else None)
            body = s.get("state") if isinstance(s, dict) and isinstance(s.get("state"), dict) else s
            if ts is None or not isinstance(body, dict):
                continue
            body = dict(body)
            body["ts"] = ts
            norm_states.append(body)
        norm_states.sort(key=lambda x: x["ts"])
        signals.sort(key=lambda x: self._parse_ts(x.get("ts")) or 0)
        portfolio.sort(key=lambda x: self._parse_ts(x.get("ts")) or 0)

        analyzed = []
        for i, row in enumerate(trades_rows, 1):
            tr = self._normalize_trade(row)
            if tr is None:
                self.skipped_bad_trade_rows += 1
                continue
            side = tr["side"]
            target_ticks = self.ticks_by_stream.get(side, [])
            entry_ts = tr["entry_ts"]
            exit_ts = tr["exit_ts"]
            entry = tr["entry"]
            exitp = tr["exit"]

            pre = self._window(target_ticks, entry_ts - 120, entry_ts)
            tw = self._window(target_ticks, entry_ts, exit_ts)
            p30 = self._window(target_ticks, exit_ts, exit_ts + 30)
            p60 = self._window(target_ticks, exit_ts, exit_ts + 60)
            p120 = self._window(target_ticks, exit_ts, exit_ts + 120)
            p180 = self._window(target_ticks, exit_ts, exit_ts + 180)
            after30 = self._window(target_ticks, entry_ts, entry_ts + 30)
            after60 = self._window(target_ticks, entry_ts, entry_ts + 60)

            pre_low = min((x["ltp"] for x in pre), default=entry)
            pre_high = max((x["ltp"] for x in pre), default=entry)
            entry_pos = (entry - pre_low) / max(pre_high - pre_low, 1e-9)
            first_30_high = max((x["ltp"] for x in after30), default=entry)
            first_30_low = min((x["ltp"] for x in after30), default=entry)
            first_60_high = max((x["ltp"] for x in after60), default=entry)
            first_60_low = min((x["ltp"] for x in after60), default=entry)
            favorable_30 = first_30_high - entry
            adverse_30 = entry - first_30_low
            favorable_60 = first_60_high - entry
            adverse_60 = entry - first_60_low
            trade_peak = max((x["ltp"] for x in tw), default=entry)
            trade_trough = min((x["ltp"] for x in tw), default=entry)
            mfe = trade_peak - entry
            mae = trade_trough - entry

            first_fav_ts = next((x["ts"] for x in after60 if x["ltp"] > entry), None)
            max_fav_tick = max(tw, key=lambda x: x["ltp"], default=None)
            max_adv_tick = min(tw, key=lambda x: x["ltp"], default=None)
            t_first_fav = (first_fav_ts - entry_ts) if first_fav_ts else None
            t_max_fav = (max_fav_tick["ts"] - entry_ts) if max_fav_tick else None
            t_max_adv = (max_adv_tick["ts"] - entry_ts) if max_adv_tick else None

            post_30_high = max((x["ltp"] for x in p30), default=exitp)
            post_60_high = max((x["ltp"] for x in p60), default=exitp)
            post_120_high = max((x["ltp"] for x in p120), default=exitp)
            post_180_high = max((x["ltp"] for x in p180), default=exitp)
            post_30_low = min((x["ltp"] for x in p30), default=exitp)
            post_60_low = min((x["ltp"] for x in p60), default=exitp)
            post_120_low = min((x["ltp"] for x in p120), default=exitp)
            post_180_low = min((x["ltp"] for x in p180), default=exitp)

            missed_30 = post_30_high - exitp
            missed_60 = post_60_high - exitp
            missed_120 = post_120_high - exitp
            missed_180 = post_180_high - exitp
            adv_exit_30 = exitp - post_30_low
            adv_exit_60 = exitp - post_60_low
            adv_exit_120 = exitp - post_120_low
            adv_exit_180 = exitp - post_180_low
            giveback = trade_peak - exitp

            e_state = self._nearest_state(entry_ts, norm_states)
            x_state = self._nearest_state(exit_ts, norm_states)
            warnings = []
            if e_state is None or x_state is None:
                warnings.append("STATE_SNAPSHOT_MISSING")
            ce_edge = self._to_float((e_state or {}).get("ce_edge"), 0.0)
            pe_edge = self._to_float((e_state or {}).get("pe_edge"), 0.0)
            edge_vs = (ce_edge - pe_edge) if side == "CE" else (pe_edge - ce_edge)
            if edge_vs < 0:
                warnings.append("SELECTED_SIDE_EDGE_WEAKER_THAN_OPPOSITE")

            entry_class = self._entry_class(favorable_30, adverse_30, mfe, entry_pos, side, tw, entry)
            exit_class = self._exit_class(p120, tr["net_pnl"], missed_120, adv_exit_120, giveback, entry, exitp)

            chart_rows = []
            chart_start, chart_end = entry_ts - 120, exit_ts + 180
            for stream in ["FUT", "CE", "PE"]:
                for tk in self._window(self.ticks_by_stream[stream], chart_start, chart_end):
                    st = self._nearest_state(tk["ts"], norm_states) or {}
                    if stream == "CE":
                        phase, support, risk, edge = st.get("ce_phase"), st.get("ce_support"), st.get("ce_risk"), st.get("ce_edge")
                    elif stream == "PE":
                        phase, support, risk, edge = st.get("pe_phase"), st.get("pe_support"), st.get("pe_risk"), st.get("pe_edge")
                    else:
                        phase, support, risk, edge = st.get("fut_phase"), None, None, None
                    chart_rows.append({
                        "ts": tk["ts"], "time": tk.get("time"), "stream": stream, "ltp": tk["ltp"],
                        "phase": phase, "support": support, "risk": risk, "edge": edge,
                        "is_entry_marker": abs(tk["ts"] - entry_ts) <= 0.5 and stream == side,
                        "is_exit_marker": abs(tk["ts"] - exit_ts) <= 0.5 and stream == side,
                    })
            chart_rows.sort(key=lambda x: (x["ts"], x["stream"]))
            charts_dir = os.path.join(self.session_dir, "charts")
            os.makedirs(charts_dir, exist_ok=True)
            base = f"trade_{i:03d}"
            with open(os.path.join(charts_dir, f"{base}.json"), "w", encoding="utf-8") as f:
                json.dump(chart_rows, f, indent=2)
            with open(os.path.join(charts_dir, f"{base}.csv"), "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=list(chart_rows[0].keys()) if chart_rows else ["ts", "time", "stream", "ltp", "phase", "support", "risk", "edge", "is_entry_marker", "is_exit_marker"])
                w.writeheader()
                if chart_rows:
                    w.writerows(chart_rows)

            analyzed.append({
                "trade_number": i, "side": side, **tr,
                "entry_quality": entry_class, "exit_quality": exit_class,
                "warnings": warnings,
                "selected_side_edge_vs_opposite": edge_vs,
                "entry_state": e_state, "exit_state": x_state,
                "metrics": {
                    "mfe": mfe, "mae": mae, "favorable_30s_points": favorable_30, "adverse_30s_points": adverse_30,
                    "favorable_60s_points": favorable_60, "adverse_60s_points": adverse_60,
                    "missed_profit_30": missed_30, "missed_profit_60": missed_60, "missed_profit_120": missed_120, "missed_profit_180": missed_180,
                    "adverse_after_exit_30": adv_exit_30, "adverse_after_exit_60": adv_exit_60, "adverse_after_exit_120": adv_exit_120, "adverse_after_exit_180": adv_exit_180,
                    "profit_giveback_points": giveback, "time_to_first_favorable_tick": t_first_fav,
                    "time_to_max_favorable": t_max_fav, "time_to_max_adverse": t_max_adv,
                },
            })

        pnls = [t["net_pnl"] for t in analyzed]
        holds = [t["hold_sec"] for t in analyzed]
        entry_dist = dict(Counter(t["entry_quality"] for t in analyzed))
        exit_dist = dict(Counter(t["exit_quality"] for t in analyzed))
        exit_reason_dist = dict(Counter((t.get("exit_reason") or "UNKNOWN") for t in analyzed))
        wins = sum(1 for x in pnls if x > 0)
        losses = sum(1 for x in pnls if x < 0)
        self.report = {
            "session_dir": self.session_dir,
            "total_trades": len(analyzed), "net_pnl": sum(pnls), "gross_pnl": sum(t["gross_pnl"] for t in analyzed),
            "fees_paid": sum(t["fee"] for t in analyzed), "win_rate": (wins / len(pnls) * 100) if pnls else 0.0,
            "avg_hold_sec": mean(holds) if holds else 0.0, "win_count": wins, "loss_count": losses,
            "churn_ratio": (len(analyzed) / max(len(portfolio), 1)) if portfolio else None,
            "entry_quality_distribution": entry_dist, "exit_quality_distribution": exit_dist,
            "exit_reason_distribution": exit_reason_dist,
            "early_entries": entry_dist.get("EARLY_ENTRY", 0), "late_entries": entry_dist.get("LATE_ENTRY", 0), "bad_entries": entry_dist.get("BAD_ENTRY", 0),
            "early_exits": exit_dist.get("EARLY_EXIT", 0), "late_exits": exit_dist.get("LATE_EXIT", 0), "bad_exits": exit_dist.get("BAD_EXIT", 0), "good_risk_exits": exit_dist.get("GOOD_RISK_EXIT", 0),
            "skipped_bad_json_rows": self.skipped_bad_json_rows, "skipped_bad_trade_rows": self.skipped_bad_trade_rows,
            "trades": analyzed,
            "worst_5_trades": sorted(analyzed, key=lambda x: x["net_pnl"])[:5],
            "top_5_early_exits": sorted([t for t in analyzed if t["exit_quality"] == "EARLY_EXIT"], key=lambda x: x["metrics"]["missed_profit_120"], reverse=True)[:5],
            "top_5_late_entries": sorted([t for t in analyzed if t["entry_quality"] == "LATE_ENTRY"], key=lambda x: x["metrics"]["mfe"])[:5],
        }
        return self.report

    def write_reports(self):
        if not self.report:
            self.analyze()
        os.makedirs(self.session_dir, exist_ok=True)
        json_path = os.path.join(self.session_dir, "analysis_report.json")
        md_path = os.path.join(self.session_dir, "analysis_report.md")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(self.report, f, indent=2)

        lines = ["# TriWave Replay Analysis", "", "## Summary", f"- Total trades: {self.report['total_trades']}", f"- Net pnl: {self.report['net_pnl']:.2f}", f"- Win rate: {self.report['win_rate']:.2f}%", f"- Fees paid: {self.report['fees_paid']:.2f}", f"- Avg hold sec: {self.report['avg_hold_sec']:.2f}", f"- Entry quality counts: {self.report['entry_quality_distribution']}", f"- Exit quality counts: {self.report['exit_quality_distribution']}", f"- Exit reason distribution: {self.report['exit_reason_distribution']}", ""]
        for t in self.report.get("trades", []):
            m = t["metrics"]
            comment = "Trade was valid; entry and exit aligned with wave movement."
            if t["entry_quality"] == "EARLY_ENTRY":
                comment = "Entry was early; premium dropped first before recovery."
            elif t["entry_quality"] == "LATE_ENTRY":
                comment = "Entry was late near local high."
            elif t["exit_quality"] == "EARLY_EXIT":
                comment = "Exit was early; premium continued after exit."
            elif t["exit_quality"] == "GOOD_RISK_EXIT":
                comment = "Exit protected capital; premium continued adverse."
            elif "SELECTED_SIDE_EDGE_WEAKER_THAN_OPPOSITE" in t.get("warnings", []):
                comment = "Trade should be avoided; selected side edge was weaker than opposite."
            lines.extend([
                f"## Trade #{t['trade_number']}", f"Side: {t['side']}", f"Entry time: {t['entry_time']}", f"Exit time: {t['exit_time']}",
                f"Entry: {t['entry']}", f"Exit: {t['exit']}", f"Net: {t['net_pnl']}", f"Hold: {t['hold_sec']}",
                f"Entry reason: {t['entry_reason']}", f"Exit reason: {t['exit_reason']}", f"Entry quality: {t['entry_quality']}", f"Exit quality: {t['exit_quality']}",
                f"MFE: {m['mfe']}", f"MAE: {m['mae']}", f"Favorable 30s: {m['favorable_30s_points']}", f"Adverse 30s: {m['adverse_30s_points']}",
                f"Missed profit 120s: {m['missed_profit_120']}", f"Adverse after exit 120s: {m['adverse_after_exit_120']}", f"Profit giveback: {m['profit_giveback_points']}",
                f"Selected side edge vs opposite: {t['selected_side_edge_vs_opposite']}", f"Warnings: {', '.join(t.get('warnings', [])) or 'None'}", f"Comment: {comment}", ""
            ])
        with open(md_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return json_path, md_path
