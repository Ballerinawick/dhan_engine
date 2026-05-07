import argparse
import json
import os
from statistics import mean


class TriWaveReplayAnalyzer:
    def __init__(self, session_dir: str):
        self.session_dir = session_dir
        self.report = {}

    def load_jsonl(self, filename):
        path = os.path.join(self.session_dir, filename)
        if not os.path.exists(path):
            return []
        rows = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    def _classify_entry(self, mae_pct, mfe_pct, first_30):
        if not first_30:
            return "UNKNOWN_ENTRY"
        if mfe_pct > 0 and mae_pct >= -1.0:
            return "PERFECT_ENTRY"
        if mae_pct < -1.0 and mfe_pct > 0:
            return "EARLY_ENTRY"
        if mfe_pct < 0.4:
            return "LATE_ENTRY"
        if mae_pct < -2.0 and mfe_pct < 0.5:
            return "BAD_ENTRY"
        return "UNKNOWN_ENTRY"

    def _classify_exit(self, post_best_pct, post_worst_pct, giveback_pct, pnl_pct):
        if post_best_pct is None:
            return "UNKNOWN_EXIT"
        if post_best_pct > 1.5:
            return "EARLY_EXIT"
        if post_worst_pct < -1.0:
            return "GOOD_RISK_EXIT"
        if giveback_pct > 2.0:
            return "LATE_EXIT"
        if pnl_pct < 0 and post_best_pct > 1.2:
            return "BAD_EXIT"
        return "PERFECT_EXIT"

    def analyze(self):
        ticks = self.load_jsonl("ticks.jsonl")
        trades = self.load_jsonl("trades.jsonl")
        by_side = {"CE": [], "PE": []}
        for t in ticks:
            if t.get("stream") in by_side:
                by_side[t["stream"]].append(t)
        for s in by_side.values():
            s.sort(key=lambda x: x.get("ts", 0))

        analyzed = []
        for row in trades:
            tr = row.get("trade", {})
            side = str(tr.get("side") or ("CE" if "CE" in str(tr.get("tag", "")) else "PE" if "PE" in str(tr.get("tag", "")) else ""))
            if side not in by_side:
                continue
            entry_ts = float(tr.get("entry_ts", tr.get("entry_time", 0)) or 0)
            exit_ts = float(tr.get("exit_ts", tr.get("exit_time", row.get("ts", 0))) or 0)
            entry = float(tr.get("entry", tr.get("entry_price", 0)) or 0)
            exitp = float(tr.get("exit", tr.get("exit_price", 0)) or 0)
            if entry <= 0:
                continue
            hold_ticks = [x for x in by_side[side] if entry_ts <= x.get("ts", 0) <= exit_ts]
            post_ticks = [x for x in by_side[side] if exit_ts < x.get("ts", 0) <= exit_ts + 120]
            if not hold_ticks:
                entry_class, exit_class = "UNKNOWN_ENTRY", "UNKNOWN_EXIT"
                metrics = {}
            else:
                prices = [float(x.get("ltp", 0) or 0) for x in hold_ticks]
                post_prices = [float(x.get("ltp", 0) or 0) for x in post_ticks] or []
                peak = max(prices); worst = min(prices)
                post_peak = max(post_prices) if post_prices else None
                post_worst = min(post_prices) if post_prices else None
                mfe_points = peak - entry
                mae_points = worst - entry
                mfe_pct = (mfe_points / entry) * 100.0
                mae_pct = (mae_points / entry) * 100.0
                first_30 = [x for x in by_side[side] if entry_ts <= x.get("ts", 0) <= entry_ts + 30]
                giveback_pct = ((peak - exitp) / entry) * 100.0 if entry else 0.0
                post_best_pct = (((post_peak - exitp) / exitp) * 100.0) if (post_peak and exitp) else None
                post_worst_pct = (((post_worst - exitp) / exitp) * 100.0) if (post_worst and exitp) else None
                pnl_pct = ((exitp - entry) / entry) * 100.0
                entry_class = self._classify_entry(mae_pct, mfe_pct, first_30)
                exit_class = self._classify_exit(post_best_pct, post_worst_pct, giveback_pct, pnl_pct)
                metrics = {
                    "entry_to_peak_points": mfe_points,
                    "entry_to_peak_pct": mfe_pct,
                    "entry_to_worst_points": mae_points,
                    "entry_to_worst_pct": mae_pct,
                    "exit_to_future_peak_points": (post_peak - exitp) if post_peak is not None else None,
                    "exit_to_future_peak_pct": post_best_pct,
                    "exit_to_future_worst_points": (post_worst - exitp) if post_worst is not None else None,
                    "exit_to_future_worst_pct": post_worst_pct,
                    "mfe_points": mfe_points,
                    "mae_points": mae_points,
                }
            analyzed.append({"trade": tr, "entry_class": entry_class, "exit_class": exit_class, "metrics": metrics})

        self.report = {"session_dir": self.session_dir, "total_trades": len(analyzed), "trades": analyzed}
        return self.report

    def write_reports(self):
        if not self.report:
            self.analyze()
        json_path = os.path.join(self.session_dir, "analysis_report.json")
        md_path = os.path.join(self.session_dir, "analysis_report.md")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(self.report, f, indent=2)
        lines = [f"# TriWave Replay Analysis\n", f"Session: `{self.session_dir}`\n", f"Total trades: **{self.report.get('total_trades',0)}**\n"]
        for i, t in enumerate(self.report.get("trades", []), 1):
            lines.append(f"## Trade {i}\n- Entry: {t['entry_class']}\n- Exit: {t['exit_class']}\n- Metrics: `{t['metrics']}`\n")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return json_path, md_path
