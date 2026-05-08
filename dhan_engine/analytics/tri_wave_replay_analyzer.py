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
        with open(path, "r", encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]

    def _entry_class(self, move30, edge_adv):
        if move30 is None:
            return "UNKNOWN_ENTRY"
        if edge_adv is not None and edge_adv < 0:
            return "BAD_ENTRY"
        if move30 > 0:
            return "PERFECT_ENTRY"
        if move30 < -0.8:
            return "EARLY_ENTRY"
        if -0.2 <= move30 <= 0.2:
            return "LATE_ENTRY"
        return "UNKNOWN_ENTRY"

    def _exit_class(self, p30, p60, p120, pnl):
        if p30 is None and p60 is None and p120 is None:
            return "UNKNOWN_EXIT"
        mx = max([x for x in [p30, p60, p120] if x is not None], default=0)
        mn = min([x for x in [p30, p60, p120] if x is not None], default=0)
        if mx > 1.2:
            return "EARLY_EXIT"
        if mn < -1.0:
            return "GOOD_RISK_EXIT"
        if pnl < 0 and mx > 1.0:
            return "BAD_EXIT"
        return "PERFECT_EXIT"

    def analyze(self):
        ticks = self.load_jsonl("ticks.jsonl")
        trades_rows = self.load_jsonl("trades.jsonl")
        by_side = {"CE": [], "PE": [], "FUT": []}
        for t in ticks:
            s = t.get("stream")
            if s in by_side:
                by_side[s].append(t)
        for s in by_side.values():
            s.sort(key=lambda x: float(x.get("ts", 0) or 0))

        analyzed = []
        for i, row in enumerate(trades_rows, 1):
            tr = row.get("trade", {})
            side = str(tr.get("side") or ("CE" if "CE" in str(tr.get("tag", "")) else "PE" if "PE" in str(tr.get("tag", "")) else ""))
            entry_ts = float(tr.get("entry_ts", tr.get("entry_time", 0)) or 0)
            exit_ts = float(tr.get("exit_ts", tr.get("exit_time", row.get("ts", 0))) or 0)
            entry = float(tr.get("entry", tr.get("entry_price", 0)) or 0)
            exitp = float(tr.get("exit", tr.get("exit_price", 0)) or 0)
            hold = [x for x in by_side.get(side, []) if entry_ts <= float(x.get("ts", 0) or 0) <= exit_ts]
            pre30 = [x for x in by_side.get(side, []) if entry_ts - 30 <= float(x.get("ts", 0) or 0) < entry_ts]
            post30 = [x for x in by_side.get(side, []) if exit_ts < float(x.get("ts", 0) or 0) <= exit_ts + 30]
            post60 = [x for x in by_side.get(side, []) if exit_ts < float(x.get("ts", 0) or 0) <= exit_ts + 60]
            post120 = [x for x in by_side.get(side, []) if exit_ts < float(x.get("ts", 0) or 0) <= exit_ts + 120]

            prices = [float(x.get("ltp", 0) or 0) for x in hold] if hold else []
            mfe = (max(prices) - entry) if prices and entry else 0.0
            mae = (min(prices) - entry) if prices and entry else 0.0
            p30 = ((max([float(x.get("ltp", 0) or 0) for x in post30], default=exitp) - exitp) / exitp * 100.0) if exitp and post30 else None
            p60 = ((max([float(x.get("ltp", 0) or 0) for x in post60], default=exitp) - exitp) / exitp * 100.0) if exitp and post60 else None
            p120 = ((max([float(x.get("ltp", 0) or 0) for x in post120], default=exitp) - exitp) / exitp * 100.0) if exitp and post120 else None
            move30 = ((pre30[-1].get("ltp", entry) - pre30[0].get("ltp", entry)) / entry * 100.0) if pre30 and entry else None
            edge_adv = None
            if hold:
                f = hold[0].get("features", {}) or {}
                this_edge = float(f.get("edge", 0) or 0)
                opp_edge = float(f.get("opp_edge", 0) or 0)
                edge_adv = this_edge - opp_edge

            pnl = float(tr.get("net_pnl", 0) or 0)
            entry_class = self._entry_class(move30, edge_adv)
            exit_class = self._exit_class(p30, p60, p120, pnl)
            comment = "Entry was good; price moved favorable quickly." if entry_class == "PERFECT_ENTRY" else "Entry was early; price dropped first before recovery." if entry_class == "EARLY_ENTRY" else "Trade should be avoided; selected side support was weaker than opposite." if entry_class == "BAD_ENTRY" else "Entry quality uncertain."
            if exit_class == "EARLY_EXIT":
                comment += " Exit was early; premium continued higher after exit."
            elif exit_class == "GOOD_RISK_EXIT":
                comment += " Exit protected capital; premium continued adverse."

            analyzed.append({
                "trade_number": i,
                "trade": tr,
                "side": side,
                "entry_class": entry_class,
                "exit_class": exit_class,
                "comment": comment,
                "metrics": {
                    "mfe_points": mfe,
                    "mae_points": mae,
                    "move_30s_before_entry_pct": move30,
                    "post_exit_30s_pct": p30,
                    "post_exit_60s_pct": p60,
                    "post_exit_120s_pct": p120,
                    "missed_favorable_points": (max([float(x.get("ltp", 0) or 0) for x in post120], default=exitp) - exitp) if post120 else None,
                    "adverse_protection_points": (exitp - min([float(x.get("ltp", 0) or 0) for x in post120], default=exitp)) if post120 else None,
                    "selected_side_edge_vs_opposite": edge_adv,
                },
            })

        fees_paid = sum(float((t.get("trade", {}) or {}).get("fees", 0) or 0) for t in analyzed)
        pnls = [float((t.get("trade", {}) or {}).get("net_pnl", 0) or 0) for t in analyzed]
        holds = [float((t.get("trade", {}) or {}).get("hold_sec", 0) or 0) for t in analyzed]
        exit_reason_dist = {}
        entry_q = {}
        exit_q = {}
        for t in analyzed:
            r = str((t.get("trade", {}) or {}).get("exit_reason", "UNKNOWN") or "UNKNOWN")
            exit_reason_dist[r] = exit_reason_dist.get(r, 0) + 1
            entry_q[t["entry_class"]] = entry_q.get(t["entry_class"], 0) + 1
            exit_q[t["exit_class"]] = exit_q.get(t["exit_class"], 0) + 1

        self.report = {
            "session_dir": self.session_dir,
            "total_trades": len(analyzed),
            "net_pnl": sum(pnls),
            "win_rate": (sum(1 for x in pnls if x > 0) / len(pnls) * 100.0) if pnls else 0.0,
            "fees_paid": fees_paid,
            "avg_hold_sec": mean(holds) if holds else 0.0,
            "churn_ratio": (len(analyzed) / max(len(ticks), 1)),
            "exit_reason_distribution": exit_reason_dist,
            "entry_quality_distribution": entry_q,
            "exit_quality_distribution": exit_q,
            "top_5_worst_trades": sorted(analyzed, key=lambda x: float((x.get("trade", {}) or {}).get("net_pnl", 0) or 0))[:5],
            "top_5_early_exits": [t for t in analyzed if t.get("exit_class") == "EARLY_EXIT"][:5],
            "top_5_late_entries": [t for t in analyzed if t.get("entry_class") == "LATE_ENTRY"][:5],
            "tomorrow_tuning": "Reduce entries with weak edge advantage and review EARLY_EXIT cases for better trailing.",
            "trades": analyzed,
        }
        return self.report

    def write_reports(self):
        if not self.report:
            self.analyze()
        json_path = os.path.join(self.session_dir, "analysis_report.json")
        md_path = os.path.join(self.session_dir, "analysis_report.md")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(self.report, f, indent=2)

        lines = [
            "# TriWave Replay Analysis", "",
            f"Session: `{self.session_dir}`", "",
            f"Total trades: **{self.report.get('total_trades', 0)}**",
            f"Net pnl: **{self.report.get('net_pnl', 0.0):.2f}**",
            f"Win rate: **{self.report.get('win_rate', 0.0):.2f}%**",
            f"Fees paid: **{self.report.get('fees_paid', 0.0):.2f}**",
            f"Avg hold (sec): **{self.report.get('avg_hold_sec', 0.0):.2f}**",
            "",
            f"Tomorrow tune: {self.report.get('tomorrow_tuning', '')}", "",
        ]
        for t in self.report.get("trades", []):
            tr = t.get("trade", {})
            lines.append(
                f"## Trade {t.get('trade_number')}\n"
                f"- Side: {t.get('side')}\n"
                f"- Entry: {tr.get('entry')} @ {tr.get('entry_ts')}\n"
                f"- Exit: {tr.get('exit')} @ {tr.get('exit_ts')}\n"
                f"- Net pnl: {tr.get('net_pnl')}\n"
                f"- Hold sec: {tr.get('hold_sec')}\n"
                f"- Entry reason: {tr.get('entry_reason')}\n"
                f"- Exit reason: {tr.get('exit_reason')}\n"
                f"- Entry class: {t.get('entry_class')}\n"
                f"- Exit class: {t.get('exit_class')}\n"
                f"- Comment: {t.get('comment')}\n"
            )
        with open(md_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return json_path, md_path
