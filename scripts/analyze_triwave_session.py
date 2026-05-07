import argparse
import os
from statistics import mean

from dhan_engine.analytics.tri_wave_replay_analyzer import TriWaveReplayAnalyzer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None)
    args = parser.parse_args()
    date = args.date
    if not date:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        date = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")
    session_dir = os.path.join("data", "triwave_sessions", date)
    analyzer = TriWaveReplayAnalyzer(session_dir)
    report = analyzer.analyze()
    j, m = analyzer.write_reports()
    trades = report.get("trades", [])
    pnls = [float((t.get("trade", {}) or {}).get("net_pnl", 0) or 0) for t in trades]
    wins = sum(1 for p in pnls if p > 0)
    holds = [float((t.get("trade", {}) or {}).get("hold_sec", 0) or 0) for t in trades]
    entry_classes = [t.get("entry_class") for t in trades]
    exit_classes = [t.get("exit_class") for t in trades]
    print(f"session path: {session_dir}")
    print(f"total trades: {len(trades)}")
    print(f"total net pnl: {sum(pnls):.2f}")
    print(f"win rate: {(wins/len(trades)*100.0) if trades else 0.0:.2f}%")
    print(f"avg hold: {mean(holds) if holds else 0.0:.2f}")
    print(f"early entries: {entry_classes.count('EARLY_ENTRY')}")
    print(f"late entries: {entry_classes.count('LATE_ENTRY')}")
    print(f"bad entries: {entry_classes.count('BAD_ENTRY')}")
    print(f"early exits: {exit_classes.count('EARLY_EXIT')}")
    print(f"late exits: {exit_classes.count('LATE_EXIT')}")
    print(f"good risk exits: {exit_classes.count('GOOD_RISK_EXIT')}")
    worst = sorted(trades, key=lambda t: float((t.get('trade',{}) or {}).get('net_pnl',0) or 0))[:5]
    early = sorted([t for t in trades if t.get('exit_class') == 'EARLY_EXIT'], key=lambda t: float((t.get('metrics',{}) or {}).get('exit_to_future_peak_points',0) or 0), reverse=True)[:5]
    print("top 5 worst trades:")
    for t in worst:
        print(t.get("trade", {}))
    print("top 5 early exits by missed points:")
    for t in early:
        print(t.get("trade", {}), t.get("metrics", {}).get("exit_to_future_peak_points"))
    print(f"TRI_WAVE_REPLAY_ANALYSIS_DONE | report={j}")


if __name__ == "__main__":
    main()
