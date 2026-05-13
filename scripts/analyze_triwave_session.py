import argparse
import os

from dhan_engine.analytics.tri_wave_replay_analyzer import TriWaveReplayAnalyzer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None)
    parser.add_argument("--expiry", default="unknown")
    parser.add_argument("--base-dir", default=os.getenv("TRIWAVE_SESSION_BASE_DIR", "data/triwave_sessions"))
    args = parser.parse_args()

    if not args.date:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        date = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")
    else:
        date = args.date

    session_dir = os.path.join(args.base_dir, date, f"expiry={args.expiry}")
    analyzer = TriWaveReplayAnalyzer(session_dir)
    report = analyzer.analyze()
    j, m = analyzer.write_reports()

    print(f"session path: {session_dir}")
    print(f"analysis_report.json: {j}")
    print(f"analysis_report.md: {m}")
    print(f"total trades: {report.get('total_trades', 0)}")
    print(f"total net pnl: {float(report.get('net_pnl', 0.0) or 0.0):.2f}")
    print(f"win rate: {float(report.get('win_rate', 0.0) or 0.0):.2f}%")
    print(f"avg hold: {float(report.get('avg_hold_sec', 0.0) or 0.0):.2f}")
    print(f"entry quality distribution: {report.get('entry_quality_distribution', {})}")
    print(f"exit quality distribution: {report.get('exit_quality_distribution', {})}")
    print("worst 5 trades by net pnl:")
    for t in report.get("worst_5_trades", []):
        print(f"  trade#{t.get('trade_number')} side={t.get('side')} net={float(t.get('net_pnl', 0.0) or 0.0):.2f}")
    print(f"early exit count: {int(report.get('early_exits', 0) or 0)}")
    print(f"bad entry count: {int(report.get('bad_entries', 0) or 0)}")
    print(f"TRI_WAVE_REPLAY_ANALYSIS_DONE | report={j}")


if __name__ == "__main__":
    main()
