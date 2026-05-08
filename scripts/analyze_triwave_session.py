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
    j, _ = analyzer.write_reports()

    print(f"session path: {session_dir}")
    print(f"total trades: {report.get('total_trades', 0)}")
    print(f"total net pnl: {float(report.get('net_pnl', 0.0) or 0.0):.2f}")
    print(f"win rate: {float(report.get('win_rate', 0.0) or 0.0):.2f}%")
    print(f"avg hold: {float(report.get('avg_hold_sec', 0.0) or 0.0):.2f}")
    print(f"TRI_WAVE_REPLAY_ANALYSIS_DONE | report={j}")


if __name__ == "__main__":
    main()
