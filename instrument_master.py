# run_ws_9_instruments.py
import os
import time
import csv
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from dhan_marketfeed_ws import DhanLiveMarketFeedWS
from dhan_depth20_ws import DhanTwentyDepthWS

from tick_filter import TickFilter
from quant_processor import QuantProcessor
from microstructure_state import MicrostructureState
from signal_engine import SignalEngine
from depth_micro_features import DepthMicroFeatureBuilder


# Allow override in Railway
CSV_FILE = os.getenv("CSV_FILE", "api-scrip-master.csv")

INDEXES = ["NIFTY", "BANKNIFTY", "FINNIFTY"]
STRIKE_STEP = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50}

# Options depth segment (Dhan docs for derivatives depth commonly NSE_FNO)
OPT_EXCHANGE_SEGMENT_20D = "NSE_FNO"

SWITCH_COOLDOWN_SEC = 8.0
HEARTBEAT_SEC = 30.0


def compute_itm_strikes(ltp: float, step: int):
    # ATM rounding
    atm = int(round(ltp / step) * step)
    # 1 ITM CE and 1 ITM PE around ATM (your current design)
    return atm, atm - step, atm + step


def main():
    token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
    client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
    if not token or not client_id:
        raise RuntimeError("Missing DHAN_ACCESS_TOKEN / DHAN_CLIENT_ID (set in environment variables)")

    master = InstrumentMaster(CSV_FILE)

    tick_filter = TickFilter()
    quant = QuantProcessor()
    micro = MicrostructureState()
    signal_engine = SignalEngine()
    feature_builder = DepthMicroFeatureBuilder()

    # ---------- CSV LOGGER SETUP ----------
    Path("logs").mkdir(exist_ok=True)
    log_file = f"logs/market_ticks_{datetime.now().strftime('%Y-%m-%d')}.csv"

    csv_fp = open(log_file, "a", newline="")
    csv_writer = csv.writer(csv_fp)

    if csv_fp.tell() == 0:
        csv_writer.writerow([
            "timestamp", "tag", "ltp", "flow",
            "volume", "vol_delta",
            "oi", "oi_delta",
            "bid_qty", "ask_qty",
            "absorption_flag", "absorption_strength",
            "vacuum_flag",
            "delta", "theta",
            "signal"
        ])

    print("\n🚀 LIVE MODE (CSV + FUT-from-CSV)")
    print("1) FUT LTP  -> MarketFeed WS (FULL packet)")
    print("2) OPTIONS -> ONE TwentyDepth WS (20 depth)\n")

    fut_ltp = {k: 0.0 for k in INDEXES}
    current = {idx: {"atm": None, "last_switch": 0.0} for idx in INDEXES}
    latest_sig = {}

    last_heartbeat = 0.0

    # ---------- OPTION DEPTH CALLBACK ----------
    def on_opt_depth(secid: int, tag: str, bid, ask):
        try:
            micro_raw = feature_builder.build(secid, bid, ask)
            tick = tick_filter.extract({**micro_raw, "tag": tag})
            q = quant.compute(tick)
            q = micro.update(q)
            sig = signal_engine.generate(q)

            latest_sig[tag] = sig

            # -------- CSV LOG --------
            csv_writer.writerow([
                datetime.now().strftime("%H:%M:%S"),
                tag,
                q.get("ltp", 0),
                q.get("flow", 0),
                q.get("volume", 0),
                q.get("vol_delta", 0),
                q.get("oi", 0),
                q.get("oi_delta", 0),
                q.get("bid_qty", 0),
                q.get("ask_qty", 0),
                q.get("absorption_flag"),
                q.get("absorption_strength"),
                q.get("vacuum_flag"),
                q.get("delta", 0),
                q.get("theta", 0),
                sig
            ])
            csv_fp.flush()

            # -------- CONSOLE (only if signal interesting) --------
            if sig != "HOLD":
                print(f"{datetime.now().strftime('%H:%M:%S')} | {tag} → {sig}")

        except Exception as e:
            # Never crash WS thread
            print("❌ on_opt_depth error:", str(e))

    depth20 = DhanTwentyDepthWS(
        token=token,
        client_id=client_id,
        auth_type=2,
        exchange_segment=OPT_EXCHANGE_SEGMENT_20D,
        on_depth=on_opt_depth,
        debug=False
    )
    depth20.connect()
    print("✅ 20Depth WS started (OPTIONS only)")

    # ---------- FUT MARKETFEED CALLBACK ----------
    def on_fut_full(secid: int, tag: str, ltp: float, depth5):
        idx = tag.replace("_FUT", "")
        if idx in fut_ltp:
            fut_ltp[idx] = float(ltp)

    mfeed = DhanLiveMarketFeedWS(
        token=token,
        client_id=client_id,
        auth_type=2,
        on_full=on_fut_full,
        debug=False
    )
    mfeed.connect()

    # ✅ Build FUT subscriptions dynamically from CSV (no hardcode)
    fut_subs = []
    fut_seg = master.get_fut_exchange_segment()  # "NSE_FNO"

    for idx in INDEXES:
        fut_id = master.get_current_fut_security_id(idx)  # current active FUTIDX securityId
        fut_subs.append({
            "ExchangeSegment": fut_seg,
            "SecurityId": str(fut_id),
            "tag": f"{idx}_FUT"
        })

    mfeed.subscribe_full(fut_subs)

    print("✅ Subscribed FUTs (from CSV):")
    for s in fut_subs:
        print("   ", s)

    print("\n⏳ Waiting for FUT LTP ticks...\n")

    # ---------- MAIN LOOP ----------
    while True:
        now_ts = time.time()

        # Heartbeat so Railway logs keep showing life
        if now_ts - last_heartbeat >= HEARTBEAT_SEC:
            last_heartbeat = now_ts
            fut_state = " | ".join([f"{k}:{fut_ltp[k]:.2f}" for k in INDEXES])
            print(f"🫀 {datetime.now().strftime('%H:%M:%S')} HEARTBEAT | FUT_LTP => {fut_state}")

        for idx in INDEXES:
            ltp = fut_ltp[idx]
            if ltp <= 0:
                continue

            step = STRIKE_STEP[idx]
            atm, ce_strike, pe_strike = compute_itm_strikes(ltp, step)

            # switch only when ATM changes and cooldown passed
            if (
                current[idx]["atm"] != atm
                and (now_ts - current[idx]["last_switch"]) >= SWITCH_COOLDOWN_SEC
            ):
                current[idx]["atm"] = atm
                current[idx]["last_switch"] = now_ts

                expiry = master.get_nearest_option_expiry(idx)
                ce_secid = master.find_option_security_id(idx, expiry, ce_strike, "CE")
                pe_secid = master.find_option_security_id(idx, expiry, pe_strike, "PE")

                # Subscribe options to 20-depth
                depth20.subscribe([
                    {"SecurityId": str(ce_secid), "tag": f"{idx}_CE"},
                    {"SecurityId": str(pe_secid), "tag": f"{idx}_PE"},
                ])

                # ✅ Print strike AND securityId (so no confusion)
                print(
                    f"🔁 [{idx}] FUT_LTP:{ltp:.2f} | ATM:{atm} | "
                    f"CE_STRIKE:{ce_strike} (secid:{ce_secid}) | "
                    f"PE_STRIKE:{pe_strike} (secid:{pe_secid})"
                )

        time.sleep(0.2)


if __name__ == "__main__":
    main()