import os
import time
import csv
import json
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from instrument_master import InstrumentMaster
from dhan_depth20_ws import DhanTwentyDepthWS
from ltp_rest_engine import DhanLtpRestEngine

from tick_filter import TickFilter
from quant_processor import QuantProcessor
from microstructure_state import MicrostructureState
from signal_engine import SignalEngine
from depth_micro_features import DepthMicroFeatureBuilder


# ---------------- CONFIG ----------------
CSV_FILE = os.getenv("CSV_FILE", "api-scrip-master.csv")

INDEXES = ["NIFTY", "BANKNIFTY", "FINNIFTY"]
STRIKE_STEP = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50}

OPT_EXCHANGE_SEGMENT_20D = "NSE_FNO"

REST_POLL_INTERVAL_SEC = 1.2
REST_MAX_WAIT_SEC = 45

HEARTBEAT_SEC = 30.0

PRINT_RAW_JSON = True
RAW_JSON_THROTTLE_SEC = 1.0


def compute_itm_strikes(ltp: float, step: int):
    atm = int(round(ltp / step) * step)
    return atm, atm - step, atm + step


def main():
    token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
    client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
    if not token or not client_id:
        raise RuntimeError("Missing DHAN_ACCESS_TOKEN / DHAN_CLIENT_ID")

    # -------- CORE OBJECTS --------
    master = InstrumentMaster(CSV_FILE)
    ltp_rest = DhanLtpRestEngine(token, client_id, debug=True)

    tick_filter = TickFilter()
    quant = QuantProcessor()
    micro = MicrostructureState()
    signal_engine = SignalEngine()
    feature_builder = DepthMicroFeatureBuilder()

    # -------- CSV LOGGER --------
    Path("logs").mkdir(exist_ok=True)
    log_file = f"logs/market_ticks_{datetime.now().strftime('%Y-%m-%d')}.csv"
    csv_fp = open(log_file, "a", newline="")
    csv_writer = csv.writer(csv_fp)

    if csv_fp.tell() == 0:
        csv_writer.writerow([
            "time", "tag", "ltp", "flow",
            "volume", "vol_delta",
            "oi", "oi_delta",
            "bid_qty", "ask_qty",
            "absorption_flag", "absorption_strength",
            "vacuum_flag",
            "delta", "theta",
            "signal"
        ])

    print("\n🚀 LIVE MODE (ONE-SHOT REST FUT + WS OPTIONS)")
    print("1) FUT LTP  -> REST (ONLY UNTIL FIRST VALID LTP)")
    print("2) OPTIONS -> ONE 20Depth WS (main focus)\n")

    # -------- FUT IDS --------
    fut_secids = {}
    for idx in INDEXES:
        fut = master.get_nearest_future(idx)
        fut_secids[idx] = int(fut["security_id"])

    fut_ltp = {k: 0.0 for k in INDEXES}
    last_raw_print = {}

    # -------- OPTION DEPTH CALLBACK --------
    def on_opt_depth(secid: int, tag: str, bid, ask):
        try:
            raw = feature_builder.build(secid, bid, ask)
            if not raw:
                return

            raw["tag"] = tag
            raw["secid"] = secid
            raw["ts"] = time.time()

            # 🔥 RAW OPTION TICK PRINT (THIS WAS MISSING EARLIER)
            if PRINT_RAW_JSON:
                now = time.time()
                lp = last_raw_print.get(tag, 0.0)
                if (now - lp) >= RAW_JSON_THROTTLE_SEC:
                    last_raw_print[tag] = now

                    print(
                        f"📌 OPT_TICK_JSON | {tag} | "
                        f"LTP:{raw.get('ltp', 0):.2f} | "
                        f"BID_Q:{raw.get('bid_qty', 0)} | "
                        f"ASK_Q:{raw.get('ask_qty', 0)} | "
                        f"FLOW:{raw.get('flow', 0):.0f} | "
                        f"IMB:{raw.get('imbalance_5', 0):+.2f} | "
                        f"VAC:{raw.get('vacuum_flag')} | "
                        f"ABS:{raw.get('absorption_flag')}"
                    )

            q = quant.compute(raw)
            if not q:
                return

            q = micro.update(q)
            if not q:
                return

            sig = signal_engine.generate(q) or "HOLD"

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

            if sig != "HOLD":
                print(
                    f"🚨 SIGNAL | {tag} | "
                    f"LTP:{q.get('ltp', 0):.2f} | "
                    f"FLOW:{q.get('flow', 0):.0f} → {sig}"
                )

        except Exception as e:
            print("❌ on_opt_depth error:", e)

    # -------- OPTIONS WS --------
    depth20 = DhanTwentyDepthWS(
        token=token,
        client_id=client_id,
        auth_type=2,
        exchange_segment=OPT_EXCHANGE_SEGMENT_20D,
        on_depth=on_opt_depth,
        debug=False
    )
    depth20.connect()
    print("✅ 20Depth WS started")

    # -------- ONE-SHOT FUT LTP --------
    print("\n⏳ Fetching initial FUT LTP (REST one-shot mode)...")
    t0 = time.time()
    last_hb = 0.0

    while True:
        now = time.time()

        if now - last_hb >= HEARTBEAT_SEC:
            last_hb = now
            print(
                f"🫀 {datetime.now().strftime('%H:%M:%S')} HEARTBEAT | "
                + " | ".join([f"{k}:{fut_ltp[k]:.2f}" for k in INDEXES])
            )

        if (now - t0) > REST_MAX_WAIT_SEC:
            print("⚠️ REST timeout, proceeding.")
            break

        ltp_map = ltp_rest.fetch_ltp_map({"NSE_FNO": list(fut_secids.values())})
        if not ltp_map:
            time.sleep(REST_POLL_INTERVAL_SEC)
            continue

        for idx, secid in fut_secids.items():
            if secid in ltp_map:
                fut_ltp[idx] = float(ltp_map[secid] or 0)

        if all(fut_ltp[i] > 0 for i in INDEXES):
            print("✅ Initial FUT LTP captured (one-shot).")
            break

        time.sleep(REST_POLL_INTERVAL_SEC)

    # -------- SUBSCRIBE CE / PE --------
    print("\n🎯 Selecting CE/PE once and subscribing to 20Depth...")
    for idx in INDEXES:
        ltp = fut_ltp[idx]
        if ltp <= 0:
            continue

        atm, ce_strike, pe_strike = compute_itm_strikes(ltp, STRIKE_STEP[idx])
        expiry = master.get_nearest_option_expiry(idx)

        ce = master.find_option_security_id(idx, expiry, ce_strike, "CE")
        pe = master.find_option_security_id(idx, expiry, pe_strike, "PE")

        depth20.subscribe([
            {"SecurityId": str(ce), "tag": f"{idx}_CE"},
            {"SecurityId": str(pe), "tag": f"{idx}_PE"},
        ])

        print(
            f"✅ [{idx}] FUT:{ltp:.2f} ATM:{atm} "
            f"CE:{ce_strike}(id:{ce}) PE:{pe_strike}(id:{pe})"
        )

    print("\n🔥 LIVE: Now focusing ONLY on CE/PE depth ticks.\n")

    # -------- RUN FOREVER --------
    last_hb2 = 0.0
    while True:
        if time.time() - last_hb2 >= HEARTBEAT_SEC:
            last_hb2 = time.time()
            print(f"🫀 {datetime.now().strftime('%H:%M:%S')} HEARTBEAT | OPTIONS_STREAM_RUNNING")
        time.sleep(0.2)


if __name__ == "__main__":
    main()