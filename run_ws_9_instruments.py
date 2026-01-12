# run_ws_9_instruments.py
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

# ✅ One-shot FUT LTP (REST) – we will STOP after we get first valid LTPs
REST_POLL_INTERVAL_SEC = 1.05  # keep > 1s to avoid 429
REST_MAX_WAIT_SEC = 45         # try up to 45 sec to get initial LTP (adjust if needed)

# ✅ After we subscribe CE/PE once, we won’t keep switching by ATM
SUBSCRIBE_ONCE_ONLY = True

HEARTBEAT_SEC = 30.0

# ✅ Raw CE/PE "tick JSON" print (throttled per tag)
PRINT_RAW_JSON = True
RAW_JSON_THROTTLE_SEC = 1.0  # print at most 1 per sec per tag


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

    ltp_rest = DhanLtpRestEngine(
        access_token=token,
        client_id=client_id,
        debug=True
    )

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

    # -------- PREPARE FUT SECURITY IDS (FROM CSV) --------
    fut_secids = {}
    for idx in INDEXES:
        fut = master.get_nearest_future(idx)
        fut_secids[idx] = int(fut["security_id"])

    fut_ltp = {k: 0.0 for k in INDEXES}
    subscribed_once = {k: False for k in INDEXES}

    # For throttled raw JSON printing
    last_raw_print = {}  # tag -> epoch

    # -------- OPTION DEPTH CALLBACK --------
    def on_opt_depth(secid: int, tag: str, bid, ask):
        try:
            # ✅ Build micro raw from depth20
            raw = feature_builder.build(secid, bid, ask)

            # ✅ Your TickFilter expects keys like last_price/depth/etc
            # feature_builder should already provide compatible keys.
            tick = tick_filter.extract({**(raw or {}), "tag": tag})

            # ✅ IMPORTANT GUARDS (fixes NoneType crash)
            if tick is None:
                return

            q = quant.compute(tick)
            if q is None:
                return

            q = micro.update(q)
            if q is None:
                return

            sig = signal_engine.generate(q) or "HOLD"

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

            # -------- RAW "TICK JSON" PRINT (THROTTLED) --------
            if PRINT_RAW_JSON:
                now = time.time()
                lp = last_raw_print.get(tag, 0.0)
                if (now - lp) >= RAW_JSON_THROTTLE_SEC:
                    last_raw_print[tag] = now

                    # Build a clean JSON snapshot (safe, no heavy objects)
                    snap = {
                        "ts": datetime.utcnow().isoformat() + "Z",
                        "tag": tag,
                        "security_id": int(secid),
                        "ltp": q.get("ltp", 0),
                        "bid_price": q.get("bid_price", 0),
                        "bid_qty": q.get("bid_qty", 0),
                        "ask_price": q.get("ask_price", 0),
                        "ask_qty": q.get("ask_qty", 0),
                        "spread": q.get("spread", 0),
                        "signal": sig,
                    }
                    print("📌 OPT_TICK_JSON:", json.dumps(snap, separators=(",", ":")))

            # -------- SIGNAL PRINT --------
            if sig != "HOLD":
                print(f"{datetime.now().strftime('%H:%M:%S')} | {tag} → {sig}")

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

    # ------------------------------------------------------------------
    # ✅ STEP 1: ONE-SHOT FUT LTP via REST (stop after first valid LTPs)
    # ------------------------------------------------------------------
    print("\n⏳ Fetching initial FUT LTP (REST one-shot mode)...")
    t0 = time.time()
    last_heartbeat = 0.0

    while True:
        now = time.time()

        # Heartbeat (so Railway logs show life)
        if now - last_heartbeat >= HEARTBEAT_SEC:
            last_heartbeat = now
            print(
                f"🫀 {datetime.now().strftime('%H:%M:%S')} HEARTBEAT | "
                + " | ".join([f"{k}:{fut_ltp[k]:.2f}" for k in INDEXES])
            )

        # Stop waiting if timed out
        if (now - t0) > REST_MAX_WAIT_SEC:
            print("⚠️ REST initial LTP wait timeout. Proceeding with whatever we got.")
            break

        # Call REST (max once per second)
        ltp_map = ltp_rest.fetch_ltp_map({
            "NSE_FNO": list(fut_secids.values())
        })

        # If failed, sleep and retry
        if not ltp_map:
            time.sleep(REST_POLL_INTERVAL_SEC)
            continue

        # Update fut_ltp
        for idx, secid in fut_secids.items():
            if secid in ltp_map:
                fut_ltp[idx] = float(ltp_map[secid] or 0.0)

        # Check if we got at least one valid LTP per index
        got_all = all(fut_ltp[i] > 0 for i in INDEXES)
        if got_all:
            print("✅ Initial FUT LTP captured (one-shot). Stopping REST calls now.")
            break

        # Sleep to respect rate limit
        time.sleep(REST_POLL_INTERVAL_SEC)

    # ------------------------------------------------------------------
    # ✅ STEP 2: Use that ONE-SHOT FUT LTP to select ITM CE/PE and subscribe
    # ------------------------------------------------------------------
    print("\n🎯 Selecting CE/PE once and subscribing to 20Depth...")
    for idx in INDEXES:
        ltp = fut_ltp[idx]
        if ltp <= 0:
            print(f"⚠️ {idx}: FUT LTP not available, skipping CE/PE subscribe.")
            continue

        step = STRIKE_STEP[idx]
        atm, ce_strike, pe_strike = compute_itm_strikes(ltp, step)

        expiry = master.get_nearest_option_expiry(idx)
        ce = master.find_option_security_id(idx, expiry, ce_strike, "CE")
        pe = master.find_option_security_id(idx, expiry, pe_strike, "PE")

        depth20.subscribe([
            {"SecurityId": str(ce), "tag": f"{idx}_CE"},
            {"SecurityId": str(pe), "tag": f"{idx}_PE"},
        ])

        subscribed_once[idx] = True

        print(
            f"✅ [{idx}] FUT:{ltp:.2f} ATM:{atm} "
            f"CE:{ce_strike}(id:{ce}) PE:{pe_strike}(id:{pe})"
        )

    print("\n🔥 LIVE: Now focusing ONLY on CE/PE depth ticks (REST stopped).")
    print("   You should start seeing 📌 OPT_TICK_JSON logs.\n")

    # ------------------------------------------------------------------
    # ✅ STEP 3: Run forever just for CE/PE ticks (no REST, no switching)
    # ------------------------------------------------------------------
    last_heartbeat2 = 0.0
    while True:
        now = time.time()
        if now - last_heartbeat2 >= HEARTBEAT_SEC:
            last_heartbeat2 = now
            print(f"🫀 {datetime.now().strftime('%H:%M:%S')} HEARTBEAT | OPTIONS_STREAM_RUNNING")

        # If you want ATM switching later, set SUBSCRIBE_ONCE_ONLY=False and implement here.
        time.sleep(0.2)


if __name__ == "__main__":
    main()