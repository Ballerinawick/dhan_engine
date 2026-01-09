import os
import time
import csv
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
SWITCH_COOLDOWN_SEC = 8.0
HEARTBEAT_SEC = 30.0


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

    print("\n🚀 LIVE MODE (REST FUT + WS OPTIONS)")
    print("1) FUT LTP  -> REST (official)")
    print("2) OPTIONS -> ONE 20Depth WS\n")

    fut_ltp = {k: 0.0 for k in INDEXES}
    current = {k: {"atm": None, "last_switch": 0.0} for k in INDEXES}
    last_heartbeat = 0.0

    # -------- OPTION DEPTH CALLBACK --------
    def on_opt_depth(secid: int, tag: str, bid, ask):
        try:
            raw = feature_builder.build(secid, bid, ask)
            tick = tick_filter.extract({**raw, "tag": tag})
            q = quant.compute(tick)
            q = micro.update(q)
            sig = signal_engine.generate(q)

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

    # -------- PREPARE FUT SECURITY IDS (FROM CSV) --------
    fut_secids = {}
    for idx in INDEXES:
        fut = master.get_nearest_future(idx)
        fut_secids[idx] = int(fut["security_id"])

    # -------- MAIN LOOP --------
    while True:
        now = time.time()

        # ---- REST LTP (1 req/sec) ----
        ltp_map = ltp_rest.safe_poll_every_1s({
            "NSE_FNO": list(fut_secids.values())
        })

        for idx, secid in fut_secids.items():
            if secid in ltp_map:
                fut_ltp[idx] = ltp_map[secid]

        # ---- HEARTBEAT ----
        if now - last_heartbeat >= HEARTBEAT_SEC:
            last_heartbeat = now
            print(
                f"🫀 {datetime.now().strftime('%H:%M:%S')} HEARTBEAT | "
                + " | ".join([f"{k}:{fut_ltp[k]:.2f}" for k in INDEXES])
            )

        # ---- OPTION SWITCH LOGIC ----
        for idx in INDEXES:
            ltp = fut_ltp[idx]
            if ltp <= 0:
                continue

            step = STRIKE_STEP[idx]
            atm, ce_strike, pe_strike = compute_itm_strikes(ltp, step)

            if (
                current[idx]["atm"] != atm
                and now - current[idx]["last_switch"] >= SWITCH_COOLDOWN_SEC
            ):
                current[idx]["atm"] = atm
                current[idx]["last_switch"] = now

                expiry = master.get_nearest_option_expiry(idx)
                ce = master.find_option_security_id(idx, expiry, ce_strike, "CE")
                pe = master.find_option_security_id(idx, expiry, pe_strike, "PE")

                depth20.subscribe([
                    {"SecurityId": str(ce), "tag": f"{idx}_CE"},
                    {"SecurityId": str(pe), "tag": f"{idx}_PE"},
                ])

                print(
                    f"🔁 [{idx}] FUT:{ltp:.2f} ATM:{atm} "
                    f"CE:{ce_strike}(id:{ce}) PE:{pe_strike}(id:{pe})"
                )

        time.sleep(0.05)


if __name__ == "__main__":
    main()