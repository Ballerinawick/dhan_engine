import os
import time
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

from instrument_master import InstrumentMaster
from dhan_depth20_ws import DhanTwentyDepthWS
from ltp_rest_engine import DhanLtpRestEngine

from depth_micro_features import DepthMicroFeatureBuilder
from options_momentum_engine import OptionsMomentumEngine


# ---------------- CONFIG ----------------
CSV_FILE = os.getenv("CSV_FILE", "api-scrip-master.csv")

INDEXES = ["NIFTY", "BANKNIFTY", "FINNIFTY"]
STRIKE_STEP = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50}

OPT_EXCHANGE_SEGMENT_20D = "NSE_FNO"

REST_POLL_INTERVAL_SEC = 1.1
REST_MAX_WAIT_SEC = 45
HEARTBEAT_SEC = 30.0


def compute_itm_strikes(ltp: float, step: int):
    atm = int(round(ltp / step) * step)
    return atm, atm - step, atm + step


def main():
    token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
    client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
    if not token or not client_id:
        raise RuntimeError("Missing DHAN_ACCESS_TOKEN / DHAN_CLIENT_ID")

    # ---------------- CORE OBJECTS ----------------
    master = InstrumentMaster(CSV_FILE)

    ltp_rest = DhanLtpRestEngine(
        access_token=token,
        client_id=client_id,
        debug=True
    )

    feature_builder = DepthMicroFeatureBuilder()
    momentum_engine = OptionsMomentumEngine()

    # ---------------- FUT IDS ----------------
    fut_secids = {}
    for idx in INDEXES:
        fut = master.get_nearest_future(idx)
        fut_secids[idx] = int(fut["security_id"])

    fut_ltp = {k: 0.0 for k in INDEXES}

    # ---------------- OPTION DEPTH CALLBACK ----------------
    def on_opt_depth(secid: int, tag: str, bid, ask):
        try:
            raw = feature_builder.build(secid, bid, ask)
            if not raw:
                return

            raw["secid"] = secid
            raw["tag"] = tag

            action = momentum_engine.on_tick(secid, raw)

            if action != "NO_TRADE":
                pnl = momentum_engine.get_trade_pnl(secid)  # ✅ FIXED
                print(
                    f"🚦 {datetime.now().strftime('%H:%M:%S')} | "
                    f"{tag} | {action} | "
                    f"LTP:{raw['ltp']:.2f} | TradePnL:{pnl:.2f}"
                )

        except Exception as e:
            print("❌ on_opt_depth error:", e)

    # ---------------- WS INIT ----------------
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

    # --------------------------------------------------
    # STEP 1: ONE-SHOT FUT LTP (REST)
    # --------------------------------------------------
    print("\n⏳ Fetching initial FUT LTP (one-shot)...")
    t0 = time.time()
    last_heartbeat = 0.0

    while True:
        now = time.time()

        if now - last_heartbeat >= HEARTBEAT_SEC:
            last_heartbeat = now
            print(
                f"🫀 {datetime.now().strftime('%H:%M:%S')} HEARTBEAT | "
                + " | ".join([f"{k}:{fut_ltp[k]:.2f}" for k in INDEXES])
            )

        if (now - t0) > REST_MAX_WAIT_SEC:
            print("⚠️ FUT LTP timeout, proceeding.")
            break

        ltp_map = ltp_rest.fetch_ltp_map({
            "NSE_FNO": list(fut_secids.values())
        })

        if not ltp_map:
            time.sleep(REST_POLL_INTERVAL_SEC)
            continue

        for idx, secid in fut_secids.items():
            if secid in ltp_map:
                fut_ltp[idx] = float(ltp_map[secid] or 0.0)

        if all(fut_ltp[i] > 0 for i in INDEXES):
            print("✅ Initial FUT LTP captured. REST stopped.")
            break

        time.sleep(REST_POLL_INTERVAL_SEC)

    # --------------------------------------------------
    # STEP 2: SELECT CE / PE ONCE
    # --------------------------------------------------
    print("\n🎯 Subscribing CE / PE options...")
    for idx in INDEXES:
        ltp = fut_ltp[idx]
        if ltp <= 0:
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

        print(
            f"✅ [{idx}] FUT:{ltp:.2f} "
            f"CE:{ce_strike}(id:{ce}) "
            f"PE:{pe_strike}(id:{pe})"
        )

    print("\n🔥 LIVE: Options Momentum Engine ACTIVE\n")

    # --------------------------------------------------
    # RUN FOREVER (ENGINE CONTROLS MARKET HOURS)
    # --------------------------------------------------
    last_hb = 0.0
    while True:
        now = time.time()
        if now - last_hb >= HEARTBEAT_SEC:
            last_hb = now
            print(f"🫀 {datetime.now().strftime('%H:%M:%S')} ENGINE_RUNNING")
        time.sleep(0.2)


if __name__ == "__main__":
    main()