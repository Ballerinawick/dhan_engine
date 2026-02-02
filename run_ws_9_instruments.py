import os
import time
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
load_dotenv()

from instrument_master import InstrumentMaster
from dhan_depth20_ws import DhanTwentyDepthWS
from ltp_rest_engine import DhanLtpRestEngine

from depth_micro_features import DepthMicroFeatureBuilder
from options_momentum_engine import OptionsMomentumEngine
from paper_trade_manager import PaperTradeManager
from option_chain_selector import OptionChainSelector
from institutional_decision_engine import InstitutionalDecisionEngine

# ---------------- CONFIG ----------------
CSV_FILE = os.getenv("CSV_FILE", "api-scrip-master.csv")

INDEXES = ["NIFTY", "BANKNIFTY", "FINNIFTY"]
STRIKE_STEP = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50}

OPT_EXCHANGE_SEGMENT_20D = "NSE_FNO"

REST_POLL_INTERVAL_SEC = 1.1
REST_MAX_WAIT_SEC = 45
HEARTBEAT_SEC = 30.0

IST = ZoneInfo("Asia/Kolkata")
MARKET_START = dtime(9, 10)
MARKET_END = dtime(15, 35)


def market_open():
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    return MARKET_START <= now.time() <= MARKET_END


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
    paper_trader = PaperTradeManager(capital=100000)

    # ✅ SINGLE AUTHORITY FOR ENTRY
    decision_engine = InstitutionalDecisionEngine(debug=True)

    selector = OptionChainSelector(
        access_token=token,
        client_id=client_id,
        instrument_master=master,
        strike_step_map=STRIKE_STEP,
        mode=2,
        max_steps_each_side=10,
        debug=True
    )

    # ---------------- FUT IDS ----------------
    fut_secids = {}
    for idx in INDEXES:
        fut = master.get_nearest_future(idx)
        fut_secids[idx] = int(fut["security_id"])

    fut_ltp = {k: 0.0 for k in INDEXES}

    # ---------------- OPTION DEPTH CALLBACK ----------------
    def on_opt_depth(secid: int, tag: str, bid, ask):
        try:
            if not market_open():
                return

            raw = feature_builder.build(secid, bid, ask)
            if not raw:
                return

            raw["secid"] = secid
            raw["tag"] = tag

            # MTM update
            paper_trader.on_tick(secid, raw["ltp"])

            # 1️⃣ Momentum signal
            action = momentum_engine.on_tick(secid, raw)

            # 2️⃣ Institutional governance (ENTRY + EXIT DECISION)
            decision = decision_engine.on_signal(
                secid=secid,
                tag=tag,
                ltp=raw["ltp"],
                signal=action,
                momentum_engine=momentum_engine,
                paper_trader=paper_trader
            )

            # 3️⃣ EXIT EXECUTION ONLY
            if action == "EXIT":
                if decision and decision.get("exit_allowed") is False:
                    return

                reason = momentum_engine.last_exit_reason.get(secid, action)
                if decision and decision.get("exit_reason"):
                    reason = decision["exit_reason"]

                paper_trader.on_exit(secid, raw["ltp"], reason=reason)

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
                f"🫀 {datetime.now(IST).strftime('%H:%M:%S')} HEARTBEAT | "
                + " | ".join([f"{k}:{fut_ltp[k]:.2f}" for k in INDEXES])
            )

        if (now - t0) > REST_MAX_WAIT_SEC:
            print("⚠️ FUT LTP timeout, proceeding.")
            break

        ltp_map = ltp_rest.fetch_ltp_map({"NSE_FNO": list(fut_secids.values())})
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
    # STEP 2: SELECT OPTIONS + SUBSCRIBE
    # --------------------------------------------------
    print("\n🎯 Subscribing OPTIONS (dynamic via OptionChain)...")

    for idx in INDEXES:
        selection = selector.select_best(idx)
        if not selection:
            continue

        subs = []
        if "CE" in selection:
            subs.append({"SecurityId": str(selection["CE"]["security_id"]), "tag": f"{idx}_CE"})
        if "PE" in selection:
            subs.append({"SecurityId": str(selection["PE"]["security_id"]), "tag": f"{idx}_PE"})

        if subs:
            depth20.subscribe(subs)

    print("\n🔥 LIVE: INSTITUTIONAL OPTIONS ENGINE ACTIVE\n")

    # --------------------------------------------------
    # RUN LOOP
    # --------------------------------------------------
    last_hb = 0.0
    while True:
        if not market_open():
            print("🛑 Market closed. Sleeping.")
            time.sleep(60)
            continue

        now = time.time()
        if now - last_hb >= HEARTBEAT_SEC:
            last_hb = now
            print(f"🫀 {datetime.now(IST).strftime('%H:%M:%S')} ENGINE_RUNNING")

        time.sleep(0.2)


if __name__ == "__main__":
    main()