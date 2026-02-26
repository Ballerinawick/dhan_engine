import os
import time
import requests
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
from institutional_trailing_exit_engine import InstitutionalTrailingExitEngine
from structure_exit_engine import StructureExitEngine


# ================= CONFIG =================
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

# ================= CSV AUTO DOWNLOAD =================
MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

def download_master_csv(csv_url: str, save_path: str):
    try:
        print("📥 Downloading latest api-scrip-master.csv...")

        response = requests.get(csv_url, timeout=30)
        response.raise_for_status()

        with open(save_path, "wb") as f:
            f.write(response.content)

        abs_path = os.path.abspath(save_path)

        print("✅ Master CSV downloaded successfully.")
        print(f"📂 CSV saved at: {abs_path}")

        if os.path.exists(save_path):
            size_kb = round(os.path.getsize(save_path) / 1024, 2)
            print(f"📊 CSV size: {size_kb} KB")

    except Exception as e:
        print("❌ CSV download failed:", e)
        raise

def market_open():
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    return MARKET_START <= now.time() <= MARKET_END


# ================= MAIN =================
def main():
    token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
    client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
    if not token or not client_id:
        raise RuntimeError("Missing DHAN_ACCESS_TOKEN / DHAN_CLIENT_ID")

    try:
        download_master_csv(MASTER_URL, CSV_FILE)
    except Exception:
        print("⚠️ CSV download failed. Continuing with existing file.")

    # -------- Core objects --------
    master = InstrumentMaster(CSV_FILE)

    ltp_rest = DhanLtpRestEngine(
        access_token=token,
        client_id=client_id,
        debug=True
    )

    feature_builder = DepthMicroFeatureBuilder()
    momentum_engine = OptionsMomentumEngine()
    paper_trader = PaperTradeManager(capital=100000)

    # 🔑 Institutional engines
    decision_engine = InstitutionalDecisionEngine(debug=True)
    trailing_exit_engine = InstitutionalTrailingExitEngine(debug=True)
    structure_exit_engine = StructureExitEngine(debug=True)

    selector = OptionChainSelector(
        access_token=token,
        client_id=client_id,
        instrument_master=master,
        strike_step_map=STRIKE_STEP,
        mode=2,
        max_steps_each_side=10,
        debug=True
    )

    # -------- FUT security IDs --------
    fut_secids = {}
    for idx in INDEXES:
        fut = master.get_nearest_future(idx)
        fut_secids[idx] = int(fut["security_id"])

    fut_ltp = {k: 0.0 for k in INDEXES}

    # ==================================================
    # OPTION DEPTH CALLBACK (INSTITUTIONAL FLOW)
    # ==================================================
    def on_opt_depth(secid: int, tag: str, bid, ask):
        try:
            if not market_open():
                return

            bid_levels = len(bid.prices) if hasattr(bid, "prices") else len(bid)
            ask_levels = len(ask.prices) if hasattr(ask, "prices") else len(ask)
            print(f"🧩 DEPTH_CALLBACK | secid={secid} | bid_levels={bid_levels} | ask_levels={ask_levels}")

            raw = feature_builder.build(secid, bid, ask)
            if not raw:
                return

            raw["secid"] = secid
            raw["tag"] = tag

            # 0️⃣ MTM update
            paper_trader.on_tick(secid, raw["ltp"])

            # -------------- unified exit (CRITICAL FIX) --------------
            def force_exit(reason: str):
                # 1) decision engine must see EXIT to release locks/ctx safely
                try:
                    decision = decision_engine.on_signal(
                        secid=secid,
                        tag=tag,
                        ltp=raw["ltp"],
                        signal="EXIT",
                        momentum_engine=momentum_engine,
                        paper_trader=paper_trader
                    )
                    if decision and decision.get("exit_allowed") is False:
                        return False
                except Exception as e:
                    print("❌ force_exit: decision_engine EXIT error:", e)

                # 2) close paper position
                paper_trader.on_exit(secid, raw["ltp"], reason=reason)

                # 3) CRITICAL: clear momentum trade state so entries can happen again
                try:
                    momentum_engine.last_exit_reason[secid] = reason
                except Exception:
                    pass
                try:
                    momentum_engine.active_trade.pop(secid, None)
                except Exception:
                    pass

                return True
            # ---------------------------------------------------------

            # 1️⃣ Momentum signal
            action = momentum_engine.on_tick(secid, raw)

            # 2️⃣ Institutional decision layer (ENTRY gating etc)
            decision = decision_engine.on_signal(
                secid=secid,
                tag=tag,
                ltp=raw["ltp"],
                signal=action,
                momentum_engine=momentum_engine,
                paper_trader=paper_trader
            )

            # 3️⃣ Structure Exit (SCALP vs TREND)
            struct = structure_exit_engine.on_tick(
                secid=secid,
                tag=tag,
                ltp=raw["ltp"],
                paper_trader=paper_trader,
                decision_engine=decision_engine
            )
            if struct and struct.get("exit"):
                print(f"📉 EXIT_OVERRIDE | {tag} | reason={struct['reason']}")
                force_exit(struct["reason"])
                return

            # 4️⃣ Trailing exit (PROFIT PROTECT)
            trail = trailing_exit_engine.on_tick(
                secid=secid,
                tag=tag,
                ltp=raw["ltp"],
                paper_trader=paper_trader,
                momentum_engine=momentum_engine
            )
            if trail and trail.get("exit"):
                print(f"📉 EXIT_OVERRIDE | {tag} | reason={trail['reason']}")
                force_exit(trail["reason"])
                return

            # 5️⃣ Momentum / Decision exit
            if action == "EXIT":
                # if decision vetoes, do nothing
                if decision and decision.get("exit_allowed") is False:
                    return

                reason = decision.get(
                    "exit_reason",
                    momentum_engine.last_exit_reason.get(secid, action)
                )
                force_exit(reason)
                return

        except Exception as e:
            print("❌ on_opt_depth error:", e)

    # ================= WS INIT =================
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

    # ================= FUT LTP =================
    print("\n⏳ Fetching initial FUT LTP...")
    t0 = time.time()
    last_hb = 0.0

    while True:
        now = time.time()

        if now - last_hb >= HEARTBEAT_SEC:
            last_hb = now
            print(
                f"🫀 {datetime.now(IST).strftime('%H:%M:%S')} HEARTBEAT | "
                + " | ".join([f"{k}:{fut_ltp[k]:.2f}" for k in INDEXES])
            )

        if now - t0 > REST_MAX_WAIT_SEC:
            print("⚠️ FUT LTP timeout")
            break

        ltp_map = ltp_rest.fetch_ltp_map({
            "NSE_FNO": list(fut_secids.values())
        })

        if ltp_map:
            for idx, secid in fut_secids.items():
                if secid in ltp_map:
                    fut_ltp[idx] = float(ltp_map[secid] or 0.0)

            if all(fut_ltp[i] > 0 for i in INDEXES):
                print("✅ Initial FUT LTP captured.")
                break

        time.sleep(REST_POLL_INTERVAL_SEC)

    # ================= OPTION SUBSCRIPTION =================
    print("\n🎯 Subscribing OPTIONS...")

    for idx in INDEXES:
        try:
            selection = selector.select_best(idx)
            if not selection:
                continue

            subs = []
            if "CE" in selection:
                subs.append({
                    "SecurityId": str(selection["CE"]["security_id"]),
                    "tag": f"{idx}_CE"
                })
            if "PE" in selection:
                subs.append({
                    "SecurityId": str(selection["PE"]["security_id"]),
                    "tag": f"{idx}_PE"
                })

            if subs:
                depth20.subscribe(subs)

        except Exception as e:
            print(f"❌ [{idx}] OptionChain error:", e)

    print("\n🔥 LIVE: INSTITUTIONAL OPTIONS ENGINE ACTIVE\n")

    # ================= RUN LOOP =================
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
