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

# ✅ NEW IMPORT
from option_chain_selector import OptionChainSelector


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
    paper_trader = PaperTradeManager(capital=100000)

    # ✅ NEW: OptionChain selector (mode can be 1 or 2)
    # mode=1 => ONE best option (either CE or PE)
    # mode=2 => best CE and best PE
    selector = OptionChainSelector(
        access_token=token,
        client_id=client_id,
        instrument_master=master,
        strike_step_map=STRIKE_STEP,
        mode=2,                    # change 1/2 as you want
        max_steps_each_side=10,     # 10 steps each side near ATM
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

            # 🔁 LIVE MTM
            paper_trader.on_tick(secid, raw["ltp"])

            action = momentum_engine.on_tick(secid, raw)

            # ---------------- ENTRY ----------------
            if action in ("A_ENTRY", "B_ENTRY"):
                trade = momentum_engine.active_trade.get(secid)
                if trade:
                    # keep your flow (if your PaperTradeManager accepts reason, fine)
                    try:
                        paper_trader.on_entry(
                            secid=secid,
                            tag=tag,
                            side=trade["side"],
                            ltp=raw["ltp"],
                            reason=action
                        )
                    except TypeError:
                        paper_trader.on_entry(
                            secid=secid,
                            tag=tag,
                            side=trade["side"],
                            ltp=raw["ltp"]
                        )

            # ---------------- EXIT ----------------
            elif action == "EXIT":
                try:
                    paper_trader.on_exit(secid, raw["ltp"], reason=action)
                except TypeError:
                    paper_trader.on_exit(secid, raw["ltp"])

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
    # STEP 2: SELECT OPTIONS (DYNAMIC) + SUBSCRIBE
    # --------------------------------------------------
    print("\n🎯 Subscribing OPTIONS (dynamic via OptionChain)...")

    for idx in INDEXES:
        try:
            selection = selector.select_best(idx)
            if not selection:
                print(f"⚠️ [{idx}] No selection. Skipping subscribe.")
                continue

            subs = []
            if "BEST" in selection:
                info = selection["BEST"]
                subs.append({"SecurityId": str(info["security_id"]), "tag": f"{idx}_{info['side']}"})
                print(f"✅ [{idx}] BEST {info['side']} | strike:{info['strike']} | id:{info['security_id']} | score:{info['score']:.3f}")

            else:
                # mode=2
                if "CE" in selection:
                    ce = selection["CE"]
                    subs.append({"SecurityId": str(ce["security_id"]), "tag": f"{idx}_CE"})
                    print(f"✅ [{idx}] CE | strike:{ce['strike']} | id:{ce['security_id']} | score:{ce['score']:.3f}")

                if "PE" in selection:
                    pe = selection["PE"]
                    subs.append({"SecurityId": str(pe["security_id"]), "tag": f"{idx}_PE"})
                    print(f"✅ [{idx}] PE | strike:{pe['strike']} | id:{pe['security_id']} | score:{pe['score']:.3f}")

            if subs:
                depth20.subscribe(subs)

        except Exception as e:
            print(f"❌ [{idx}] OptionChain select/subscribe error:", e)

    print("\n🔥 LIVE: Options Momentum Engine ACTIVE\n")

    # --------------------------------------------------
    # RUN LOOP
    # --------------------------------------------------
    last_hb = 0.0
    while True:
        if not market_open():
            print("🛑 Market closed. Sleeping to save cloud cost.")
            time.sleep(60)
            continue

        now = time.time()
        if now - last_hb >= HEARTBEAT_SEC:
            last_hb = now
            print(f"🫀 {datetime.now(IST).strftime('%H:%M:%S')} ENGINE_RUNNING")

        time.sleep(0.2)


if __name__ == "__main__":
    main()