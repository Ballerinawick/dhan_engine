# run_itm_auto_live.py
import os
import time
from datetime import datetime

from instrument_master import InstrumentMaster
from dhan_optionchain_feed import DhanOptionChainFeed

from tick_filter import TickFilter
from quant_processor import QuantProcessor
from microstructure_state import MicrostructureState
from signal_engine import SignalEngine

from dhan_depth20_ws import DhanTwentyDepthWS
from depth_micro_features import DepthMicroFeatureBuilder


CSV_FILE = "api-scrip-master.csv"
INDEXES = ["NIFTY", "BANKNIFTY", "FINNIFTY"]

STRIKE_STEP = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50}
UNDERLYING_SEG = {"NIFTY": "IDX_I", "BANKNIFTY": "IDX_I", "FINNIFTY": "IDX_I"}
UNDERLYING_SCRIP = {"NIFTY": 13, "BANKNIFTY": 25, "FINNIFTY": 27}

SWITCH_COOLDOWN_SEC = 8.0


def pick_itm_from_oc(oc_data: dict, fut_ltp: float, step: int):
    atm = int(round(fut_ltp / step) * step)
    ce_strike = atm - step
    pe_strike = atm + step
    ce_key = f"{float(ce_strike):.6f}"
    pe_key = f"{float(pe_strike):.6f}"
    ce = (oc_data.get(ce_key) or {}).get("ce")
    pe = (oc_data.get(pe_key) or {}).get("pe")
    return atm, ce_strike, pe_strike, ce, pe


def main():
    master = InstrumentMaster(CSV_FILE)
    oc_feed = DhanOptionChainFeed()

    tick_filter = TickFilter()
    quant = QuantProcessor()
    micro = MicrostructureState()
    signal_engine = SignalEngine()

    feature_builder = DepthMicroFeatureBuilder()

    DHAN_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
    DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "").strip()

    if not DHAN_TOKEN or not DHAN_CLIENT_ID:
        raise RuntimeError("Missing DHAN_ACCESS_TOKEN / DHAN_CLIENT_ID in .env")

    print("\n🚀 LIVE MODE: OptionChain (SLOW) + 20Depth WS (FAST)\n")

    latest_q = {}

    def on_depth(secid: int, tag: str, bid, ask):
        micro_raw = feature_builder.build(secid, bid, ask)
        tick = tick_filter.extract({**micro_raw, "tag": tag})
        q = quant.compute(tick)
        q = micro.update(q)
        sig = signal_engine.generate(q)
        latest_q[tag] = (q, sig)

    depth_ws = DhanTwentyDepthWS(
        token=DHAN_TOKEN,
        client_id=DHAN_CLIENT_ID,
        auth_type=2,
        exchange_segment="NSE_FNO",
        on_depth=on_depth,
        debug=True
    )
    depth_ws.connect()

    current = {idx: {"atm": None, "last_switch": 0.0} for idx in INDEXES}
    expiry_cache = {}

    i = 0
    while True:
        idx = INDEXES[i % len(INDEXES)]
        i += 1

        uscrip = UNDERLYING_SCRIP[idx]
        useg = UNDERLYING_SEG[idx]

        # ---------------------------
        # EXPIRY FETCH (ONCE ONLY)
        # ---------------------------
        if idx not in expiry_cache:
            expiries = oc_feed.expiry_list(uscrip, useg)
            if not expiries:
                print(f"{datetime.now().strftime('%H:%M:%S')} | {idx} ❌ expirylist failed")
                time.sleep(2)
                continue
            expiry_cache[idx] = expiries[0]

        expiry = expiry_cache[idx]

        data = oc_feed.option_chain(uscrip, useg, expiry)
        if not data:
            time.sleep(2)
            continue

        underlying_ltp = float(data.get("last_price") or 0.0)
        oc = data.get("oc") or {}
        if underlying_ltp <= 0 or not oc:
            time.sleep(2)
            continue

        step = STRIKE_STEP[idx]
        atm, ce_strike, pe_strike, ce_leg, pe_leg = pick_itm_from_oc(
            oc, underlying_ltp, step
        )

        if not ce_leg or not pe_leg:
            time.sleep(2)
            continue

        now_ts = time.time()
        if (
            current[idx]["atm"] is None
            or (atm != current[idx]["atm"]
                and (now_ts - current[idx]["last_switch"]) >= SWITCH_COOLDOWN_SEC)
        ):
            current[idx]["atm"] = atm
            current[idx]["last_switch"] = now_ts

            print(
                f"\n🔁 [{idx}] ITM PICK | ULTP:{underlying_ltp:.2f} "
                f"| EXP:{expiry} | ATM:{atm} | CE:{ce_strike} | PE:{pe_strike}\n"
            )

            try:
                ce_secid = master.find_option_security_id(idx, expiry, ce_strike, "CE")
                pe_secid = master.find_option_security_id(idx, expiry, pe_strike, "PE")
            except Exception as e:
                print(f"{datetime.now().strftime('%H:%M:%S')} | {idx} ❌ ID lookup:", e)
                time.sleep(3)
                continue

            depth_ws.subscribe([
                {"SecurityId": str(ce_secid), "tag": f"{idx}_CE_{ce_strike}"},
                {"SecurityId": str(pe_secid), "tag": f"{idx}_PE_{pe_strike}"},
            ])

        time.sleep(6.0)   # REST SAFE GAP


if __name__ == "__main__":
    main()
