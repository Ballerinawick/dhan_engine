import os
import json
import asyncio
import time
import threading
import requests
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
load_dotenv()

from instrument_master import InstrumentMaster
from dhan_async_depth_adapter import DhanAsyncDepthAdapter
from ltp_rest_engine import DhanLtpRestEngine

from depth_micro_features import DepthMicroFeatureBuilder
from options_momentum_engine import OptionsMomentumEngine
from paper_trade_manager import PaperTradeManager
from option_chain_selector import OptionChainSelector
from institutional_decision_engine import InstitutionalDecisionEngine
from institutional_trailing_exit_engine import InstitutionalTrailingExitEngine
from structure_exit_engine import StructureExitEngine
from market_state_engine import MarketStateEngine
from turn_detection_engine import TurnDetectionEngine

try:
    from dhanhq.marketfeed import DhanFeed
except Exception:
    DhanFeed = None


# ================= CONFIG =================
CSV_FILE = os.getenv("CSV_FILE", "api-scrip-master.csv")

INDEXES = ["NIFTY"]
STRIKE_STEP = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50}

OPT_EXCHANGE_SEGMENT_20D = "NSE_FNO"

REST_POLL_INTERVAL_SEC = 1.1
REST_MAX_WAIT_SEC = 45
HEARTBEAT_SEC = 30.0
FULL_QUOTE_REQ_CODE = 21
FULL_QUOTE_SEGMENT = 2
FULL_QUOTE_LOG_SEC = 10

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


def _first_non_none(d: dict, *keys):
    for key in keys:
        if key in d and d[key] is not None:
            return d[key]
    return None


class FullMarketQuoteSampler:
    """
    Parallel broker full-marketfeed sampler.
    - Subscribes exactly to secids already chosen for depth stream.
    - Prints one JSON tick per second per secid.
    - Auto-stops after FULL_QUOTE_LOG_SEC logs per secid.
    """

    def __init__(self, client_id: str, token: str, secid_tag_map: dict):
        if DhanFeed is None:
            raise RuntimeError("dhanhq is not installed. Run: pip install dhanhq")

        self.client_id = str(client_id)
        self.token = str(token)
        self.secid_tag_map = {int(k): str(v) for k, v in secid_tag_map.items()}

        self._latest = {}
        self._count = {int(k): 0 for k in self.secid_tag_map.keys()}
        self._lock = threading.Lock()

    def _on_ticks(self, msg):
        ticks = msg if isinstance(msg, list) else [msg]
        with self._lock:
            for tick in ticks:
                sid = _first_non_none(tick, "security_id", "securityId", "sec_id", "SecurityId")
                if sid is None:
                    continue
                try:
                    sid = int(sid)
                except Exception:
                    continue
                if sid in self.secid_tag_map:
                    self._latest[sid] = tick

    @staticmethod
    def _extract(tick: dict, secid: int, tag: str) -> dict:
        broker_ts = _first_non_none(
            tick,
            "exchange_time", "exchangeTime",
            "last_trade_time", "lastTradeTime",
            "ltt", "timestamp", "time",
        )
        return {
            "secid": secid,
            "tag": tag,
            "ltp": _first_non_none(tick, "LTP", "ltp", "last_traded_price", "lastTradedPrice"),
            "ltq": _first_non_none(tick, "LTQ", "ltq", "last_traded_quantity", "lastTradedQuantity"),
            "total_volume": _first_non_none(tick, "volume", "Volume", "total_volume", "totalTradedVolume"),
            "oi": _first_non_none(tick, "oi", "OI", "open_interest", "openInterest"),
            "bid_price": _first_non_none(tick, "best_bid_price", "bestBidPrice", "bid_price", "bidPrice"),
            "ask_price": _first_non_none(tick, "best_ask_price", "bestAskPrice", "ask_price", "askPrice"),
            "iv": _first_non_none(tick, "iv", "IV", "implied_volatility", "impliedVolatility"),
            "delta": _first_non_none(tick, "delta", "Delta"),
            "timestamp": broker_ts if broker_ts is not None else datetime.now(IST).isoformat(),
            "raw": tick,
        }

    def run(self):
        secids = list(self.secid_tag_map.keys())
        if not secids:
            return

        # dhanhq marketfeed constructor expects a current loop in this thread.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        instruments = [(FULL_QUOTE_SEGMENT, sid, FULL_QUOTE_REQ_CODE) for sid in secids]
        feed = DhanFeed(
            client_id=self.client_id,
            access_token=self.token,
            instruments=instruments,
            version="v2",
        )
        feed.on_ticks = self._on_ticks

        ws_thread = threading.Thread(target=feed.run_forever, name="FullQuoteWS", daemon=True)
        ws_thread.start()

        try:
            while True:
                time.sleep(1.0)
                with self._lock:
                    snap = dict(self._latest)

                for sid in secids:
                    if self._count[sid] >= FULL_QUOTE_LOG_SEC:
                        continue
                    tick = snap.get(sid)
                    if not tick:
                        continue

                    payload = self._extract(tick, sid, self.secid_tag_map[sid])
                    print(json.dumps(payload, ensure_ascii=False))
                    self._count[sid] += 1

                if all(self._count[sid] >= FULL_QUOTE_LOG_SEC for sid in secids):
                    print("✅ Full marketfeed sampling completed (10s per instrument).")
                    break
        finally:
            try:
                if hasattr(feed, "disconnect"):
                    loop.run_until_complete(feed.disconnect())
            except Exception as e:
                print("⚠️ Full marketfeed disconnect warning:", e)
            finally:
                loop.close()
                asyncio.set_event_loop(None)


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
    market_engine = MarketStateEngine()
    turn_engine = TurnDetectionEngine()

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
    zero_book_counter = {}
    zero_book_warned = set()
    live_state = {
        "NIFTY": {
            "ce": None,
            "pe": None,
            "underlying": None,
            "ce_id": None,
            "pe_id": None,
            "ready_logged": False,
            "last_turn_signal": None
        }
    }

    def underlying_poll_loop():
        while True:
            try:
                if market_open():
                    ltp_map = ltp_rest.fetch_ltp_map({
                        "NSE_FNO": list(fut_secids.values())
                    })
                    if ltp_map:
                        for idx, secid in fut_secids.items():
                            if secid not in ltp_map:
                                continue
                            new_ltp = float(ltp_map[secid] or 0.0)
                            old_ltp = float(fut_ltp.get(idx, 0.0) or 0.0)
                            fut_ltp[idx] = new_ltp
                            if idx in live_state:
                                live_state[idx]["underlying"] = new_ltp
                            if abs(new_ltp - old_ltp) > 0:
                                print(f"📈 UNDERLYING_TICK | {idx} | old={old_ltp:.2f} | new={new_ltp:.2f}")
                    time.sleep(1.2)
                else:
                    time.sleep(5)
            except Exception as e:
                print(f"❌ UNDERLYING_POLL_ERROR | {e}")
                time.sleep(2)

    # ==================================================
    # OPTION DEPTH CALLBACK (INSTITUTIONAL FLOW)
    # ==================================================
    def on_opt_depth(secid: int, tag: str, bid, ask):
        try:
            if not market_open():
                return

            raw = feature_builder.build(secid, bid, ask)
            if not raw:
                return

            idx = "NIFTY"
            trade_id = idx

            # Assign CE / PE raw
            if secid == live_state[idx]["ce_id"]:
                live_state[idx]["ce"] = raw
            elif secid == live_state[idx]["pe_id"]:
                live_state[idx]["pe"] = raw

            s = live_state[idx]

            # Log once when fully ready
            if s["ce"] and s["pe"] and s["underlying"] and not s["ready_logged"]:
                s["ready_logged"] = True
                print(f"✅ PAIR_STATE_READY | {idx} | CE+PE+UNDERLYING LIVE")

            # Build snapshot
            if s["ce"] and s["pe"] and s["underlying"]:
                snapshot = market_engine.update(
                    idx,
                    s["underlying"],
                    s["ce"],
                    s["pe"]
                )

                # optional: keep last snapshot
                if snapshot:
                    s["last_snapshot"] = snapshot
                    turn_signal = turn_engine.update(snapshot)
                    if turn_signal:
                        live_state[idx]["last_turn_signal"] = turn_signal
                        signal_text = str(turn_signal.get("signal", "") or "")
                        route_tag = "CE" if "BULLISH" in signal_text else ("PE" if "BEARISH" in signal_text else tag)
                        route_ltp = raw["ltp"]
                        if route_tag == "CE" and s.get("ce"):
                            route_ltp = float(s["ce"].get("ltp", route_ltp) or route_ltp)
                        elif route_tag == "PE" and s.get("pe"):
                            route_ltp = float(s["pe"].get("ltp", route_ltp) or route_ltp)
                        decision_engine.on_signal(
                            secid=trade_id,
                            tag=route_tag,
                            ltp=route_ltp,
                            signal=turn_signal["signal"],
                            momentum_engine=momentum_engine,
                            paper_trader=paper_trader,
                            snapshot=snapshot
                        )

            bid_price = float(raw.get("bid_price", 0.0) or 0.0)
            ask_price = float(raw.get("ask_price", 0.0) or 0.0)
            if bid_price == 0.0 and ask_price == 0.0:
                count = zero_book_counter.get(secid, 0) + 1
                zero_book_counter[secid] = count
                if count >= 20 and secid not in zero_book_warned:
                    print(
                        f"⚠️ ZERO_BOOK | {tag} | secid={secid} | "
                        "depth prices are 0 — check Dhan depth entitlement/segment/subscription"
                    )
                    zero_book_warned.add(secid)
            else:
                zero_book_counter[secid] = 0
                zero_book_warned.discard(secid)

            raw["secid"] = secid
            raw["tag"] = tag
            raw["ts"] = time.time()

            # 0️⃣ MTM update
            paper_trader.on_tick(trade_id, raw["ltp"])

            # -------------- unified exit (CRITICAL FIX) --------------
            def force_exit(reason: str):
                # 1) decision engine must see EXIT to release locks/ctx safely
                try:
                    decision = decision_engine.on_signal(
                        secid=trade_id,
                        tag=tag,
                        ltp=raw["ltp"],
                        signal="EXIT",
                        momentum_engine=momentum_engine,
                        paper_trader=paper_trader,
                        snapshot=live_state.get(idx, {}).get("last_snapshot")
                    )
                    if decision and decision.get("exit_allowed") is False:
                        return False
                except Exception as e:
                    print("❌ force_exit: decision_engine EXIT error:", e)

                # 2) close paper position
                paper_trader.on_exit(trade_id, raw["ltp"], reason=reason)

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

            # 2️⃣ Institutional decision layer now handles entry routing via turn_engine.
            # Momentum engine is only used for active trade management and exits.
            decision = {"exit_allowed": True}
            if action == "EXIT":
                decision = decision_engine.on_signal(
                    secid=trade_id,
                    tag=tag,
                    ltp=raw["ltp"],
                    signal=action,
                    momentum_engine=momentum_engine,
                    paper_trader=paper_trader,
                    snapshot=live_state.get(idx, {}).get("last_snapshot")
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
    depth_adapter = DhanAsyncDepthAdapter(
        client_id=client_id,
        token=token,
        exchange_segment=OPT_EXCHANGE_SEGMENT_20D,
        on_depth=on_opt_depth
    )

    depth_adapter.start()

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
                    if idx in live_state:
                        live_state[idx]["underlying"] = fut_ltp[idx]
                        print(f"📈 UNDERLYING_UPDATE | {idx} | ltp={fut_ltp[idx]}")

            if all(fut_ltp[i] > 0 for i in INDEXES):
                print("✅ Initial FUT LTP captured.")
                threading.Thread(target=underlying_poll_loop, name="UnderlyingPoller", daemon=True).start()
                break

        time.sleep(REST_POLL_INTERVAL_SEC)

    # ================= OPTION SUBSCRIPTION =================
    print("\n🎯 Subscribing OPTIONS...")
    full_quote_secid_tag = {}

    for idx in INDEXES:
        try:
            selection = selector.select_best(idx)
            if not selection:
                continue

            subs = []
            if "CE" in selection:
                ce_secid = str(selection["CE"]["security_id"])
                subs.append({
                    "SecurityId": ce_secid,
                    "tag": f"{idx}_CE"
                })
                live_state[idx]["ce_id"] = int(ce_secid)
            if "PE" in selection:
                pe_secid = str(selection["PE"]["security_id"])
                subs.append({
                    "SecurityId": pe_secid,
                    "tag": f"{idx}_PE"
                })
                live_state[idx]["pe_id"] = int(pe_secid)

            if live_state[idx]["ce_id"] and live_state[idx]["pe_id"]:
                print(
                    f"🧩 PAIR_REGISTERED | {idx} | "
                    f"CE={live_state[idx]['ce_id']} | PE={live_state[idx]['pe_id']}"
                )

            if subs:
                instruments = []
                for s in subs:
                    instruments.append((2, s["SecurityId"], s["tag"]))
                    full_quote_secid_tag[int(s["SecurityId"])] = s["tag"]
                depth_adapter.subscribe(instruments)

        except Exception as e:
            print(f"❌ [{idx}] OptionChain error:", e)

    if full_quote_secid_tag:
        try:
            sampler = FullMarketQuoteSampler(
                client_id=client_id,
                token=token,
                secid_tag_map=full_quote_secid_tag,
            )
            threading.Thread(target=sampler.run, name="FullQuoteSampler", daemon=True).start()
            print(f"✅ Full marketfeed sampler started for {len(full_quote_secid_tag)} instruments")
        except Exception as e:
            print("⚠️ Full marketfeed sampler not started:", e)

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
