# run_nifty_itm_live.py
from instrument_master import InstrumentMaster
from depth_ws_engine import DepthWSEngine
from datetime import datetime

CSV_FILE = "api-scrip-master.csv"

# ---------- SCALP SETTINGS ----------
FUT_IMB_ENTRY = 0.20
OPT_IMB_CONFIRM = 0.25
MAX_SPREAD_OPT = 1.00     # keep loose now; tighten later when we confirm typical option spreads
MAX_HOLD_SECS = 30
# -----------------------------------

class PaperTrader:
    def __init__(self):
        self.position = None  # None / "CE" / "PE"
        self.entry_price = None
        self.entry_time = None

    def maybe_enter(self, fut, ce, pe):
        if self.position is not None:
            return

        # FUT bias
        if fut["imbalance"] > FUT_IMB_ENTRY:
            # confirm on CE
            if ce["imbalance"] > OPT_IMB_CONFIRM and ce["spread"] <= MAX_SPREAD_OPT:
                self.position = "CE"
                self.entry_price = ce["best_ask"]  # buy at ask
                self.entry_time = datetime.now()
                print(f"🟢 PAPER BUY CE @ {self.entry_price:.2f}")

        elif fut["imbalance"] < -FUT_IMB_ENTRY:
            # confirm on PE
            if pe["imbalance"] < -OPT_IMB_CONFIRM and pe["spread"] <= MAX_SPREAD_OPT:
                self.position = "PE"
                self.entry_price = pe["best_ask"]
                self.entry_time = datetime.now()
                print(f"🔴 PAPER BUY PE @ {self.entry_price:.2f}")

    def maybe_exit(self, fut, ce, pe):
        if self.position is None:
            return

        now = datetime.now()
        held = (now - self.entry_time).total_seconds()

        if self.position == "CE":
            # exit conditions
            if fut["imbalance"] < 0 or held > MAX_HOLD_SECS:
                exit_price = ce["best_bid"]  # sell at bid
                pnl = exit_price - self.entry_price
                print(f"✅ PAPER EXIT CE @ {exit_price:.2f} | PnL={pnl:+.2f} | held={held:.1f}s")
                self.position = None

        if self.position == "PE":
            if fut["imbalance"] > 0 or held > MAX_HOLD_SECS:
                exit_price = pe["best_bid"]
                pnl = exit_price - self.entry_price
                print(f"✅ PAPER EXIT PE @ {exit_price:.2f} | PnL={pnl:+.2f} | held={held:.1f}s")
                self.position = None


def main():
    master = InstrumentMaster(CSV_FILE)

    # 1) FUT
    fut = master.get_nearest_future("NIFTY")
    print("\n✅ NIFTY FUT:", fut)

    # 2) For live session: use current NIFTY FUT price (manual fast input)
    # (Later we can compute mid from FUT depth itself once stable)
    fut_ltp = float(input("\nEnter current NIFTY FUT LTP (example 26225.4): ").strip())

    # 3) ITM CE+PE
    itm = master.get_itm_ce_pe("NIFTY", fut_ltp=fut_ltp, strike_step=50, itm_steps=1)
    print("\n✅ ITM PICK:", itm)

    # 4) subscribe FUT + CE + PE
    instruments = [
        {"ExchangeSegment": "NSE_FNO", "SecurityId": fut["security_id"], "tag": "NIFTY_FUT"},
        {"ExchangeSegment": "NSE_FNO", "SecurityId": itm["ce"]["security_id"], "tag": f"NIFTY_ITM_CE_{itm['ce']['strike']}"},
        {"ExchangeSegment": "NSE_FNO", "SecurityId": itm["pe"]["security_id"], "tag": f"NIFTY_ITM_PE_{itm['pe']['strike']}"},
    ]

    engine = DepthWSEngine(instruments)

    # --- PATCH engine to include paper trade on every update ---
    trader = PaperTrader()

    orig_on_message = engine.on_message

    def wrapped_on_message(ws, message):
        orig_on_message(ws, message)

        # after orig updates books, attempt paper logic when all three are ready
        fut_book = engine.books.get(fut["security_id"])
        ce_book  = engine.books.get(itm["ce"]["security_id"])
        pe_book  = engine.books.get(itm["pe"]["security_id"])

        if not fut_book or not ce_book or not pe_book:
            return
        if not (fut_book.ready() and ce_book.ready() and pe_book.ready()):
            return

        fut_s = fut_book.top5_stats()
        ce_s  = ce_book.top5_stats()
        pe_s  = pe_book.top5_stats()
        if not (fut_s and ce_s and pe_s):
            return

        # enter/exit
        trader.maybe_enter(fut_s, ce_s, pe_s)
        trader.maybe_exit(fut_s, ce_s, pe_s)

    engine.on_message = wrapped_on_message

    print("\n🚀 Starting WS for NIFTY FUT + ITM CE/PE ...\n")
    engine.run()


if __name__ == "__main__":
    main()
