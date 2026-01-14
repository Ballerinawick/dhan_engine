# paper_trade_manager.py
from collections import defaultdict
from datetime import datetime
import pytz


class PaperTradeManager:
    """
    PAPER TRADE + LOG MANAGER

    Responsibilities:
    1) Manage ₹ capital (default 10,000)
    2) Track OPEN positions
    3) Update LIVE MTM on every tick
    4) Book PnL on EXIT
    5) Clean & throttled logs (no spam)

    This class does NOT decide entry/exit.
    It only reacts to signals.
    """

    IST = pytz.timezone("Asia/Kolkata")

    def __init__(self, capital=10000):
        self.capital = capital

        # secid -> position
        self.positions = {}

        # secid -> realized pnl
        self.realized_pnl = defaultdict(float)

        # log throttling
        self.last_mtm_log_sec = {}
        self.MTM_LOG_EVERY_SEC = 5  # print MTM every 5 seconds per instrument

    # --------------------------------------------------
    # ENTRY
    # --------------------------------------------------
    def on_entry(self, secid, tag, side, ltp):
        if secid in self.positions:
            return  # already in position

        qty = max(int(self.capital // ltp), 1)

        self.positions[secid] = {
            "tag": tag,
            "side": side,
            "entry": ltp,
            "qty": qty,
            "entry_time": datetime.now(self.IST),
        }

        print(
            f"🟢 ENTRY | {tag} | {side} | "
            f"Qty:{qty} | Entry:{ltp:.2f}"
        )

    # --------------------------------------------------
    # EXIT
    # --------------------------------------------------
    def on_exit(self, secid, ltp):
        pos = self.positions.pop(secid, None)
        if not pos:
            return

        side = pos["side"]
        entry = pos["entry"]
        qty = pos["qty"]

        pnl = (ltp - entry) * qty if side == "LONG" else (entry - ltp) * qty
        self.realized_pnl[secid] += pnl

        print(
            f"🔴 EXIT | {pos['tag']} | "
            f"Exit:{ltp:.2f} | "
            f"PnL:{pnl:.2f} | "
            f"Total:{self.realized_pnl[secid]:.2f}"
        )

    # --------------------------------------------------
    # LIVE MTM (called every tick)
    # --------------------------------------------------
    def on_tick(self, secid, ltp):
        pos = self.positions.get(secid)
        if not pos:
            return

        now_sec = int(datetime.now(self.IST).timestamp())
        if self.last_mtm_log_sec.get(secid) == now_sec:
            return

        if now_sec % self.MTM_LOG_EVERY_SEC != 0:
            return

        self.last_mtm_log_sec[secid] = now_sec

        side = pos["side"]
        entry = pos["entry"]
        qty = pos["qty"]

        mtm = (ltp - entry) * qty if side == "LONG" else (entry - ltp) * qty

        print(
            f"📈 MTM | {pos['tag']} | "
            f"LTP:{ltp:.2f} | "
            f"MTM:{mtm:.2f}"
        )