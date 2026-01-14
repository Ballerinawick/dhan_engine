import time
from collections import defaultdict
from datetime import datetime


class PaperTradeManager:
    def __init__(self, capital=10000, log_interval_sec=5):
        self.initial_capital = capital
        self.cash = capital

        self.positions = {}   # secid -> position dict
        self.realized_pnl = 0.0

        self.last_log_ts = 0.0
        self.log_interval = log_interval_sec

    # ---------------- ENTRY ----------------
    def on_entry(self, secid, tag, side, ltp, qty=None):
        if secid in self.positions:
            return  # already open

        if qty is None:
            qty = max(int(self.cash / (ltp * 1.1)), 1)

        cost = qty * ltp
        if cost > self.cash:
            return

        self.cash -= cost

        self.positions[secid] = {
            "tag": tag,
            "side": side,
            "qty": qty,
            "entry": ltp,
            "ltp": ltp
        }

        print(
            f"🟢 ENTRY | {tag} | {side} | Qty:{qty} | Entry:{ltp:.2f}"
        )

    # ---------------- EXIT ----------------
    def on_exit(self, secid, ltp):
        pos = self.positions.pop(secid, None)
        if not pos:
            return

        if pos["side"] == "LONG":
            pnl = (ltp - pos["entry"]) * pos["qty"]
        else:
            pnl = (pos["entry"] - ltp) * pos["qty"]

        self.cash += (pos["qty"] * ltp)
        self.realized_pnl += pnl

        print(
            f"🔴 EXIT | {pos['tag']} | Qty:{pos['qty']} | "
            f"Exit:{ltp:.2f} | PnL:{pnl:.2f}"
        )

    # ---------------- MTM UPDATE ----------------
    def on_tick(self, secid, ltp):
        if secid in self.positions:
            self.positions[secid]["ltp"] = ltp

        now = time.time()
        if now - self.last_log_ts >= self.log_interval:
            self.last_log_ts = now
            self._log_consolidated()

    # ---------------- CONSOLIDATED LOG ----------------
    def _log_consolidated(self):
        unrealized = 0.0
        used_margin = 0.0

        for p in self.positions.values():
            entry = p["entry"]
            ltp = p["ltp"]
            qty = p["qty"]

            used_margin += entry * qty

            if p["side"] == "LONG":
                unrealized += (ltp - entry) * qty
            else:
                unrealized += (entry - ltp) * qty

        net_pnl = self.realized_pnl + unrealized

        print(
            f"📊 POSITIONS | Open:{len(self.positions)} | "
            f"Capital:{self.initial_capital:.2f} | "
            f"Used:{used_margin:.2f} | "
            f"Free:{self.cash:.2f} | "
            f"Unrealized:{unrealized:+.2f} | "
            f"Realized:{self.realized_pnl:+.2f} | "
            f"NetPnL:{net_pnl:+.2f}"
        )