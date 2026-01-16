import time
from collections import defaultdict
from datetime import datetime


class PaperTradeManager:
    """
    PAPER TRADE MANAGER (LOT-BASED)

    ✔ Fixed lot sizes (NIFTY / BANKNIFTY / FINNIFTY)
    ✔ 1 lot per entry (scalable later)
    ✔ Full-lot exit only
    ✔ Consolidated MTM logging
    ✔ Clean, low-noise logs
    """

    # ---------------- LOT SIZES ----------------
    LOT_SIZES = {
        "NIFTY": 65,
        "BANKNIFTY": 30,
        "FINNIFTY": 60,
    }

    def __init__(self, capital=100000, log_interval_sec=5):
        self.initial_capital = float(capital)
        self.cash = float(capital)

        self.positions = {}        # secid -> position dict
        self.realized_pnl = 0.0

        self.last_log_ts = 0.0
        self.log_interval = log_interval_sec

    # --------------------------------------------------
    # INTERNAL HELPERS
    # --------------------------------------------------
    def _extract_index(self, tag: str) -> str:
        for idx in self.LOT_SIZES:
            if tag.startswith(idx):
                return idx
        return None

    # --------------------------------------------------
    # ENTRY (LOT BASED)
    # --------------------------------------------------
    def on_entry(self, secid, tag, side, ltp, lots=1):
        if secid in self.positions:
            return  # already open

        if side == "SHORT":
            return

        index = self._extract_index(tag)
        if not index:
            return

        lot_size = self.LOT_SIZES[index]
        qty = lots * lot_size
        cost = qty * ltp

        if cost > self.cash:
            return  # insufficient capital

        self.cash -= cost

        self.positions[secid] = {
            "tag": tag,
            "side": side,
            "lots": lots,
            "lot_size": lot_size,
            "qty": qty,
            "entry": ltp,
            "ltp": ltp,
        }

        print(
            f"🟢 ENTRY | {tag} | {side} | "
            f"Lots:{lots} | Qty:{qty} | Entry:{ltp:.2f}"
        )

    # --------------------------------------------------
    # EXIT (FULL LOT ONLY)
    # --------------------------------------------------
    def on_exit(self, secid, ltp):
        pos = self.positions.pop(secid, None)
        if not pos:
            return

        entry = pos["entry"]
        qty = pos["qty"]
        side = pos["side"]

        if side == "LONG":
            pnl = (ltp - entry) * qty
        else:
            pnl = (entry - ltp) * qty

        self.cash += qty * ltp
        self.realized_pnl += pnl

        print(
            f"🔴 EXIT | {pos['tag']} | "
            f"Lots:{pos['lots']} | "
            f"Exit:{ltp:.2f} | PnL:{pnl:+.2f}"
        )

    # --------------------------------------------------
    # TICK UPDATE (NO PER-SYMBOL LOG)
    # --------------------------------------------------
    def on_tick(self, secid, ltp):
        if secid in self.positions:
            self.positions[secid]["ltp"] = ltp

        now = time.time()
        if now - self.last_log_ts >= self.log_interval:
            self.last_log_ts = now
            self._log_consolidated()

    # --------------------------------------------------
    # CONSOLIDATED PORTFOLIO LOG
    # --------------------------------------------------
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
            f"📊 PORTFOLIO | "
            f"Open:{len(self.positions)} | "
            f"Capital:{self.initial_capital:.2f} | "
            f"Used:{used_margin:.2f} | "
            f"Free:{self.cash:.2f} | "
            f"Unrealized:{unrealized:+.2f} | "
            f"Realized:{self.realized_pnl:+.2f} | "
            f"NetPnL:{net_pnl:+.2f}"
        )
