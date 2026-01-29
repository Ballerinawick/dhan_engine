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
    ✔ Open-position snapshot (entry, LTP, age) + exit reason in exit logs
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

    def _fmt_duration(self, seconds: float) -> str:
        m, s = divmod(int(seconds), 60)
        return f"{m}m{s:02d}s"

    # --------------------------------------------------
    # ENTRY (LOT BASED)
    # --------------------------------------------------
    def on_entry(self, secid, tag, side, ltp, lots=1, reason="ENTRY"):
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

        now_ts = time.time()
        self.cash -= cost

        self.positions[secid] = {
            "tag": tag,
            "side": side,
            "lots": lots,
            "lot_size": lot_size,
            "qty": qty,
            "entry": ltp,
            "ltp": ltp,
            "entry_ts": now_ts,
            "entry_reason": reason,
        }

        print(
            f"🟢 ENTRY | {tag} | {side} | "
            f"Lots:{lots} | Qty:{qty} | Entry:{ltp:.2f} | Reason:{reason}"
        )

    # --------------------------------------------------
    # EXIT (FULL LOT ONLY)
    # --------------------------------------------------
    def on_exit(self, secid, ltp, reason="EXIT"):
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

        hold_sec = time.time() - pos["entry_ts"]

        print(
            f"🔴 EXIT | {pos['tag']} | "
            f"Lots:{pos['lots']} | "
            f"Exit:{ltp:.2f} | "
            f"PnL:{pnl:+.2f} | "
            f"Hold:{self._fmt_duration(hold_sec)} | "
            f"Reason:{reason}"
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
    # CONSOLIDATED PORTFOLIO + OPEN SNAPSHOT
    # --------------------------------------------------
    def _log_consolidated(self):
        unrealized = 0.0
        used_margin = 0.0
        now = time.time()

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
            f"PremiumDeployed:{used_margin:.2f} | "
            f"Free:{self.cash:.2f} | "
            f"Unrealized:{unrealized:+.2f} | "
            f"Realized:{self.realized_pnl:+.2f} | "
            f"NetPnL:{net_pnl:+.2f}"
        )

        if not self.positions:
            return

        print("📌 OPEN POSITIONS:")
        for p in self.positions.values():
            hold = self._fmt_duration(now - p["entry_ts"])
            pnl = (p["ltp"] - p["entry"]) * p["qty"]

            print(
                f"  - {p['tag']} | "
                f"Entry:{p['entry']:.2f} | "
                f"LTP:{p['ltp']:.2f} | "
                f"Hold:{hold} | "
                f"PnL:{pnl:+.2f}"
            )
