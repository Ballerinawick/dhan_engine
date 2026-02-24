import time
from collections import defaultdict
from datetime import datetime


class PaperTradeManager:
    """
    PAPER TRADE MANAGER (LOT-BASED) — v3.1 REALISM

    ✔ Fixed lot sizes (NIFTY / BANKNIFTY / FINNIFTY)
    ✔ 1 lot per entry
    ✔ Full-lot exit only
    ✔ Consolidated MTM logging
    ✔ REALISTIC ₹60 round-trip fee per trade (Dhan approx)
    ✔ NO strategy or flow changes
    """

    # ---------------- LOT SIZES ----------------
    LOT_SIZES = {
        "NIFTY": 65,
        "BANKNIFTY": 30,
        "FINNIFTY": 60,
    }

    # ---------------- REALISTIC FEE MODEL ----------------
    ROUND_TRIP_FEE = 60.0   # ₹60 per completed trade (BUY + SELL)

    # ---------------- OBSERVABILITY FLAGS ----------------
    MIN_HOLD_SECONDS = None
    MAX_OPEN_POSITIONS = None
    MAX_INDEX_EXPOSURE = None
    FINNIFTY_SCORE_MULTIPLIER = None

    def __init__(self, capital=100000, log_interval_sec=5):
        self.initial_capital = float(capital)
        self.cash = float(capital)

        self.positions = {}
        self.realized_pnl = 0.0

        self.last_log_ts = 0.0
        self.log_interval = log_interval_sec

        # Metrics
        self.entries_total = 0
        self.exits_total = 0
        self.entries_by_index = defaultdict(int)
        self.exits_by_index = defaultdict(int)

        self.max_concurrent_open = 0
        self.total_hold_seconds = 0.0
        self.total_fees = 0.0
        self.open_positions_dirty = False
        self.recent_trade_pnls = []

        # Daily counters
        self.current_day = datetime.now().date()
        self.opened_today = 0
        self.closed_today = 0

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

    def _maybe_reset_daily_counts(self, now_ts: float) -> None:
        today = datetime.fromtimestamp(now_ts).date()
        if today != self.current_day:
            self.current_day = today
            self.opened_today = 0
            self.closed_today = 0

    # --------------------------------------------------
    # ENTRY
    # --------------------------------------------------
    def on_entry(self, secid, tag, side, ltp, lots=1, reason="ENTRY"):
        if secid in self.positions:
            return

        side_norm = str(side).upper()
        if side_norm in {"SHORT", "SELL", "-1"}:
            print(
                f"🛑 LONG_ONLY_BLOCKED | attempted={side_norm} | secid={secid} | strategy={reason}"
            )
            return

        index = self._extract_index(tag)
        if not index:
            return

        lot_size = self.LOT_SIZES[index]
        qty = lots * lot_size
        cost = qty * ltp

        if cost > self.cash:
            return

        now_ts = time.time()
        self.cash -= cost

        self._maybe_reset_daily_counts(now_ts)
        self.entries_total += 1
        self.entries_by_index[index] += 1
        self.opened_today += 1

        self.positions[secid] = {
            "tag": tag,
            "side": side,
            "lots": lots,
            "lot_size": lot_size,
            "qty": qty,
            "entry": float(ltp),
            "ltp": float(ltp),
            "entry_ts": now_ts,
            "entry_reason": reason,
        }

        self.max_concurrent_open = max(self.max_concurrent_open, len(self.positions))
        self.open_positions_dirty = True

        print(
            f"✅ ENTRY_COMMITTED | {tag} | {side} | "
            f"Lots:{lots} | Qty:{qty} | Entry:{ltp:.2f} | Reason:{reason}"
        )

    # --------------------------------------------------
    # EXIT (FULL LOT ONLY) — FEES APPLIED HERE
    # --------------------------------------------------
    def on_exit(self, secid, ltp, reason="EXIT"):
        pos = self.positions.pop(secid, None)
        if not pos:
            return

        entry = pos["entry"]
        qty = pos["qty"]
        side = pos["side"]

        if side == "LONG":
            gross_pnl = (ltp - entry) * qty
        else:
            gross_pnl = (entry - ltp) * qty

        # ✅ APPLY REALISTIC ROUND-TRIP FEE
        fee = self.ROUND_TRIP_FEE
        net_pnl = gross_pnl - fee

        self.cash += qty * ltp
        self.realized_pnl += net_pnl
        self.total_fees += fee

        now_ts = time.time()
        hold_sec = now_ts - pos["entry_ts"]
        self.total_hold_seconds += hold_sec

        self.exits_total += 1
        idx = self._extract_index(pos["tag"])
        if idx:
            self.exits_by_index[idx] += 1

        self._maybe_reset_daily_counts(now_ts)
        self.closed_today += 1
        self.open_positions_dirty = True

        self.recent_trade_pnls.append(net_pnl)
        if len(self.recent_trade_pnls) > 10:
            self.recent_trade_pnls = self.recent_trade_pnls[-10:]

        exit_tag = "EXIT_TIME" if "TIME" in str(reason).upper() else "EXIT_TURN"
        icon = "⏱️" if exit_tag == "EXIT_TIME" else "🚪"

        print(
            f"{icon} {exit_tag} | {pos['tag']} | "
            f"Lots:{pos['lots']} | "
            f"Exit:{ltp:.2f} | "
            f"PnL:{gross_pnl:+.2f} | "
            f"Hold:{self._fmt_duration(hold_sec)} | "
            f"Reason:{reason}"
        )

    # --------------------------------------------------
    # TICK UPDATE
    # --------------------------------------------------
    def on_tick(self, secid, ltp):
        if secid in self.positions:
            self.positions[secid]["ltp"] = float(ltp)

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
        now = time.time()
        self._maybe_reset_daily_counts(now)

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
        avg_hold_seconds = (self.total_hold_seconds / self.exits_total) if self.exits_total else 0.0
        churn_ratio = (self.exits_total / self.entries_total) if self.entries_total else 0.0

        print(
            f"📊 PORTFOLIO | "
            f"Open:{len(self.positions)} | "
            f"Capital:{self.initial_capital:.2f} | "
            f"PremiumDeployed:{used_margin:.2f} | "
            f"Free:{self.cash:.2f} | "
            f"Unrealized:{unrealized:+.2f} | "
            f"Realized:{self.realized_pnl:+.2f} | "
            f"NetPnL:{net_pnl:+.2f} | "
            f"EntriesTaken:{self.entries_total} | "
            f"ExitsTaken:{self.exits_total} | "
            f"OpenedToday:{self.opened_today} | "
            f"ClosedToday:{self.closed_today} | "
            f"AvgHoldTime:{avg_hold_seconds:.1f}s | "
            f"ChurnRatio:{churn_ratio:.2f} | "
            f"FeesPaid:₹{self.total_fees:.2f} | "
            f"NetPnL_AfterFees:₹{net_pnl:+.2f}"
        )