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

    # ---------------- OBSERVABILITY FLAGS (DEFAULT OFF) ----------------
    MIN_HOLD_SECONDS = None
    MAX_OPEN_POSITIONS = None
    MAX_INDEX_EXPOSURE = None
    FINNIFTY_SCORE_MULTIPLIER = None

    # ---------------- FEE MODEL (REPORTING ONLY) ----------------
    BROKERAGE_PER_ORDER = 0.0
    TRANSACTION_CHARGE_PCT = 0.0
    TRANSACTION_CHARGE_FLAT = 0.0
    SLIPPAGE_BPS = 0.0
    FEE_PER_TRADE = 0.0

    def __init__(self, capital=100000, log_interval_sec=5):
        self.initial_capital = float(capital)
        self.cash = float(capital)

        self.positions = {}        # secid -> position dict
        self.realized_pnl = 0.0

        self.last_log_ts = 0.0
        self.log_interval = log_interval_sec

        # Metrics (reset on engine start)
        self.entries_total = 0
        self.exits_total = 0
        self.entries_by_index = defaultdict(int)
        self.exits_by_index = defaultdict(int)
        self.probe_entries = 0
        self.dominance_exits = 0
        self.normal_exits = 0
        self.max_concurrent_open = 0
        self.total_hold_seconds = 0.0
        self.total_fees = 0.0
        self.fee_drag_per_trade = 0.0
        self.fee_per_trade = float(self.FEE_PER_TRADE)
        self.open_positions_dirty = False
        self.recent_trade_pnls = []

        # Daily counters
        self.current_day = datetime.now().date()
        self.opened_today = 0
        self.closed_today = 0

        self._log_enabled_flags()

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

    def _calculate_order_fees(self, notional: float) -> float:
        return (
            self.BROKERAGE_PER_ORDER
            + self.TRANSACTION_CHARGE_FLAT
            + (notional * self.TRANSACTION_CHARGE_PCT)
            + (notional * (self.SLIPPAGE_BPS / 10000.0))
        )

    def _maybe_reset_daily_counts(self, now_ts: float) -> None:
        today = datetime.fromtimestamp(now_ts).date()
        if today != self.current_day:
            self.current_day = today
            self.opened_today = 0
            self.closed_today = 0

    def _log_enabled_flags(self) -> None:
        enabled_flags = []
        if self.MIN_HOLD_SECONDS is not None:
            enabled_flags.append(f"MIN_HOLD_SECONDS={self.MIN_HOLD_SECONDS}")
        if self.MAX_OPEN_POSITIONS is not None:
            enabled_flags.append(f"MAX_OPEN_POSITIONS={self.MAX_OPEN_POSITIONS}")
        if self.MAX_INDEX_EXPOSURE is not None:
            enabled_flags.append(f"MAX_INDEX_EXPOSURE={self.MAX_INDEX_EXPOSURE}")
        if self.FINNIFTY_SCORE_MULTIPLIER is not None:
            enabled_flags.append(
                f"FINNIFTY_SCORE_MULTIPLIER={self.FINNIFTY_SCORE_MULTIPLIER}"
            )
        if enabled_flags:
            print(f"🧭 TUNING FLAGS ENABLED | {' | '.join(enabled_flags)}")

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

        self._maybe_reset_daily_counts(now_ts)
        self.entries_total += 1
        self.entries_by_index[index] += 1
        if "PROBE" in reason.upper():
            self.probe_entries += 1
        self.opened_today += 1

        order_fees = self._calculate_order_fees(cost)
        self.total_fees += order_fees

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

        self.max_concurrent_open = max(self.max_concurrent_open, len(self.positions))
        self.open_positions_dirty = True

        print(
            f"✅ ENTRY_COMMITTED | {tag} | {side} | "
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

        now_ts = time.time()
        hold_sec = now_ts - pos["entry_ts"]
        self.total_hold_seconds += hold_sec
        self.exits_total += 1
        self.exits_by_index[self._extract_index(pos["tag"])] += 1
        if "DOMINANCE" in reason.upper():
            self.dominance_exits += 1
        else:
            self.normal_exits += 1
        self._maybe_reset_daily_counts(now_ts)
        self.closed_today += 1

        order_fees = self._calculate_order_fees(qty * ltp)
        self.total_fees += order_fees
        self.open_positions_dirty = True

        self.recent_trade_pnls.append(pnl)
        if len(self.recent_trade_pnls) > 10:
            self.recent_trade_pnls = self.recent_trade_pnls[-10:]

        exit_tag = "EXIT_TIME" if "TIME" in reason.upper() else "EXIT_TURN"
        icon = "⏱️" if exit_tag == "EXIT_TIME" else "🚪"
        print(
            f"{icon} {exit_tag} | {pos['tag']} | "
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
        fees_paid = (self.entries_total + self.exits_total) * self.fee_per_trade
        net_pnl_after_fees = net_pnl - fees_paid
        avg_hold_seconds = (
            self.total_hold_seconds / self.exits_total if self.exits_total else 0.0
        )
        churn_ratio = self.exits_total / self.entries_total if self.entries_total else 0.0
        self.fee_drag_per_trade = (
            self.total_fees / self.exits_total if self.exits_total else 0.0
        )

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
            f"FeesPaid:₹{fees_paid:.2f} | "
            f"NetPnL_AfterFees:₹{net_pnl_after_fees:+.2f}"
        )

        if not self.positions or not self.open_positions_dirty:
            return
        self.open_positions_dirty = False

    def note_regime_change(self, secid, tag, mode, reason):
        self.open_positions_dirty = True
