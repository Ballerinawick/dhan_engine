import os
import time
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo


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
    
    IST = ZoneInfo("Asia/Kolkata")
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
        self.trades = self.positions
        self.realized_pnl = 0.0

        self.last_log_ts = 0.0
        self.log_interval = log_interval_sec
        self.stale_position_exit_sec = float(os.getenv("STALE_POSITION_EXIT_SEC", "90") or 90)

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
        self.current_day = datetime.now(self.IST).date()
        self.opened_today = 0
        self.closed_today = 0
        self.last_trade_summary = None
        self.enable_parsed_logs = False   # 🔥 toggle for debug logs

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
        today = datetime.fromtimestamp(now_ts, self.IST).date()
        if today != self.current_day:
            self.current_day = today
            self.opened_today = 0
            self.closed_today = 0

    def has_open_position(self):
        return len(self.positions) > 0

    def debug_position_snapshot(self):
        print("PAPER_TRADER_STATE →", {
            "open_positions": len(self.positions),
            "keys": list(self.positions.keys()),
            "cash": self.cash,
            "realized_pnl": self.realized_pnl,
        })

    # --------------------------------------------------
    # ENTRY
    # --------------------------------------------------
    def on_entry(self, secid, tag, side, ltp, lots=1, reason="ENTRY", metadata: dict | None = None):
        # 🚫 BLOCK ANY NEW ENTRY IF ONE EXISTS
        if self.has_open_position():
            print(
                f"⛔ ENTRY_BLOCKED_SINGLE_POSITION | Attempt:{tag} | Reason:Existing position active"
            )
            self.debug_position_snapshot()
            return False

        # if self.has_open_position():
        #     existing = next(iter(self.positions.values()))
        #     if existing["tag"] != tag:
        #         print("🔄 OPPOSITE_SIGNAL_DETECTED → EXIT THEN ENTRY")
        #         return False

        if secid in self.positions:
            self.debug_position_snapshot()
            return False

        side_norm = str(side).upper()
        if side_norm in {"SHORT", "SELL", "-1"}:
            print(
                f"🛑 LONG_ONLY_BLOCKED | attempted={side_norm} | secid={secid} | strategy={reason}"
            )
            self.debug_position_snapshot()
            return False

        index = self._extract_index(tag)
        if not index:
            self.debug_position_snapshot()
            return False

        lot_size = self.LOT_SIZES[index]
        qty = lots * lot_size
        cost = qty * ltp

        if cost > self.cash:
            self.debug_position_snapshot()
            return False

        now_ts = time.time()
        self.cash -= cost

        self._maybe_reset_daily_counts(now_ts)
        self.entries_total += 1
        self.entries_by_index[index] += 1
        self.opened_today += 1

        entry_record = {
            "secid": int(secid),
            "tag": tag,
            "side": side,
            "lots": lots,
            "lot_size": lot_size,
            "qty": qty,
            "entry": float(ltp),
            "ltp": float(ltp),
            "entry_ts": now_ts,
            "last_tick_ts": now_ts,
            "entry_reason": reason,
        }
        metadata = dict(metadata or {})
        strategy_owner = metadata.get("strategy_owner")
        if strategy_owner:
            entry_record["strategy_owner"] = strategy_owner
        entry_reason_source = metadata.get("entry_reason_source")
        if entry_reason_source:
            entry_record["entry_reason_source"] = entry_reason_source
        for key, value in metadata.items():
            if key not in entry_record:
                entry_record[key] = value

        self.positions[secid] = entry_record
        print(f"🔥 TRADE STORED CONFIRMED | {tag} | {ltp}")

        self.max_concurrent_open = max(self.max_concurrent_open, len(self.positions))
        self.open_positions_dirty = True

        print(
            f"✅ ENTRY_COMMITTED | {tag} | {side} | "
            f"Lots:{lots} | Qty:{qty} | Entry:{ltp:.2f} | Reason:{reason}"
        )
        print(
            f"💰 ENTRY_COST_ANALYSIS | "
            f"Entry:{ltp:.2f} | "
            f"RoundTripFee:{self.ROUND_TRIP_FEE}"
        )
        self.debug_position_snapshot()
        self._log_consolidated()
        return True

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
        entry_time_ist = datetime.fromtimestamp(pos["entry_ts"], self.IST).strftime("%H:%M:%S")
        exit_time_ist = datetime.fromtimestamp(now_ts, self.IST).strftime("%H:%M:%S")

        self.last_trade_summary = {
            "secid": int(secid),
            "tag": pos["tag"],
            "side": pos["side"],
            "lots": pos["lots"],
            "qty": qty,
            "entry": float(entry),
            "exit": float(ltp),
            "gross_pnl": float(gross_pnl),
            "fee": float(fee),
            "net_pnl": float(net_pnl),
            "hold_sec": float(hold_sec),
            "entry_reason": pos.get("entry_reason"),
            "exit_reason": reason,
            "entry_ts": float(pos["entry_ts"]),
            "exit_ts": float(now_ts),
            "entry_time": entry_time_ist,
            "exit_time": exit_time_ist,
        }

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
        print(
            f"📘 TRADE_SUMMARY | {pos['tag']} | "
            f"Entry:{entry:.2f} | Exit:{ltp:.2f} | "
            f"GrossPnL:{gross_pnl:+.2f} | Fee:{fee:.2f} | NetPnL:{net_pnl:+.2f} | "
            f"Hold:{self._fmt_duration(hold_sec)} | "
            f"EntryTime:{self.last_trade_summary['entry_time']} | ExitTime:{self.last_trade_summary['exit_time']} | "
            f"EntryReason:{pos.get('entry_reason')} | ExitReason:{reason}"
        )
        self.debug_position_snapshot()
        self._log_consolidated()

    def _log_open_positions(self):
        if not self.positions:
            return

        now_ts = time.time()

        for secid, pos in list(self.positions.items()):
            last_tick_ts = float(pos.get("last_tick_ts") or pos.get("entry_ts") or now_ts)
            stale_age = now_ts - last_tick_ts
            if self.stale_position_exit_sec > 0 and stale_age >= self.stale_position_exit_sec:
                ltp = float(pos.get("ltp", pos.get("entry", 0.0)) or 0.0)
                print(
                    f"⚠️ STALE_POSITION_EXIT | {pos['tag']} | "
                    f"secid:{secid} | stale_for:{stale_age:.1f}s | ltp:{ltp:.2f}"
                )
                self.on_exit(secid, ltp, reason="TRI_WAVE_V2_EXIT:STALE_MARKET_DATA")
                continue

            entry = float(pos["entry"])
            ltp = float(pos.get("ltp", entry))
            qty = int(pos["qty"])

            if pos["side"] == "LONG":
                pnl = (ltp - entry) * qty
            else:
                pnl = (entry - ltp) * qty

            pnl_pct = ((ltp - entry) / entry) * 100 if entry > 0 else 0.0
            hold_sec = now_ts - pos["entry_ts"]

            print(
                f"📊 OPEN_POSITION | {pos['tag']} | "
                f"Entry:{entry:.2f} | LTP:{ltp:.2f} | "
                f"PnL:{pnl:+.2f} ({pnl_pct:+.2f}%) | "
                f"Hold:{self._fmt_duration(hold_sec)}"
            )

    # --------------------------------------------------
    # TICK UPDATE
    # --------------------------------------------------
    def on_tick(self, secid, ltp):
        if secid in self.positions:
            self.positions[secid]["ltp"] = float(ltp)
            self.positions[secid]["last_tick_ts"] = time.time()

        # OPTIONAL DEBUG LOG
        if self.enable_parsed_logs:
            print(f"📡 TICK | {secid} | LTP:{ltp}")

        # OPEN POSITION TRACKING (THROTTLED)
        if not hasattr(self, "_last_open_log_ts"):
            self._last_open_log_ts = 0

        interval = 10
        try:
            from config.trading_config import CONFIG
            interval = CONFIG["logging"]["open_position_interval_sec"]
        except Exception:
            pass

        if time.time() - self._last_open_log_ts > interval:
            self._log_open_positions()
            self._last_open_log_ts = time.time()

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
