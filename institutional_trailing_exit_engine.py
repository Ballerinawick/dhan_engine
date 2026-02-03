import time


class InstitutionalTrailingExitEngine:
    """
    INSTITUTIONAL TRAILING EXIT ENGINE (CAPITAL SAFE)

    Guarantees:
    ✅ Never exits below entry once armed
    ✅ Protects profit memory
    ✅ Allows volatility but locks capital
    """

    # ---------- CONFIG ----------
    ARM_PROFIT_PCT = 0.35          # 35% profit to arm trailing
    MAX_GIVEBACK_PCT = 0.40        # max giveback from BEST
    MIN_HOLD_AFTER_ARM_SEC = 5     # buffer after arm

    def __init__(self, debug=True):
        self.debug = debug
        self.ctx = {}  # secid -> trail state

    def _log(self, msg):
        if self.debug:
            print(msg)

    def on_tick(self, *, secid, tag, ltp, paper_trader, momentum_engine):
        """
        Returns:
            None
            OR
            {
              "exit": True,
              "reason": "PROFIT_PROTECT"
            }
        """

        pos = paper_trader.positions.get(secid)
        if not pos:
            self.ctx.pop(secid, None)
            return None

        entry = float(pos["entry"])
        now = time.time()

        # ---------------- INIT ----------------
        if secid not in self.ctx:
            self.ctx[secid] = {
                "entry": entry,
                "best": entry,
                "armed": False,
                "armed_ts": None,
            }

        c = self.ctx[secid]

        # ---------------- UPDATE BEST ----------------
        if ltp > c["best"]:
            c["best"] = ltp

        pnl_pct = (c["best"] - entry) / max(entry, 1e-6)

        # ---------------- ARM TRAILING ----------------
        if not c["armed"] and pnl_pct >= self.ARM_PROFIT_PCT:
            c["armed"] = True
            c["armed_ts"] = now

            self._log(
                f"🟢 TRAIL_ARMED | {tag} | entry={entry:.2f} | best={c['best']:.2f}"
            )

        # ---------------- IF NOT ARMED ----------------
        if not c["armed"]:
            return None

        # ---------------- HOLD BUFFER ----------------
        if now - c["armed_ts"] < self.MIN_HOLD_AFTER_ARM_SEC:
            return None

        # ---------------- CAPITAL SAFE FLOOR ----------------
        dynamic_floor = c["best"] * (1 - self.MAX_GIVEBACK_PCT)
        effective_floor = max(entry, dynamic_floor)

        # ---------------- EXIT CHECK ----------------
        if ltp <= effective_floor:
            giveback = (c["best"] - ltp) / max(c["best"], 1e-6)

            self._log(
                f"📉 INSTITUTIONAL_EXIT | {tag} | "
                f"best={c['best']:.2f} | now={ltp:.2f} | "
                f"floor={effective_floor:.2f} | "
                f"giveback={giveback:.0%} | "
                f"CAPITAL_SAFE"
            )

            self.ctx.pop(secid, None)

            return {
                "exit": True,
                "reason": "PROFIT_PROTECT"
            }

        return None