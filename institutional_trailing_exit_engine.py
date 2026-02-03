import time


class InstitutionalTrailingExitEngine:
    """
    PROFIT MEMORY ENGINE (INSTITUTIONAL)

    Purpose:
    - Track BEST price after entry
    - Arm trailing after profit threshold
    - Exit on controlled giveback
    """

    # ---------- CONFIG ----------
    ARM_PROFIT_PCT = 0.35          # 35% profit to arm trailing
    MAX_GIVEBACK_PCT = 0.40        # allow 40% giveback from best
    MIN_HOLD_AFTER_ARM_SEC = 5     # avoid instant exits

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
                "armed_ts": None
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

        # ---------------- IF NOT ARMED → NO EXIT ----------------
        if not c["armed"]:
            return None

        # ---------------- HOLD BUFFER ----------------
        if now - c["armed_ts"] < self.MIN_HOLD_AFTER_ARM_SEC:
            return None

        # ---------------- GIVEBACK CHECK ----------------
        giveback = (c["best"] - ltp) / max(c["best"], 1e-6)

        if giveback >= self.MAX_GIVEBACK_PCT:
            floor = c["best"] * (1 - self.MAX_GIVEBACK_PCT)

            self._log(
                f"📉 INSTITUTIONAL_EXIT | {tag} | "
                f"best={c['best']:.2f} | now={ltp:.2f} | "
                f"floor={floor:.2f} | giveback={giveback:.0%}"
            )

            self.ctx.pop(secid, None)

            return {
                "exit": True,
                "reason": "PROFIT_PROTECT"
            }

        return None