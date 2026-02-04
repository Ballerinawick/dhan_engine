import time
from collections import defaultdict, deque


class InstitutionalDecisionEngine:
    """
    INSTITUTIONAL DECISION ENGINE — REENTRY FIX (v2)

    Fixes:
    ✅ Track last_exit_side per index (real flip detection)
    ✅ After exit:
        - cooldown always
        - SAME SIDE re-entry: no shadow + relaxed structure
        - OPPOSITE flip: requires shadow confirmation + structure
    ✅ Prevents dead engine after first trade cycle
    """

    # ---------------- CONFIG ----------------
    STRUCTURE_LOOKBACK_SEC = 45
    STRUCTURE_MIN_POINTS = 6
    STRUCTURE_COMPRESSION_PCT = 0.12
    BALANCE_LOOKBACK_SEC = 30

    # Post-entry validation (SCALP only)
    DISPLACEMENT_THRESHOLD_PCT = 0.15
    HOLD_CONFIRM_SEC = 3
    POST_ENTRY_VALIDATE_MAX_SEC = 6

    # Mode gating
    MODE_DEFAULT = "SCALP"
    MODE_UPGRADE_CONFIRM_TICKS = 3

    # Flip safety
    FLIP_COOLDOWN_SEC = 12
    SHADOW_CONFIRM_TICKS = 3
    SHADOW_WINDOW_SEC = 30

    # Re-entry leniency (same side only)
    REENTRY_ALLOW_WITHOUT_STRUCT = True

    def __init__(self, debug=True):
        self.debug = debug

        self.trade_ctx = {}  # secid -> ctx
        self.price_track = defaultdict(deque)  # secid -> price history

        # INDEX-LEVEL locks
        self.index_active_side = {}        # index -> "CE"/"PE"
        self.index_active_secid = {}       # index -> secid currently in position
        self.index_last_exit_ts = {}       # index -> timestamp of last dynamic exit

        # ✅ NEW: store last exited side
        self.index_last_exit_side = {}     # index -> "CE"/"PE"

        # Shadow tracking
        self.shadow = defaultdict(lambda: {
            "CE": {"ticks": 0, "last_ts": 0.0, "last_ltp": 0.0, "structure_ok": False},
            "PE": {"ticks": 0, "last_ts": 0.0, "last_ltp": 0.0, "structure_ok": False},
        })

    # --------------------------------------------------
    def _log(self, msg):
        if self.debug:
            print(msg)

    def _index_from_tag(self, tag: str) -> str:
        return tag.split("_")[0].strip().upper()

    def _side_from_tag(self, tag: str) -> str:
        return "CE" if "CE" in tag.upper() else "PE"

    def _update_price_history(self, secid, now, ltp):
        q = self.price_track[secid]
        q.append((now, float(ltp)))
        while q and now - q[0][0] > self.STRUCTURE_LOOKBACK_SEC:
            q.popleft()

    def _balance_price(self, secid, now, fallback):
        pts = [p for ts, p in self.price_track[secid] if now - ts <= self.BALANCE_LOOKBACK_SEC]
        return sum(pts) / len(pts) if pts else float(fallback)

    def _structure_ok(self, ltp, secid):
        prices = list(self.price_track[secid])
        if len(prices) < self.STRUCTURE_MIN_POINTS:
            return False

        vals = [p for _, p in prices]
        rng = max(vals) - min(vals)
        last = vals[-1]
        return rng <= max(last, 1e-6) * self.STRUCTURE_COMPRESSION_PCT

    def _ctx(self, secid, tag, entry, ts):
        ctx = self.trade_ctx.get(secid)
        if not ctx:
            ctx = {
                "tag": tag,
                "entry": float(entry),
                "ts": float(ts),

                "mode": self.MODE_DEFAULT,
                "accept_count": 0,

                "post_validate_until": float(ts) + self.POST_ENTRY_VALIDATE_MAX_SEC,
                "disp_start": None,
                "disp_confirmed": False
            }
            self.trade_ctx[secid] = ctx
        return ctx

    # --------------------------------------------------
    def _shadow_update(self, index: str, side: str, now: float, ltp: float, struct_ok: bool):
        s = self.shadow[index][side]
        if s["last_ts"] and (now - s["last_ts"] > self.SHADOW_WINDOW_SEC):
            s["ticks"] = 0
        s["ticks"] += 1
        s["last_ts"] = now
        s["last_ltp"] = float(ltp)
        s["structure_ok"] = bool(struct_ok)

    def _shadow_is_confirmed(self, index: str, side: str, now: float) -> bool:
        s = self.shadow[index][side]
        if now - s["last_ts"] > self.SHADOW_WINDOW_SEC:
            return False
        return (s["ticks"] >= self.SHADOW_CONFIRM_TICKS) and s["structure_ok"]

    def _flip_cooldown_ok(self, index: str, now: float) -> bool:
        last = self.index_last_exit_ts.get(index)
        if not last:
            return True
        return (now - last) >= self.FLIP_COOLDOWN_SEC

    # --------------------------------------------------
    def on_signal(self, *, secid, tag, ltp, signal, momentum_engine, paper_trader):
        now = time.time()

        index = self._index_from_tag(tag)
        side = self._side_from_tag(tag)

        # update history for structure
        self._update_price_history(secid, now, ltp)
        struct_ok = self._structure_ok(ltp, secid)

        # always shadow-track
        self._shadow_update(index, side, now, ltp, struct_ok)

        if signal in ("A_ENTRY", "B_ENTRY", "EXIT"):
            self._log(f"🧠 SIGNAL | {index} | {tag} | side={side} | sig={signal} | ltp={float(ltp):.2f} | struct={struct_ok}")

        # =============================
        # ENTRY
        # =============================
        if signal in ("A_ENTRY", "B_ENTRY"):
            trade = momentum_engine.active_trade.get(secid)
            if not trade:
                self._log(f"⛔ ENTRY_BLOCK | {tag} | reason=NO_ACTIVE_TRADE")
                return {"entry_allowed": False}

            locked_side = self.index_active_side.get(index)
            locked_secid = self.index_active_secid.get(index)

            # index already in position
            if locked_side is not None and locked_secid in paper_trader.positions:
                self._log(f"⛔ ENTRY_BLOCK | {index} | requested={side} | locked={locked_side} | reason=INDEX_ALREADY_IN_POSITION")
                return {"entry_allowed": False}

            # stale lock cleanup
            if locked_side is not None and (locked_secid not in paper_trader.positions):
                self._log(f"🧹 LOCK_CLEANUP | {index} | cleared stale lock side={locked_side}")
                self.index_active_side.pop(index, None)
                self.index_active_secid.pop(index, None)
                locked_side = None

            # -------- cooldown after any exit --------
            if index in self.index_last_exit_ts:
                if not self._flip_cooldown_ok(index, now):
                    remain = self.FLIP_COOLDOWN_SEC - (now - self.index_last_exit_ts[index])
                    self._log(f"⏳ COOLDOWN_WAIT | {index} | remain={remain:.1f}s | requested={side}")
                    return {"entry_allowed": False}

                last_exit_side = self.index_last_exit_side.get(index)

                # ✅ REAL flip detection
                is_flip = (last_exit_side is not None and last_exit_side != side)

                if is_flip:
                    # flip requires shadow confirm
                    if not self._shadow_is_confirmed(index, side, now):
                        sh = self.shadow[index][side]
                        self._log(
                            f"🛑 FLIP_BLOCK | {index} | side={side} | reason=SHADOW_NOT_CONFIRMED "
                            f"| ticks={sh['ticks']} need={self.SHADOW_CONFIRM_TICKS} | struct={sh['structure_ok']}"
                        )
                        return {"entry_allowed": False}
                    self._log(f"✅ FLIP_OK | {index} | {last_exit_side}→{side} | cooldown_ok + shadow_confirmed")
                else:
                    self._log(f"✅ REENTRY_OK | {index} | side={side} | cooldown_ok (same side)")

                    # ✅ same-side re-entry: relax structure if enabled
                    if self.REENTRY_ALLOW_WITHOUT_STRUCT:
                        struct_ok = True

            # Structure gating (still applies for first entry or flips)
            if not struct_ok:
                self._log(f"⛔ ENTRY_BLOCK | {tag} | reason=STRUCT_NOT_OK")
                return {"entry_allowed": False}

            # Commit entry
            if secid not in paper_trader.positions:
                paper_trader.on_entry(
                    secid=secid,
                    tag=tag,
                    side="LONG",
                    ltp=trade.get("entry", ltp),
                    lots=1,
                    reason=f"{signal}|STRUCT_OK"
                )

                self.index_active_side[index] = side
                self.index_active_secid[index] = secid
                self._ctx(secid, tag, trade.get("entry", ltp), now)

                self._log(f"✅ ENTRY_COMMITTED | {index} | {tag} | side={side} | lock=INDEX")

            return {"entry_allowed": True}

        # =============================
        # EXIT (DYNAMIC ONLY)
        # =============================
        if signal == "EXIT":
            ctx = self.trade_ctx.get(secid)

            if not ctx:
                self._log(f"⚠️ EXIT_NO_CTX | {tag} | allow=True")
                return {"exit_allowed": True}

            reason = momentum_engine.last_exit_reason.get(secid, "EXIT")

            if ctx["mode"] == "TREND" and str(reason).upper() in (
                "ENTRY_REJECTED_CONFIRM",
                "ENTRY_INVALIDATED",
                "ENTRY_INVALIDATED_NO_DISPLACEMENT"
            ):
                self._log(f"🛑 EXIT_VETO | {index} | {tag} | mode=TREND | reason={reason}")
                return {"exit_allowed": False}

            self.trade_ctx.pop(secid, None)

            if self.index_active_secid.get(index) == secid:
                old = self.index_active_side.get(index)

                # ✅ store last exit side BEFORE clearing
                if old:
                    self.index_last_exit_side[index] = old

                self.index_active_side.pop(index, None)
                self.index_active_secid.pop(index, None)
                self.index_last_exit_ts[index] = now

                self._log(
                    f"🚪 EXIT_DYNAMIC | {index} | {tag} | reason={reason} | "
                    f"lock_released={old} | last_exit_side={self.index_last_exit_side.get(index)} | "
                    f"cooldown={self.FLIP_COOLDOWN_SEC}s"
                )
            else:
                self._log(f"🚪 EXIT_DYNAMIC | {index} | {tag} | reason={reason} | note=non_locked_secid_exit")

            return {"exit_allowed": True, "exit_reason": "DYNAMIC"}

        # =============================
        # POST-ENTRY VALIDATION
        # =============================
        if secid not in paper_trader.positions:
            return

        ctx = self.trade_ctx.get(secid)
        if not ctx:
            return

        # Upgrade mode SCALP -> TREND
        ctx["accept_count"] += 1
        if ctx["mode"] == "SCALP" and ctx["accept_count"] >= self.MODE_UPGRADE_CONFIRM_TICKS:
            ctx["mode"] = "TREND"
            self._log(f"🧭 MODE_UPGRADE | {index} | {tag} | SCALP→TREND | ticks={ctx['accept_count']}")

        # TREND: no post-entry kill
        if ctx["mode"] == "TREND":
            return

        # SCALP validation
        bal = self._balance_price(secid, now, ltp)
        disp = abs(float(ltp) - bal) / max(bal, 1e-6)

        if disp >= self.DISPLACEMENT_THRESHOLD_PCT:
            if ctx["disp_start"] is None:
                ctx["disp_start"] = now
        else:
            ctx["disp_start"] = None

        if ctx["disp_start"] and (now - ctx["disp_start"]) >= self.HOLD_CONFIRM_SEC:
            ctx["disp_confirmed"] = True
            self._log(f"✅ POST_ENTRY_CONFIRMED | {index} | {tag} | disp={disp:.4f}")
            return

        if now > ctx["post_validate_until"]:
            paper_trader.on_exit(secid, float(ltp), reason="ENTRY_INVALIDATED")
            momentum_engine.active_trade.pop(secid, None)

            self.trade_ctx.pop(secid, None)

            if self.index_active_secid.get(index) == secid:
                old = self.index_active_side.get(index)
                if old:
                    self.index_last_exit_side[index] = old

                self.index_active_side.pop(index, None)
                self.index_active_secid.pop(index, None)
                self.index_last_exit_ts[index] = now
                self._log(f"❌ POST_ENTRY_KILL | {index} | {tag} | reason=ENTRY_INVALIDATED | lock_released={old}")

            return