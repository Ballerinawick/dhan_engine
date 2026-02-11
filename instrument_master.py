# instrument_master.py
import pandas as pd


class InstrumentMaster:
    """
    InstrumentMaster v3.1 — Expiry-Aware + Strike-Tolerant

    FIXES:
    ✅ NIFTY uses WEEKLY expiry (nearest weekly)
    ✅ BANKNIFTY / FINNIFTY use MONTHLY expiry (last expiry in month)
    ✅ Expiry selection happens BEFORE premium filter (as per your rule)
    ✅ Strike selection is tolerant (prevents "No exact strike match" crashes)
    ✅ Still supports:
        - FUTIDX nearest
        - OPTIDX lookup by (index, expiry, strike, CE/PE)
        - INDEX spot SecurityId for OptionChain REST
    """

    # ---------------- EXPIRY MODE RULES ----------------
    # Your confirmed preference:
    # NIFTY = WEEKLY
    # BANKNIFTY / FINNIFTY = MONTHLY
    EXPIRY_MODE_BY_INDEX = {
        "NIFTY": "WEEKLY",
        "BANKNIFTY": "MONTHLY",
        "FINNIFTY": "MONTHLY",
    }

    # ---------------- STRIKE TOLERANCE ----------------
    # Dhan CSV sometimes has strikes like 25900.0 exactly,
    # but depending on rounding / chain selection you can land near it.
    # We accept nearest strike if within a reasonable tolerance.
    STRIKE_TOL_BY_INDEX = {
        "NIFTY": 0.5,       # should be exact typically; allow tiny float noise
        "BANKNIFTY": 0.5,
        "FINNIFTY": 0.5,
    }

    def __init__(self, csv_path: str):
        self.df = pd.read_csv(csv_path, low_memory=False)
        self.df.columns = self.df.columns.astype(str).str.strip()
        print("✅ CSV LOADED | rows:", len(self.df))

        # Normalize string columns
        obj_cols = self.df.select_dtypes(include=["object"]).columns
        for c in obj_cols:
            self.df[c] = self.df[c].astype(str).str.strip()

        required = [
            "SEM_EXM_EXCH_ID",
            "SEM_SEGMENT",
            "SEM_SMST_SECURITY_ID",
            "SEM_INSTRUMENT_NAME",
            "SEM_TRADING_SYMBOL",
            "SEM_CUSTOM_SYMBOL",
            "SEM_OPTION_TYPE",
            "SEM_STRIKE_PRICE",
            "SEM_LOT_UNITS",
            "SEM_EXPIRY_DATE",
        ]
        for r in required:
            if r not in self.df.columns:
                raise KeyError(f"Missing required column: {r}")

        # Parse expiry once
        self.df["SEM_EXPIRY_DATE"] = pd.to_datetime(self.df["SEM_EXPIRY_DATE"], errors="coerce")

        # Strike numeric
        self.df["SEM_STRIKE_PRICE"] = pd.to_numeric(self.df["SEM_STRIKE_PRICE"], errors="coerce")

        # Caches
        self._opt_cache = {}
        self._fut_cache = {}
        self._index_cache = {}

    # ---------------------------------------------------
    # Internal: OPTIDX filtered dataframe for index
    # ---------------------------------------------------
    def _get_optidx_df(self, index: str):
        idx = str(index).upper().strip()
        if idx in self._opt_cache:
            return self._opt_cache[idx]

        df = self.df
        opts = df[
            (df["SEM_EXM_EXCH_ID"] == "NSE")
            & (df["SEM_SEGMENT"] == "D")
            & (df["SEM_INSTRUMENT_NAME"].astype(str).str.upper() == "OPTIDX")
            & (df["SEM_TRADING_SYMBOL"].astype(str).str.upper().str.startswith(idx, na=False))
        ].copy()

        opts = opts.dropna(subset=[
            "SEM_EXPIRY_DATE",
            "SEM_STRIKE_PRICE",
            "SEM_SMST_SECURITY_ID",
            "SEM_OPTION_TYPE"
        ])

        self._opt_cache[idx] = opts
        return opts

    # ---------------------------------------------------
    # Internal: FUTIDX filtered dataframe for index
    # ---------------------------------------------------
    def _get_futidx_df(self, index: str):
        idx = str(index).upper().strip()
        if idx in self._fut_cache:
            return self._fut_cache[idx]

        df = self.df
        fut = df[
            (df["SEM_EXM_EXCH_ID"] == "NSE")
            & (df["SEM_SEGMENT"] == "D")
            & (df["SEM_INSTRUMENT_NAME"].astype(str).str.upper() == "FUTIDX")
            & (df["SEM_TRADING_SYMBOL"].astype(str).str.upper().str.startswith(idx, na=False))
        ].copy()

        fut = fut.dropna(subset=["SEM_EXPIRY_DATE", "SEM_SMST_SECURITY_ID"])
        self._fut_cache[idx] = fut
        return fut

    # ---------------------------------------------------
    # ✅ FUT: Nearest active Index Future
    # ---------------------------------------------------
    def get_nearest_future(self, index_name: str):
        idx = str(index_name).upper().strip()
        fut = self._get_futidx_df(idx)

        now = pd.Timestamp.now().normalize()
        fut2 = fut[fut["SEM_EXPIRY_DATE"] >= now].copy()

        if fut2.empty:
            raise Exception(f"❌ No ACTIVE FUT found for {idx}")

        row = fut2.sort_values("SEM_EXPIRY_DATE").iloc[0]

        return {
            "security_id": str(row["SEM_SMST_SECURITY_ID"]),
            "symbol": str(row["SEM_TRADING_SYMBOL"]),
            "expiry": row["SEM_EXPIRY_DATE"],
            "lot_size": int(float(row["SEM_LOT_UNITS"])) if "SEM_LOT_UNITS" in row else 0,
        }

    def get_current_fut_security_id(self, index_name: str) -> str:
        fut = self.get_nearest_future(index_name)
        return str(fut["security_id"])

    def get_fut_exchange_segment(self) -> str:
        return "NSE_FNO"

    # ---------------------------------------------------
    # ✅ Expiry mode helper
    # ---------------------------------------------------
    def get_expiry_mode(self, index: str) -> str:
        idx = str(index).upper().strip()
        return self.EXPIRY_MODE_BY_INDEX.get(idx, "AUTO")

    # ---------------------------------------------------
    # ✅ Core: Get expiry list (future)
    # ---------------------------------------------------
    def _get_future_expiries(self, index: str):
        opts = self._get_optidx_df(index)
        now = pd.Timestamp.now().normalize()
        exps = opts[opts["SEM_EXPIRY_DATE"] >= now]["SEM_EXPIRY_DATE"].dropna().unique()
        exps = sorted(pd.to_datetime(exps))
        return exps

    # ---------------------------------------------------
    # ✅ Classify: monthly expiry = last expiry in that month
    # ---------------------------------------------------
    def _monthly_expiry_for_month(self, expiries, year: int, month: int):
        same_month = [e for e in expiries if (e.year == year and e.month == month)]
        if not same_month:
            return None
        return max(same_month)

    # ---------------------------------------------------
    # ✅ Get option expiry (WEEKLY/MONTHLY aware)
    # ---------------------------------------------------
    def get_option_expiry(self, index: str, expiry_mode: str = None):
        idx = str(index).upper().strip()
        mode = (expiry_mode or self.get_expiry_mode(idx)).upper().strip()

        expiries = self._get_future_expiries(idx)
        if not expiries:
            raise Exception(f"❌ No OPTIDX expiry >= now for {idx}")

        # AUTO fallback: nearest expiry
        if mode == "AUTO":
            return expiries[0]

        # MONTHLY: choose last expiry in current month if still valid else next month last expiry
        if mode == "MONTHLY":
            now = pd.Timestamp.now().normalize()
            cur_month_monthly = self._monthly_expiry_for_month(expiries, now.year, now.month)
            if cur_month_monthly and cur_month_monthly >= now:
                return cur_month_monthly

            # else choose next available month's monthly
            for e in expiries:
                m = self._monthly_expiry_for_month(expiries, e.year, e.month)
                if m and m >= now:
                    return m

            return expiries[-1]  # last resort

        # WEEKLY (NIFTY): choose nearest expiry that is NOT the monthly expiry of that month.
        # If only monthly is left (monthly week), then fallback to monthly.
        if mode == "WEEKLY":
            now = pd.Timestamp.now().normalize()
            # determine this month's monthly expiry
            monthly_this_month = self._monthly_expiry_for_month(expiries, now.year, now.month)

            # candidates = nearest expiry excluding monthly expiry of that month
            weekly_candidates = []
            for e in expiries:
                # exclude monthly expiry for that month
                monthly_for_e_month = self._monthly_expiry_for_month(expiries, e.year, e.month)
                if monthly_for_e_month and e == monthly_for_e_month:
                    continue
                weekly_candidates.append(e)

            if weekly_candidates:
                return weekly_candidates[0]

            # if no weekly candidate (rare), fallback to nearest expiry
            return expiries[0]

        # Unknown mode → nearest
        return expiries[0]

    # ---------------------------------------------------
    # Backward compatibility: old method name
    # ---------------------------------------------------
    def get_nearest_option_expiry(self, index: str):
        # IMPORTANT: now respects index rule (NIFTY weekly, others monthly)
        return self.get_option_expiry(index)

    # ---------------------------------------------------
    # Find option SecurityId (strike tolerant)
    # ---------------------------------------------------
    def find_option_security_id(self, index: str, expiry_dt, strike, opt_type: str):
        idx = str(index).upper().strip()
        ot = str(opt_type).upper().strip()

        if ot not in ("CE", "PE"):
            raise ValueError("opt_type must be 'CE' or 'PE'")

        exp = pd.to_datetime(expiry_dt, errors="coerce")
        if pd.isna(exp):
            raise Exception(f"❌ Invalid expiry: {expiry_dt}")

        strike_f = float(strike)
        exp_date = exp.date()

        opts = self._get_optidx_df(idx)
        df = opts[
            (opts["SEM_EXPIRY_DATE"].dt.date == exp_date)
            & (opts["SEM_OPTION_TYPE"].astype(str).str.upper() == ot)
        ].copy()

        if df.empty:
            raise Exception(f"❌ No option rows for {idx} {exp_date} {ot}")

        df["__strike_diff"] = (df["SEM_STRIKE_PRICE"] - strike_f).abs()
        best = df.sort_values("__strike_diff").iloc[0]

        tol = float(self.STRIKE_TOL_BY_INDEX.get(idx, 0.5))
        if float(best["__strike_diff"]) > tol:
            raise Exception(
                f"❌ No strike within tolerance | idx={idx} exp={exp_date} ot={ot} "
                f"wanted={strike_f} best={float(best['SEM_STRIKE_PRICE'])} diff={float(best['__strike_diff'])}"
            )

        return int(float(best["SEM_SMST_SECURITY_ID"]))

    # ---------------------------------------------------
    # ✅ INDEX (SPOT) security id for OptionChain REST
    # ---------------------------------------------------
    def get_index_security_id(self, index_name: str) -> int:
        idx = str(index_name).upper().strip()

        if idx in self._index_cache:
            return self._index_cache[idx]

        df = self.df[
            (self.df["SEM_EXM_EXCH_ID"] == "NSE")
            & (self.df["SEM_SEGMENT"] == "I")
            & (self.df["SEM_INSTRUMENT_NAME"].astype(str).str.upper() == "INDEX")
            & (self.df["SEM_TRADING_SYMBOL"].astype(str).str.upper() == idx)
        ]

        if df.empty:
            raise Exception(f"❌ No INDEX security id found for {idx}")

        secid = int(df.iloc[0]["SEM_SMST_SECURITY_ID"])
        self._index_cache[idx] = secid
        return secid