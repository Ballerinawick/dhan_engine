# instrument_master.py
import pandas as pd


class InstrumentMaster:
    """
    InstrumentMaster v3.1 (SAFE + DEBUG + EXPIRY FIX)

    ✅ Keeps existing public methods (so other modules won't break)
    ✅ Adds missing methods your logs show:
       - get_nearest_option_expiry()
       - get_index_security_id()
    ✅ Expiry selection updated for NSE circular change (post Aug-2025):
       - NIFTY weekly expiry = Tuesday (prefer weekly, avoid monthly last-Tuesday when asked)
       - BANKNIFTY / FINNIFTY = monthly only (prefer last Tuesday of month)
    ✅ Debug logs are inside this class only (not in main)
    """

    def __init__(self, csv_path: str, debug: bool = True):
        self.debug = bool(debug)

        self._log(f"📦 Loading Instrument Master CSV: {csv_path}")
        self.df = pd.read_csv(csv_path, low_memory=False)
        self.df.columns = self.df.columns.astype(str).str.strip()
        self._log(f"✅ CSV LOADED | rows: {len(self.df)}")

        # Normalize string columns
        obj_cols = self.df.select_dtypes(include=["object"]).columns
        for c in obj_cols:
            self.df[c] = self.df[c].astype(str).str.strip()

        # Required columns (CSV VERIFIED)
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

        # Caches for speed
        self._opt_cache = {}
        self._fut_cache = {}
        self._index_cache = {}

    # ---------------------------------------------------
    # Logging
    # ---------------------------------------------------
    def _log(self, msg: str):
        if self.debug:
            print(msg)

    # ---------------------------------------------------
    # Date helpers (Expiry rules)
    # ---------------------------------------------------
    @staticmethod
    def _is_last_tuesday(dt: pd.Timestamp) -> bool:
        if pd.isna(dt):
            return False
        d = dt.date()
        # Tuesday = 1 (Mon=0)
        if d.weekday() != 1:
            return False
        # if adding 7 days changes month => this Tuesday is last Tuesday
        return (d.day + 7) > pd.Timestamp(d).days_in_month

    @staticmethod
    def _is_tuesday(dt: pd.Timestamp) -> bool:
        if pd.isna(dt):
            return False
        return dt.date().weekday() == 1  # Tuesday

    # ---------------------------------------------------
    # Internal: OPTIDX filtered dataframe for index
    # ---------------------------------------------------
    def _get_optidx_df(self, index: str):
        idx = str(index).upper().strip()
        if idx in self._opt_cache:
            return self._opt_cache[idx]

        self._log(f"🔎 Filtering OPTIDX for {idx}")

        df = self.df
        opts = df[
            (df["SEM_EXM_EXCH_ID"] == "NSE")
            & (df["SEM_SEGMENT"] == "D")
            & (df["SEM_INSTRUMENT_NAME"].astype(str).str.upper() == "OPTIDX")
            & (df["SEM_TRADING_SYMBOL"].astype(str).str.upper().str.startswith(idx, na=False))
        ].copy()

        opts = opts.dropna(
            subset=["SEM_EXPIRY_DATE", "SEM_STRIKE_PRICE", "SEM_SMST_SECURITY_ID", "SEM_OPTION_TYPE"]
        )

        self._log(f"   ➜ Found {len(opts)} OPTIDX rows")
        self._opt_cache[idx] = opts
        return opts

    # ---------------------------------------------------
    # Internal: FUTIDX filtered dataframe for index
    # ---------------------------------------------------
    def _get_futidx_df(self, index: str):
        idx = str(index).upper().strip()
        if idx in self._fut_cache:
            return self._fut_cache[idx]

        self._log(f"🔎 Filtering FUTIDX for {idx}")

        df = self.df
        fut = df[
            (df["SEM_EXM_EXCH_ID"] == "NSE")
            & (df["SEM_SEGMENT"] == "D")
            & (df["SEM_INSTRUMENT_NAME"].astype(str).str.upper() == "FUTIDX")
            & (df["SEM_TRADING_SYMBOL"].astype(str).str.upper().str.startswith(idx, na=False))
        ].copy()

        fut = fut.dropna(subset=["SEM_EXPIRY_DATE", "SEM_SMST_SECURITY_ID"])

        self._log(f"   ➜ Found {len(fut)} FUTIDX rows")
        self._fut_cache[idx] = fut
        return fut

    # ---------------------------------------------------
    # ✅ FUT: Nearest active Index Future
    # ---------------------------------------------------
    def get_nearest_future(self, index_name: str):
        idx = str(index_name).upper().strip()
        fut = self._get_futidx_df(idx)

        now = pd.Timestamp.now()
        fut2 = fut[fut["SEM_EXPIRY_DATE"] >= now].copy()

        if fut2.empty:
            raise Exception(f"❌ No ACTIVE FUT found for {idx}")

        row = fut2.sort_values("SEM_EXPIRY_DATE").iloc[0]
        symbol = str(row["SEM_TRADING_SYMBOL"])

        self._log(f"✅ {idx} FUT selected: {symbol}")

        return {
            "security_id": str(row["SEM_SMST_SECURITY_ID"]),
            "symbol": symbol,
            "expiry": row["SEM_EXPIRY_DATE"],
            "lot_size": int(float(row["SEM_LOT_UNITS"])) if "SEM_LOT_UNITS" in row else 0,
        }

    # Backward-compatible helper
    def get_current_fut_security_id(self, index_name: str) -> str:
        return str(self.get_nearest_future(index_name)["security_id"])

    def get_fut_exchange_segment(self) -> str:
        return "NSE_FNO"

    # ---------------------------------------------------
    # ✅ Nearest option expiry (UPDATED RULES)
    # ---------------------------------------------------
    def get_nearest_option_expiry(self, index: str, prefer_weekly: bool = True):
        """
        Rules (post Aug-2025):
        - NIFTY: weekly expiry Tuesday; monthly also Tuesday (last Tuesday).
          If prefer_weekly=True -> pick nearest Tuesday that is NOT last Tuesday (weekly).
        - BANKNIFTY/FINNIFTY: monthly only -> pick nearest last Tuesday if available, else fallback to nearest expiry.
        """
        idx = str(index).upper().strip()
        opts = self._get_optidx_df(idx)

        now = pd.Timestamp.now()
        opts2 = opts[opts["SEM_EXPIRY_DATE"] >= now].copy()

        if opts2.empty:
            raise Exception(f"❌ No OPTIDX expiry >= now for {idx}")

        expiries = opts2["SEM_EXPIRY_DATE"].dropna().drop_duplicates().sort_values()

        if expiries.empty:
            raise Exception(f"❌ No valid expiries for {idx}")

        # NIFTY -> prefer weekly Tuesday (not last Tuesday)
        if idx == "NIFTY":
            if prefer_weekly:
                weekly = [d for d in expiries.tolist() if self._is_tuesday(d) and (not self._is_last_tuesday(d))]
                if weekly:
                    chosen = weekly[0]
                    self._log(f"📅 {idx} weekly expiry selected: {chosen.date()} (Tue, not last Tue)")
                    return chosen

            # fallback to nearest expiry
            chosen = expiries.iloc[0]
            self._log(f"📅 {idx} nearest expiry selected (fallback): {chosen.date()}")
            return chosen

        # BANKNIFTY / FINNIFTY -> monthly only (prefer last Tuesday)
        monthly = [d for d in expiries.tolist() if self._is_last_tuesday(d)]
        if monthly:
            chosen = monthly[0]
            self._log(f"📅 {idx} monthly expiry selected: {chosen.date()} (last Tue)")
            return chosen

        chosen = expiries.iloc[0]
        self._log(f"📅 {idx} nearest expiry selected (fallback): {chosen.date()}")
        return chosen

    # Backward-compatible aliases (so other files won't break)
    def get_option_expiry(self, index: str):
        return self.get_nearest_option_expiry(index, prefer_weekly=True)

    def get_nearest_opt_expiry(self, index: str):
        return self.get_nearest_option_expiry(index, prefer_weekly=True)

    # ---------------------------------------------------
    # ✅ Find option SecurityId (exact strike required)
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

        if float(best["__strike_diff"]) > 0.001:
            raise Exception(f"❌ No exact strike match for {idx} {exp_date} {strike_f} {ot}")

        secid = int(float(best["SEM_SMST_SECURITY_ID"]))
        self._log(f"🎯 {idx} {ot} strike={strike_f:.2f} exp={exp_date} => secid={secid}")
        return secid

    # ---------------------------------------------------
    # ✅ INDEX (SPOT) security id for OptionChain REST
    # ---------------------------------------------------
    def get_index_security_id(self, index_name: str) -> int:
        """
        Dhan OptionChain REST needs INDEX segment (spot) security id.
        We search robustly because CSV naming can vary (TRADING_SYMBOL vs CUSTOM_SYMBOL).
        """
        idx = str(index_name).upper().strip()

        if idx in self._index_cache:
            return self._index_cache[idx]

        self._log(f"🔎 Finding INDEX security id for {idx}")

        df = self.df[
            (self.df["SEM_EXM_EXCH_ID"] == "NSE")
            & (self.df["SEM_SEGMENT"] == "I")
            & (self.df["SEM_INSTRUMENT_NAME"].astype(str).str.upper() == "INDEX")
        ].copy()

        if df.empty:
            raise Exception("❌ No INDEX rows found in CSV (segment=I, instrument=INDEX)")

        # Try strict match first
        d1 = df[df["SEM_TRADING_SYMBOL"].astype(str).str.upper() == idx]
        if not d1.empty:
            secid = int(float(d1.iloc[0]["SEM_SMST_SECURITY_ID"]))
            self._index_cache[idx] = secid
            self._log(f"✅ INDEX_SID | {idx} => {secid} (TRADING_SYMBOL exact)")
            return secid

        # Try custom symbol contains index name
        d2 = df[df["SEM_CUSTOM_SYMBOL"].astype(str).str.upper().str.contains(idx, na=False)]
        if not d2.empty:
            secid = int(float(d2.iloc[0]["SEM_SMST_SECURITY_ID"]))
            self._index_cache[idx] = secid
            self._log(f"✅ INDEX_SID | {idx} => {secid} (CUSTOM_SYMBOL contains)")
            return secid

        # Last fallback: startswith
        d3 = df[df["SEM_TRADING_SYMBOL"].astype(str).str.upper().str.startswith(idx, na=False)]
        if not d3.empty:
            secid = int(float(d3.iloc[0]["SEM_SMST_SECURITY_ID"]))
            self._index_cache[idx] = secid
            self._log(f"✅ INDEX_SID | {idx} => {secid} (TRADING_SYMBOL startswith)")
            return secid

        raise Exception(f"❌ No INDEX security id found for {idx}")