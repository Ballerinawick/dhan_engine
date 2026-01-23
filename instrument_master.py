# instrument_master.py
import pandas as pd


class InstrumentMaster:
    """
    Reads Dhan api-scrip-master.csv and extracts:
    - Nearest FUTIDX for index (NIFTY / BANKNIFTY / FINNIFTY)
    - Nearest OPTIDX expiry
    - Find option SecurityId by (index, expiry, strike, CE/PE)
    - ✅ INDEX (spot) SecurityId for OptionChain REST
    """

    def __init__(self, csv_path: str):
        self.df = pd.read_csv(csv_path, low_memory=False)
        self.df.columns = self.df.columns.astype(str).str.strip()
        print("✅ CSV LOADED | rows:", len(self.df))

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
        self._index_cache = {}   # ✅ NEW (safe cache)

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

        now = pd.Timestamp.now()
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

    # ---------------------------------------------------
    # Helper: FUT security id
    # ---------------------------------------------------
    def get_current_fut_security_id(self, index_name: str) -> str:
        fut = self.get_nearest_future(index_name)
        return str(fut["security_id"])

    # ---------------------------------------------------
    # FUT exchange segment
    # ---------------------------------------------------
    def get_fut_exchange_segment(self) -> str:
        return "NSE_FNO"

    # ---------------------------------------------------
    # Nearest option expiry
    # ---------------------------------------------------
    def get_nearest_option_expiry(self, index: str):
        opts = self._get_optidx_df(index)

        now = pd.Timestamp.now()
        opts2 = opts[opts["SEM_EXPIRY_DATE"] >= now]

        if opts2.empty:
            raise Exception(f"❌ No OPTIDX expiry >= now for {index}")

        return opts2["SEM_EXPIRY_DATE"].sort_values().iloc[0]

    # ---------------------------------------------------
    # Find option SecurityId
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
            raise Exception(f"❌ No exact strike match")

        return int(float(best["SEM_SMST_SECURITY_ID"]))

    # ---------------------------------------------------
    # ✅ NEW: INDEX (SPOT) security id for OptionChain REST
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