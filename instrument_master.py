# instrument_master.py
import pandas as pd


class InstrumentMaster:
    """
    Reads Dhan api-scrip-master.csv and extracts:
    - Nearest FUT for index (NIFTY / BANKNIFTY / FINNIFTY)
    - Nearest OPTIDX expiry
    - Find option SecurityId by (index, expiry, strike, CE/PE)
    - ITM CE & ITM PE for a given expiry+fut_ltp
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

        # Small cache for speed
        self._opt_cache = {}  # key: (index_upper) -> filtered df (OPTIDX for that index)

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

        # drop bad rows
        opts = opts.dropna(subset=["SEM_EXPIRY_DATE", "SEM_STRIKE_PRICE", "SEM_SMST_SECURITY_ID", "SEM_OPTION_TYPE"])

        self._opt_cache[idx] = opts
        return opts

    # ---------------------------------------------------
    # FUT: Nearest Index Future
    # ---------------------------------------------------
    def get_nearest_future(self, index_name: str):
        df = self.df
        idx = str(index_name).upper().strip()

        fut = df[
            (df["SEM_EXM_EXCH_ID"] == "NSE")
            & (df["SEM_SEGMENT"] == "D")
            & (df["SEM_INSTRUMENT_NAME"].astype(str).str.upper() == "FUTIDX")
            & (df["SEM_TRADING_SYMBOL"].astype(str).str.upper().str.startswith(idx, na=False))
        ].copy()

        fut = fut.dropna(subset=["SEM_EXPIRY_DATE"])
        fut = fut[fut["SEM_EXPIRY_DATE"] >= pd.Timestamp.now()]

        if fut.empty:
            raise Exception(f"❌ No FUT found for {idx}")

        row = fut.sort_values("SEM_EXPIRY_DATE").iloc[0]

        return {
            "security_id": str(row["SEM_SMST_SECURITY_ID"]),
            "symbol": row["SEM_TRADING_SYMBOL"],
            "expiry": row["SEM_EXPIRY_DATE"],
            "lot_size": int(float(row["SEM_LOT_UNITS"])),
        }

    # ---------------------------------------------------
    # ✅ NEW: Nearest option expiry for index (OPTIDX)
    # Returns pandas Timestamp (datetime)
    # ---------------------------------------------------
    def get_nearest_option_expiry(self, index: str):
        opts = self._get_optidx_df(index)

        now = pd.Timestamp.now()
        opts2 = opts[opts["SEM_EXPIRY_DATE"] >= now]

        if opts2.empty:
            # If market closed / system time mismatch, fallback to max available future-ish expiry
            raise Exception(f"❌ No OPTIDX expiry >= now for {index}. Check system time or CSV expiries.")

        expiry = opts2["SEM_EXPIRY_DATE"].sort_values().iloc[0]
        return expiry  # Timestamp

    # ---------------------------------------------------
    # ✅ NEW: Find SecurityId for exact option (index, expiry_dt, strike, CE/PE)
    # expiry_dt can be: Timestamp / datetime / date / string ("2026-01-06" or same)
    # ---------------------------------------------------
    def find_option_security_id(self, index: str, expiry_dt, strike, opt_type: str):
        idx = str(index).upper().strip()
        ot = str(opt_type).upper().strip()

        if ot not in ("CE", "PE"):
            raise ValueError("opt_type must be 'CE' or 'PE'")

        # normalize expiry date to date()
        exp = pd.to_datetime(expiry_dt, errors="coerce")
        if pd.isna(exp):
            raise Exception(f"❌ Invalid expiry provided: {expiry_dt}")
        exp_date = exp.date()

        # normalize strike to float
        try:
            strike_f = float(strike)
        except Exception:
            raise Exception(f"❌ Invalid strike: {strike}")

        opts = self._get_optidx_df(idx)

        # filter
        df = opts[
            (opts["SEM_EXPIRY_DATE"].dt.date == exp_date)
            & (opts["SEM_OPTION_TYPE"].astype(str).str.upper() == ot)
        ].copy()

        if df.empty:
            raise Exception(f"❌ No OPTIDX rows for {idx} expiry={exp_date} type={ot}")

        # strike match (float safe)
        # Some CSVs store strike as 26100.0 etc
        df["__strike_diff"] = (df["SEM_STRIKE_PRICE"].astype(float) - strike_f).abs()
        df = df.sort_values("__strike_diff")

        best = df.iloc[0]
        if float(best["__strike_diff"]) > 0.001:
            raise Exception(f"❌ No SecurityId found for {idx} {exp_date} {strike_f} {ot}")

        return int(float(best["SEM_SMST_SECURITY_ID"]))

    # ---------------------------------------------------
    # ITM CE & PE lookup (your existing function kept)
    # expiry_ymd -> '2026-01-06'
    # ---------------------------------------------------
    def find_itm_options(self, index: str, expiry_ymd: str, fut_ltp: float, step: int):
        exp_date = pd.to_datetime(expiry_ymd).date()
        atm = int(round(float(fut_ltp) / step) * step)

        opts = self._get_optidx_df(index)

        df = opts[opts["SEM_EXPIRY_DATE"].dt.date == exp_date].copy()
        if df.empty:
            raise Exception(f"❌ No OPTIDX rows for {index} {expiry_ymd}")

        strikes = sorted(df["SEM_STRIKE_PRICE"].dropna().unique())

        ce_strike = max([s for s in strikes if s < atm], default=None)
        pe_strike = min([s for s in strikes if s > atm], default=None)

        if ce_strike is None or pe_strike is None:
            raise Exception(f"❌ No ITM strikes | ATM={atm}")

        ce = df[(df["SEM_OPTION_TYPE"].astype(str).str.upper() == "CE") & (df["SEM_STRIKE_PRICE"] == ce_strike)]
        pe = df[(df["SEM_OPTION_TYPE"].astype(str).str.upper() == "PE") & (df["SEM_STRIKE_PRICE"] == pe_strike)]

        if ce.empty or pe.empty:
            raise Exception("❌ ITM CE/PE rows missing")

        ce_row = ce.iloc[0]
        pe_row = pe.iloc[0]

        return {        
            "atm": atm,
            "ce": {
                "security_id": str(ce_row["SEM_SMST_SECURITY_ID"]),
                "symbol": ce_row["SEM_TRADING_SYMBOL"],
                "strike": float(ce_strike),
                "lot_size": int(float(ce_row["SEM_LOT_UNITS"])),
            },
            "pe": {
                "security_id": str(pe_row["SEM_SMST_SECURITY_ID"]),
                "symbol": pe_row["SEM_TRADING_SYMBOL"],
                "strike": float(pe_strike),
                "lot_size": int(float(pe_row["SEM_LOT_UNITS"])),
            },
        }
