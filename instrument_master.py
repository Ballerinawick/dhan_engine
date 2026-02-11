import pandas as pd
from datetime import datetime


class InstrumentMaster:
    """
    Instrument Master v3.2
    -----------------------
    ✔ NIFTY → Weekly (Tuesday)
    ✔ BANKNIFTY → Monthly (Last Tuesday)
    ✔ FINNIFTY → Monthly (Last Tuesday)
    ✔ Debug logs inside class
    """

    def __init__(self, csv_path: str):
        print("📂 Loading Instrument Master CSV...")

        self.df = pd.read_csv(csv_path, low_memory=False)
        self.df.columns = self.df.columns.astype(str).str.strip()

        print(f"✅ CSV LOADED | rows: {len(self.df)}")

        # Normalize object columns
        obj_cols = self.df.select_dtypes(include=["object"]).columns
        for c in obj_cols:
            self.df[c] = self.df[c].astype(str).str.strip()

        # Required columns
        required = [
            "SEM_EXM_EXCH_ID",
            "SEM_SEGMENT",
            "SEM_SMST_SECURITY_ID",
            "SEM_INSTRUMENT_NAME",
            "SEM_TRADING_SYMBOL",
            "SEM_OPTION_TYPE",
            "SEM_STRIKE_PRICE",
            "SEM_LOT_UNITS",
            "SEM_EXPIRY_DATE",
        ]

        for r in required:
            if r not in self.df.columns:
                raise KeyError(f"❌ Missing required column: {r}")

        # Convert expiry to datetime
        self.df["SEM_EXPIRY_DATE"] = pd.to_datetime(
            self.df["SEM_EXPIRY_DATE"], errors="coerce"
        )

        # Strike numeric
        self.df["SEM_STRIKE_PRICE"] = pd.to_numeric(
            self.df["SEM_STRIKE_PRICE"], errors="coerce"
        )

        # Caches
        self._opt_cache = {}
        self._fut_cache = {}
        self._index_cache = {}

    # ==========================================================
    # INTERNAL: OPTIDX DATAFRAME
    # ==========================================================
    def _get_optidx_df(self, index: str):
        idx = index.upper().strip()

        if idx in self._opt_cache:
            return self._opt_cache[idx]

        print(f"🔎 Filtering OPTIDX for {idx}")

        opts = self.df[
            (self.df["SEM_EXM_EXCH_ID"] == "NSE")
            & (self.df["SEM_SEGMENT"] == "D")
            & (self.df["SEM_INSTRUMENT_NAME"].str.upper() == "OPTIDX")
            & (self.df["SEM_TRADING_SYMBOL"].str.upper().str.startswith(idx))
        ].copy()

        opts = opts.dropna(subset=[
            "SEM_EXPIRY_DATE",
            "SEM_STRIKE_PRICE",
            "SEM_SMST_SECURITY_ID",
            "SEM_OPTION_TYPE"
        ])

        print(f"   → Found {len(opts)} OPTIDX rows")

        self._opt_cache[idx] = opts
        return opts

    # ==========================================================
    # EXPIRY LOGIC (SEBI 2025 COMPLIANT)
    # ==========================================================
    def get_option_expiry(self, index: str):
        idx = index.upper().strip()
        opts = self._get_optidx_df(idx)

        now = pd.Timestamp.now().normalize()
        opts = opts[opts["SEM_EXPIRY_DATE"] >= now]

        if opts.empty:
            raise Exception(f"❌ No valid future expiries for {idx}")

        expiries = sorted(opts["SEM_EXPIRY_DATE"].dt.normalize().unique())

        print(f"\n📅 Available expiries for {idx}:")
        for e in expiries[:6]:
            print("   ", e.date())

        # ------------------------------
        # NIFTY → WEEKLY (Tuesday)
        # ------------------------------
        if idx == "NIFTY":
            for exp in expiries:
                if exp.weekday() == 1:  # Tuesday
                    print(f"✅ {idx} WEEKLY expiry selected: {exp.date()}")
                    return exp

            raise Exception("❌ No Tuesday weekly expiry found for NIFTY")

        # ------------------------------
        # BANKNIFTY / FINNIFTY → MONTHLY (Last Tuesday)
        # ------------------------------
        monthly = []

        for exp in expiries:
            if exp.weekday() == 1:  # Tuesday
                next_week = exp + pd.Timedelta(days=7)
                if next_week.month != exp.month:
                    monthly.append(exp)

        if monthly:
            selected = sorted(monthly)[0]
            print(f"✅ {idx} MONTHLY expiry selected: {selected.date()}")
            return selected

        raise Exception(f"❌ No monthly expiry found for {idx}")

    # ==========================================================
    # FUTURE LOGIC
    # ==========================================================
    def _get_futidx_df(self, index: str):
        idx = index.upper().strip()

        if idx in self._fut_cache:
            return self._fut_cache[idx]

        print(f"🔎 Filtering FUTIDX for {idx}")

        fut = self.df[
            (self.df["SEM_EXM_EXCH_ID"] == "NSE")
            & (self.df["SEM_SEGMENT"] == "D")
            & (self.df["SEM_INSTRUMENT_NAME"].str.upper() == "FUTIDX")
            & (self.df["SEM_TRADING_SYMBOL"].str.upper().str.startswith(idx))
        ].copy()

        fut = fut.dropna(subset=["SEM_EXPIRY_DATE", "SEM_SMST_SECURITY_ID"])

        print(f"   → Found {len(fut)} FUTIDX rows")

        self._fut_cache[idx] = fut
        return fut

    def get_nearest_future(self, index_name: str):
        idx = index_name.upper().strip()
        fut = self._get_futidx_df(idx)

        now = pd.Timestamp.now()
        fut2 = fut[fut["SEM_EXPIRY_DATE"] >= now]

        if fut2.empty:
            raise Exception(f"❌ No ACTIVE FUT found for {idx}")

        row = fut2.sort_values("SEM_EXPIRY_DATE").iloc[0]

        print(f"✅ {idx} FUT selected: {row['SEM_TRADING_SYMBOL']}")

        return {
            "security_id": str(row["SEM_SMST_SECURITY_ID"]),
            "symbol": str(row["SEM_TRADING_SYMBOL"]),
            "expiry": row["SEM_EXPIRY_DATE"],
            "lot_size": int(float(row["SEM_LOT_UNITS"])) if "SEM_LOT_UNITS" in row else 0,
        }

    # ==========================================================
    # OPTION SECURITY ID
    # ==========================================================
    def find_option_security_id(self, index: str, expiry_dt, strike, opt_type: str):
        idx = index.upper().strip()
        opt_type = opt_type.upper().strip()

        exp = pd.to_datetime(expiry_dt)
        exp_date = exp.date()
        strike_f = float(strike)

        opts = self._get_optidx_df(idx)

        df = opts[
            (opts["SEM_EXPIRY_DATE"].dt.date == exp_date)
            & (opts["SEM_OPTION_TYPE"].str.upper() == opt_type)
        ].copy()

        if df.empty:
            raise Exception(f"❌ No rows for {idx} {exp_date} {opt_type}")

        df["diff"] = (df["SEM_STRIKE_PRICE"] - strike_f).abs()
        best = df.sort_values("diff").iloc[0]

        if float(best["diff"]) > 0.001:
            raise Exception("❌ No exact strike match")

        print(f"🎯 {idx} {opt_type} {strike_f} → SecurityId {best['SEM_SMST_SECURITY_ID']}")

        return int(float(best["SEM_SMST_SECURITY_ID"]))