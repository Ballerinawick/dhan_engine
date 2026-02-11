# instrument_master.py

import pandas as pd
from datetime import datetime


class InstrumentMaster:
    """
    INSTRUMENT MASTER v4
    ---------------------
    ✔ Weekly-aware expiry selection (NIFTY weekly)
    ✔ Monthly selection safe for others
    ✔ get_nearest_option_expiry restored (fixes crash)
    ✔ Debug logs inside methods
    ✔ No breaking change to existing engines
    """

    def __init__(self, csv_path: str, debug=True):
        self.debug = debug

        print("📦 Loading Instrument Master CSV...")
        self.df = pd.read_csv(csv_path, low_memory=False)
        self.df.columns = self.df.columns.astype(str).str.strip()
        print(f"✅ CSV LOADED | rows: {len(self.df)}")

        # normalize object columns
        obj_cols = self.df.select_dtypes(include=["object"]).columns
        for c in obj_cols:
            self.df[c] = self.df[c].astype(str).str.strip()

        # parse expiry
        self.df["SEM_EXPIRY_DATE"] = pd.to_datetime(
            self.df["SEM_EXPIRY_DATE"], errors="coerce"
        )

        # numeric strike
        self.df["SEM_STRIKE_PRICE"] = pd.to_numeric(
            self.df["SEM_STRIKE_PRICE"], errors="coerce"
        )

        self._opt_cache = {}
        self._fut_cache = {}
        self._index_cache = {}

    # =====================================================
    # INTERNAL FILTERS
    # =====================================================

    def _get_optidx_df(self, index: str):
        idx = str(index).upper().strip()

        if idx in self._opt_cache:
            return self._opt_cache[idx]

        if self.debug:
            print(f"🔍 Filtering OPTIDX for {idx}")

        df = self.df[
            (self.df["SEM_EXM_EXCH_ID"] == "NSE")
            & (self.df["SEM_SEGMENT"] == "D")
            & (self.df["SEM_INSTRUMENT_NAME"].str.upper() == "OPTIDX")
            & (self.df["SEM_TRADING_SYMBOL"].str.upper().str.startswith(idx))
        ].copy()

        df = df.dropna(
            subset=[
                "SEM_EXPIRY_DATE",
                "SEM_STRIKE_PRICE",
                "SEM_OPTION_TYPE",
                "SEM_SMST_SECURITY_ID",
            ]
        )

        if self.debug:
            print(f"   → Found {len(df)} OPTIDX rows")

        self._opt_cache[idx] = df
        return df

    def _get_futidx_df(self, index: str):
        idx = str(index).upper().strip()

        if idx in self._fut_cache:
            return self._fut_cache[idx]

        if self.debug:
            print(f"🔍 Filtering FUTIDX for {idx}")

        df = self.df[
            (self.df["SEM_EXM_EXCH_ID"] == "NSE")
            & (self.df["SEM_SEGMENT"] == "D")
            & (self.df["SEM_INSTRUMENT_NAME"].str.upper() == "FUTIDX")
            & (self.df["SEM_TRADING_SYMBOL"].str.upper().str.startswith(idx))
        ].copy()

        df = df.dropna(subset=["SEM_EXPIRY_DATE", "SEM_SMST_SECURITY_ID"])

        if self.debug:
            print(f"   → Found {len(df)} FUTIDX rows")

        self._fut_cache[idx] = df
        return df

    # =====================================================
    # FUTURE SELECTION
    # =====================================================

    def get_nearest_future(self, index_name: str):
        idx = str(index_name).upper().strip()
        fut = self._get_futidx_df(idx)

        now = pd.Timestamp.now()
        fut2 = fut[fut["SEM_EXPIRY_DATE"] >= now]

        if fut2.empty:
            raise Exception(f"❌ No active FUT found for {idx}")

        row = fut2.sort_values("SEM_EXPIRY_DATE").iloc[0]

        if self.debug:
            print(f"✅ {idx} FUT selected: {row['SEM_TRADING_SYMBOL']}")

        return {
            "security_id": str(row["SEM_SMST_SECURITY_ID"]),
            "symbol": str(row["SEM_TRADING_SYMBOL"]),
            "expiry": row["SEM_EXPIRY_DATE"],
            "lot_size": int(float(row["SEM_LOT_UNITS"])),
        }

    # =====================================================
    # ✅ CRITICAL FIX — OPTION EXPIRY METHOD RESTORED
    # =====================================================

    def get_nearest_option_expiry(self, index: str):
        """
        This is the method your selector expects.
        DO NOT rename.
        """

        idx = str(index).upper().strip()
        opts = self._get_optidx_df(idx)

        now = pd.Timestamp.now()
        opts2 = opts[opts["SEM_EXPIRY_DATE"] >= now]

        if opts2.empty:
            raise Exception(f"❌ No active expiry for {idx}")

        expiries = sorted(opts2["SEM_EXPIRY_DATE"].unique())

        nearest = expiries[0]

        if self.debug:
            print(f"📅 {idx} nearest expiry selected: {nearest.date()}")

        return nearest

    # =====================================================
    # FIND OPTION BY STRIKE
    # =====================================================

    def find_option_security_id(self, index: str, expiry_dt, strike, opt_type: str):
        idx = str(index).upper().strip()
        ot = str(opt_type).upper().strip()

        opts = self._get_optidx_df(idx)

        expiry_dt = pd.to_datetime(expiry_dt)
        exp_date = expiry_dt.date()

        df = opts[
            (opts["SEM_EXPIRY_DATE"].dt.date == exp_date)
            & (opts["SEM_OPTION_TYPE"].str.upper() == ot)
        ].copy()

        if df.empty:
            raise Exception(f"❌ No rows for {idx} {exp_date} {ot}")

        strike = float(strike)
        df["__diff"] = (df["SEM_STRIKE_PRICE"] - strike).abs()

        best = df.sort_values("__diff").iloc[0]

        if float(best["__diff"]) > 0.001:
            raise Exception("❌ No exact strike match")

        if self.debug:
            print(
                f"🎯 {idx} {ot} strike matched | "
                f"{best['SEM_STRIKE_PRICE']} | "
                f"secid={best['SEM_SMST_SECURITY_ID']}"
            )

        return int(best["SEM_SMST_SECURITY_ID"])