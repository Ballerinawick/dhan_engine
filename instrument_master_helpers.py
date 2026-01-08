# instrument_master_helpers.py
from datetime import datetime

def _norm_expiry(expiry: str) -> str:
    # your CSV shows "06-01-2026 14:30"
    # we convert "2026-01-06" -> "06-01-2026"
    dt = datetime.strptime(expiry, "%Y-%m-%d")
    return dt.strftime("%d-%m-%Y")

def find_option_security_id(self, index, expiry_dt, strike, opt_type):
    """
    expiry_dt: datetime object (from CSV or parsed)
    opt_type: "CE" or "PE"
    """

    expiry_day = expiry_dt.strftime("%d")
    expiry_mon = expiry_dt.strftime("%b").upper()   # JAN
    expiry_year = expiry_dt.strftime("%Y")

    target_custom = f"{index} {expiry_day} {expiry_mon} {strike} {'CALL' if opt_type=='CE' else 'PUT'}"

    for _, row in self.df.iterrows():
        if (
            row["SEM_CUSTOM_SYMBOL"] == target_custom
            and row["SEM_OPTION_TYPE"] == opt_type
        ):
            return int(row["SEM_SMST_SECURITY"])

    raise KeyError(f"No SecurityId found for {target_custom}")
