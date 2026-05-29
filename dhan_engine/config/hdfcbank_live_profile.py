from __future__ import annotations

import os
import time
from requests import HTTPError


def install_hdfcbank_live_profile() -> None:
    """Enable stock-option profiles without changing the existing index path."""
    from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
    from dhan_engine.infrastructure.dhan import option_chain_selector as selector_module
    from dhan_engine.simulations.paper_trade_manager import PaperTradeManager

    if getattr(InstrumentMaster, "_hdfcbank_live_profile_installed", False):
        return

    stock_profiles = {
        "HDFCBANK": {
            "premium_filter": (8, 90),
            "delta_filter": (0.25, 0.65),
            "spread_max_pct": 0.025,
            "lot_size": int(os.getenv("HDFCBANK_LOT_SIZE", "550") or 550),
        },
        "RELIANCE": {
            "premium_filter": (8, 120),
            "delta_filter": (0.25, 0.65),
            "spread_max_pct": 0.025,
            "lot_size": int(os.getenv("RELIANCE_LOT_SIZE", "500") or 500),
        },
    }
    for symbol, profile in stock_profiles.items():
        selector_module.PREMIUM_FILTER.setdefault(symbol, profile["premium_filter"])
        selector_module.DELTA_FILTER.setdefault(symbol, profile["delta_filter"])
        selector_module.SPREAD_MAX_PCT.setdefault(symbol, profile["spread_max_pct"])
        PaperTradeManager.LOT_SIZES.setdefault(symbol, profile["lot_size"])

    original_init = InstrumentMaster.__init__
    original_get_nearest_option_expiry = InstrumentMaster.get_nearest_option_expiry
    original_on_entry = PaperTradeManager.on_entry

    def is_index_symbol(symbol: str) -> bool:
        return str(symbol).upper().strip() in {"NIFTY", "BANKNIFTY", "FINNIFTY"}

    def patched_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self._equity_cache = getattr(self, "_equity_cache", {})

    def get_derivative_df(self, index: str, *, future: bool):
        idx = str(index).upper().strip()
        cache = self._fut_cache if future else self._opt_cache
        if idx in cache:
            return cache[idx]

        if future:
            instrument_name = "FUTIDX" if is_index_symbol(idx) else "FUTSTK"
        else:
            instrument_name = "OPTIDX" if is_index_symbol(idx) else "OPTSTK"
        self._log(f"Filtering {instrument_name} for {idx}")

        df = self.df
        rows = df[
            (df["SEM_EXM_EXCH_ID"] == "NSE")
            & (df["SEM_SEGMENT"] == "D")
            & (df["SEM_INSTRUMENT_NAME"].astype(str).str.upper() == instrument_name)
            & (df["SEM_TRADING_SYMBOL"].astype(str).str.upper().str.startswith(idx, na=False))
        ].copy()
        subset = ["SEM_EXPIRY_DATE", "SEM_SMST_SECURITY_ID"]
        if not future:
            subset += ["SEM_STRIKE_PRICE", "SEM_OPTION_TYPE"]
        rows = rows.dropna(subset=subset)
        self._log(f"   Found {len(rows)} {instrument_name} rows")
        cache[idx] = rows
        return rows

    def get_option_df(self, index: str):
        return get_derivative_df(self, index, future=False)

    def get_future_df(self, index: str):
        return get_derivative_df(self, index, future=True)

    def get_nearest_option_expiry(self, index: str, prefer_weekly: bool = True):
        idx = str(index).upper().strip()
        if is_index_symbol(idx):
            return original_get_nearest_option_expiry(self, idx, prefer_weekly=prefer_weekly)

        import pandas as pd
        opts = self._get_optidx_df(idx)
        opts2 = opts[opts["SEM_EXPIRY_DATE"] >= pd.Timestamp.now()].copy()
        if opts2.empty:
            raise Exception(f"No OPTSTK expiry >= now for {idx}")
        expiries = opts2["SEM_EXPIRY_DATE"].dropna().drop_duplicates().sort_values()
        if expiries.empty:
            raise Exception(f"No valid expiries for {idx}")
        chosen = expiries.iloc[0]
        self._log(f"{idx} stock option expiry selected: {chosen.date()}")
        return chosen

    def get_equity_security_id(self, symbol: str) -> int:
        sym = str(symbol).upper().strip()
        cache = getattr(self, "_equity_cache", {})
        if sym in cache:
            return cache[sym]

        self._log(f"Finding EQUITY security id for {sym}")
        df = self.df[
            (self.df["SEM_EXM_EXCH_ID"] == "NSE")
            & (self.df["SEM_SEGMENT"].astype(str).str.upper().isin({"E", "C"}))
            & (self.df["SEM_TRADING_SYMBOL"].astype(str).str.upper() == sym)
        ].copy()
        if df.empty:
            df = self.df[
                (self.df["SEM_EXM_EXCH_ID"] == "NSE")
                & (self.df["SEM_SEGMENT"].astype(str).str.upper().isin({"E", "C"}))
                & (self.df["SEM_CUSTOM_SYMBOL"].astype(str).str.upper().str.contains(sym, na=False))
            ].copy()
        if df.empty:
            raise Exception(f"No EQUITY security id found for {sym}")

        secid = int(float(df.iloc[0]["SEM_SMST_SECURITY_ID"]))
        cache[sym] = secid
        self._equity_cache = cache
        self._log(f"EQUITY_SID | {sym} => {secid}")
        return secid

    def get_option_chain_underlying(self, symbol: str) -> tuple[int, str]:
        sym = str(symbol).upper().strip()
        if is_index_symbol(sym):
            return self.get_index_security_id(sym), "IDX_I"
        return self.get_equity_security_id(sym), "NSE_EQ"

    def fetch_chain(self, index: str) -> dict | None:
        self._rate_limit()
        expiry = self.im.get_nearest_option_expiry(index, prefer_weekly=True)
        expiry_str = expiry.strftime("%Y-%m-%d")
        underlying_scrip, seg = self.im.get_option_chain_underlying(index)
        payload = {"UnderlyingScrip": int(underlying_scrip), "UnderlyingSeg": seg, "Expiry": expiry_str}

        if self.debug:
            print(f"OPTIONCHAIN REQ | {index} | UNDERLYING_SID:{underlying_scrip} | SEG:{seg} | EXP:{expiry_str}")

        for attempt in range(1, 4):
            try:
                r = self._session.post(self.BASE_URL, json=payload, timeout=10)
                r.raise_for_status()
                body = r.json()
                data = body.get("data")
                if not data or "oc" not in data:
                    raise ValueError(f"Invalid option chain response keys={list(body.keys())}")
                self._last_chain_cache[index] = data
                self._last_chain_cache_ts[index] = time.time()
                self._last_fetch_ok[index] = True
                self._last_fetch_error[index] = None
                self._last_fetch_retries[index] = attempt - 1
                if self.debug:
                    print(f"OPTION_CHAIN_FETCH_OK | {index} | attempt={attempt} | expiry={expiry_str}")
                return data
            except HTTPError as exc:
                status = getattr(exc.response, "status_code", "NA")
                response_text = (getattr(exc.response, "text", "") or "")[:300]
                print(
                    "OPTION_CHAIN_HTTP_ERROR | index=%s | status=%s | url=%s | expiry=%s | underlying_scrip=%s | underlying_seg=%s | response=%s"
                    % (index, status, self.BASE_URL, expiry_str, underlying_scrip, seg, response_text)
                )
                self._last_fetch_ok[index] = False
                self._last_fetch_error[index] = str(exc)
                self._last_fetch_retries[index] = attempt
                if attempt < 3:
                    time.sleep([1, 2, 4][attempt - 1])
            except Exception as exc:
                self._last_fetch_ok[index] = False
                self._last_fetch_error[index] = str(exc)
                self._last_fetch_retries[index] = attempt
                print(f"OPTION_CHAIN_FETCH_RETRY | index={index} | attempt={attempt} | error={exc}")
                if attempt < 3:
                    time.sleep([1, 2, 4][attempt - 1])

        cached = self._last_chain_cache.get(index)
        print(f"OPTION_CHAIN_FETCH_FAILED_USING_CACHE | index={index} | cache_available={cached is not None}")
        return cached

    def has_open_position(self, index: str | None = None):
        if index is None:
            return len(self.positions) > 0
        idx = str(index).upper().strip()
        return any(self._extract_index(str(pos.get("tag", ""))) == idx for pos in self.positions.values())

    def open_position_tags(self):
        return [str(pos.get("tag", "")) for pos in self.positions.values()]

    def on_entry(self, secid, tag, side, ltp, lots=1, reason="ENTRY", metadata: dict | None = None):
        index = self._extract_index(tag)
        if index and self.has_open_position(index):
            print(f"ENTRY_BLOCKED_INDEX_POSITION | Attempt:{tag} | Index:{index} | Existing:{self.open_position_tags()}")
            self.debug_position_snapshot()
            return False

        instance_has_open_position = self.has_open_position
        self.has_open_position = lambda index=None: False if index is None else instance_has_open_position(index)
        try:
            return original_on_entry(self, secid, tag, side, ltp, lots=lots, reason=reason, metadata=metadata)
        finally:
            try:
                del self.has_open_position
            except AttributeError:
                pass

    InstrumentMaster.__init__ = patched_init
    InstrumentMaster._is_index_symbol = staticmethod(is_index_symbol)
    InstrumentMaster._get_optidx_df = get_option_df
    InstrumentMaster._get_futidx_df = get_future_df
    InstrumentMaster.get_nearest_option_expiry = get_nearest_option_expiry
    InstrumentMaster.get_equity_security_id = get_equity_security_id
    InstrumentMaster.get_option_chain_underlying = get_option_chain_underlying
    selector_module.OptionChainSelector.fetch_chain = fetch_chain
    PaperTradeManager.has_open_position = has_open_position
    PaperTradeManager.open_position_tags = open_position_tags
    PaperTradeManager.on_entry = on_entry
    InstrumentMaster._hdfcbank_live_profile_installed = True
