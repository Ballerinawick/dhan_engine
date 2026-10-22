from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest

from dhan_engine.application.nifty_cross_paper import DataRateLimited, NiftyCrossPaper
from dhan_engine.simulations.nifty_cross_book import Close


NOW = datetime(2026, 10, 22, 10, 0, tzinfo=ZoneInfo("Asia/Kolkata"))


def test_429_backs_off_instead_of_retrying_immediately():
    adapter = NiftyCrossPaper.__new__(NiftyCrossPaper)
    adapter.http = SimpleNamespace(post=Mock(return_value=SimpleNamespace(
        status_code=429, headers={"Retry-After": "120"})))
    adapter._rate_limit_count = 0
    adapter._backoff_until = 0
    with patch("dhan_engine.application.nifty_cross_paper.time.monotonic", return_value=100):
        with pytest.raises(DataRateLimited):
            adapter._post("/charts/intraday", {})
    assert adapter._backoff_until == 220
    assert adapter._rate_limit_count == 1


def test_history_is_not_reloaded_on_chain_refresh(tmp_path: Path):
    adapter = NiftyCrossPaper.__new__(NiftyCrossPaper)
    adapter.master = SimpleNamespace(get_nearest_future=lambda _: {"security_id": "7"})
    adapter.state_path = tmp_path / "cross.json"
    adapter.session = None
    adapter.future_id = None
    adapter._chain_at = 0
    adapter._backoff_until = 0
    adapter._rate_limit_count = 0
    adapter._history_attempted = set()
    adapter._index_price = None
    strikes = [22500, 22550, 22600, 22650, 22700]
    chain = {str(strike): {"ce": {"security_id": 100 + i},
                            "pe": {"security_id": 200 + i}}
             for i, strike in enumerate(strikes)}
    adapter._post = lambda endpoint, _: (
        {"data": ["2026-10-27"]} if endpoint.endswith("expirylist")
        else {"data": {"last_price": 22600, "oc": chain}})
    calls = []

    def chart(security_id, instrument, now):
        calls.append(security_id)
        return [Close(now - timedelta(minutes=1), 22600 if instrument == "FUTIDX" else 100)]

    adapter._chart = chart
    adapter.refresh_contracts(NOW, force=True)
    assert len(calls) == 11
    adapter.refresh_contracts(NOW + timedelta(minutes=2), force=True)
    assert len(calls) == 11

