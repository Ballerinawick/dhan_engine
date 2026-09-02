from __future__ import annotations

import time
from datetime import time as clock_time

from dhan_engine.infrastructure.dhan.live_order_canary import (
    LIVE_CONFIRMATION,
    NiftyLiveOrderCanary,
    NiftyLiveOrderSettings,
)


class FakeOrderApi:
    def __init__(self):
        self.position_rows = []
        self.placed_orders = []
        self.order_book_rows = []
        self.cancelled = []
        self.next_status = "TRADED"

    def positions(self):
        return list(self.position_rows), 1.5

    def orders(self):
        return list(self.order_book_rows), 1.25

    def place_market_order(
        self, *, security_id, quantity, transaction_type, correlation_id
    ):
        order_id = f"order-{len(self.placed_orders) + 1}"
        self.placed_orders.append(
            {
                "order_id": order_id,
                "security_id": security_id,
                "quantity": quantity,
                "transaction_type": transaction_type,
                "correlation_id": correlation_id,
            }
        )
        return {"orderId": order_id, "orderStatus": "TRANSIT"}, 2.5

    def order(self, order_id):
        order = next(item for item in self.placed_orders if item["order_id"] == order_id)
        filled = order["quantity"] if self.next_status == "TRADED" else 0
        return (
            {
                "orderId": order_id,
                "orderStatus": self.next_status,
                "filledQty": filled,
                "averageTradedPrice": 101.25 if filled else 0,
                "exchangeTime": "2026-09-02 10:00:00",
            },
            1.0,
        )

    def cancel(self, order_id):
        self.cancelled.append(order_id)
        self.next_status = "CANCELLED"
        return {"orderId": order_id, "orderStatus": "CANCELLED"}, 1.0


def settings(tmp_path, *, armed=True, **overrides):
    values = dict(
        enabled=True,
        confirmation=LIVE_CONFIRMATION if armed else "",
        client_id="client",
        access_token="token",
        max_entries_per_day=1,
        max_lots=1,
        fill_timeout_sec=0.1,
        poll_interval_sec=0.01,
        request_timeout_sec=1.0,
        heartbeat_sec=60.0,
        max_unrealized_loss=500.0,
        force_exit_time=clock_time(23, 59),
        state_file=str(tmp_path / "canary.json"),
    )
    values.update(overrides)
    return NiftyLiveOrderSettings(**values)


def wait_idle(canary, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not canary.health()["inflight"]:
            return
        time.sleep(0.01)
    raise AssertionError("canary worker did not become idle")


def register(canary):
    canary.register_contracts(
        {
            "CE": {
                "security_id": 101,
                "strike": 25000,
                "expiry": "2026-09-08",
                "lot_size": 65,
                "symbol": "NIFTY-Sep2026-25000-CE",
            },
            "PE": {
                "security_id": 102,
                "strike": 25000,
                "expiry": "2026-09-08",
                "lot_size": 65,
                "symbol": "NIFTY-Sep2026-25000-PE",
            },
        }
    )


def test_disarmed_canary_never_submits(tmp_path):
    api = FakeOrderApi()
    canary = NiftyLiveOrderCanary(settings(tmp_path, armed=False), api=api)
    register(canary)
    assert canary.submit_entry(
        side="CE", state="BULLISH_EXPANSION", signal_ns=time.perf_counter_ns()
    ) is False
    assert api.placed_orders == []
    canary.close()


def test_one_lot_entry_records_broker_fill_and_blocks_second_entry(tmp_path):
    api = FakeOrderApi()
    canary = NiftyLiveOrderCanary(settings(tmp_path), api=api)
    register(canary)
    assert canary.submit_entry(
        side="CE", state="BULLISH_EXPANSION", signal_ns=time.perf_counter_ns()
    ) is True
    wait_idle(canary)
    assert api.placed_orders[0]["transaction_type"] == "BUY"
    assert api.placed_orders[0]["quantity"] == 65
    health = canary.health()
    assert health["active"] is True
    assert health["entries_today"] == 1
    assert health["entry_fill_price"] == 101.25
    assert canary.submit_entry(
        side="PE", state="BEARISH_EXPANSION", signal_ns=time.perf_counter_ns()
    ) is False
    canary.close()


def test_existing_nifty_broker_position_blocks_entry(tmp_path):
    api = FakeOrderApi()
    api.position_rows = [
        {
            "securityId": "999",
            "tradingSymbol": "NIFTY-Sep2026-25100-CE",
            "exchangeSegment": "NSE_FNO",
            "netQty": 65,
        }
    ]
    canary = NiftyLiveOrderCanary(settings(tmp_path), api=api)
    register(canary)
    assert canary.submit_entry(
        side="CE", state="BULLISH_EXPANSION", signal_ns=time.perf_counter_ns()
    ) is True
    wait_idle(canary)
    assert api.placed_orders == []
    assert canary.health()["active"] is False
    canary.close()


def test_pending_nifty_broker_order_blocks_entry(tmp_path):
    api = FakeOrderApi()
    api.order_book_rows = [
        {
            "orderId": "existing",
            "securityId": "101",
            "tradingSymbol": "NIFTY-Sep2026-25000-CE",
            "transactionType": "BUY",
            "orderStatus": "PENDING",
        }
    ]
    canary = NiftyLiveOrderCanary(settings(tmp_path), api=api)
    register(canary)
    assert canary.submit_entry(
        side="CE", state="BULLISH_EXPANSION", signal_ns=time.perf_counter_ns()
    ) is True
    wait_idle(canary)
    assert api.placed_orders == []
    assert canary.health()["active"] is False
    canary.close()


def test_normal_paper_exit_remains_manual_but_external_close_is_reconciled(tmp_path):
    api = FakeOrderApi()
    canary = NiftyLiveOrderCanary(settings(tmp_path), api=api)
    register(canary)
    canary.submit_entry(
        side="CE", state="BULLISH_EXPANSION", signal_ns=time.perf_counter_ns()
    )
    wait_idle(canary)
    canary.notify_paper_exit("DEEPLOB_V2_EXIT:BULLISH_EXHAUSTION")
    assert len(api.placed_orders) == 1
    assert canary.health()["paper_exit_reason"].endswith("BULLISH_EXHAUSTION")
    canary.heartbeat()
    wait_idle(canary)
    assert canary.health()["active"] is False
    canary.close()


def test_safety_exit_sells_actual_broker_quantity(tmp_path):
    api = FakeOrderApi()
    canary = NiftyLiveOrderCanary(settings(tmp_path), api=api)
    register(canary)
    canary.submit_entry(
        side="PE", state="BEARISH_EXPANSION", signal_ns=time.perf_counter_ns()
    )
    wait_idle(canary)
    api.position_rows = [
        {
            "securityId": "102",
            "tradingSymbol": "NIFTY-Sep2026-25000-PE",
            "exchangeSegment": "NSE_FNO",
            "netQty": 65,
            "unrealizedProfit": -600,
            "buyAvg": 101.25,
        }
    ]
    assert canary.request_exit(reason="MAX_UNREALIZED_LOSS") is True
    wait_idle(canary)
    assert api.placed_orders[-1]["transaction_type"] == "SELL"
    assert api.placed_orders[-1]["quantity"] == 65
    assert canary.health()["active"] is False
    canary.close()


def test_service_shutdown_exits_open_broker_position(tmp_path):
    api = FakeOrderApi()
    canary = NiftyLiveOrderCanary(settings(tmp_path), api=api)
    register(canary)
    canary.submit_entry(
        side="CE", state="BULLISH_EXPANSION", signal_ns=time.perf_counter_ns()
    )
    wait_idle(canary)
    api.position_rows = [
        {
            "securityId": "101",
            "tradingSymbol": "NIFTY-Sep2026-25000-CE",
            "exchangeSegment": "NSE_FNO",
            "netQty": 65,
            "unrealizedProfit": 100,
            "buyAvg": 101.25,
        }
    ]
    canary.close()
    assert api.placed_orders[-1]["transaction_type"] == "SELL"
    assert canary.health()["active"] is False
