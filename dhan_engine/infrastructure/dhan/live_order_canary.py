from __future__ import annotations

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, time as clock_time
from pathlib import Path
from typing import Mapping
from zoneinfo import ZoneInfo

import requests


logger = logging.getLogger(__name__)
LIVE_CONFIRMATION = "I_ACCEPT_NIFTY_LIVE_ORDER_RISK"
TERMINAL_ORDER_STATES = {"TRADED", "REJECTED", "CANCELLED", "EXPIRED"}


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _clock(raw: str) -> clock_time:
    return datetime.strptime(raw.strip(), "%H:%M").time()


@dataclass(frozen=True)
class NiftyLiveOrderSettings:
    enabled: bool
    confirmation: str
    client_id: str
    access_token: str
    product_type: str = "INTRADAY"
    max_entries_per_day: int = 1
    max_lots: int = 1
    fill_timeout_sec: float = 5.0
    poll_interval_sec: float = 0.10
    request_timeout_sec: float = 3.0
    heartbeat_sec: float = 5.0
    max_unrealized_loss: float = 500.0
    force_exit_time: clock_time = clock_time(15, 20)
    mirror_state_exits: bool = False
    state_file: str = "/var/lib/dhan-engine/live-orders/nifty-canary.json"
    api_base_url: str = "https://api.dhan.co/v2"

    @property
    def armed(self) -> bool:
        return self.enabled and self.confirmation == LIVE_CONFIRMATION

    @classmethod
    def from_env(cls) -> "NiftyLiveOrderSettings":
        prefix = "NIFTY_LIVE_ORDERS_"
        return cls(
            enabled=_env_bool(prefix + "ENABLED"),
            confirmation=os.getenv(prefix + "CONFIRMATION", "").strip(),
            client_id=os.getenv("DHAN_CLIENT_ID", "").strip(),
            access_token=os.getenv("DHAN_ACCESS_TOKEN", "").strip(),
            product_type=os.getenv(prefix + "PRODUCT_TYPE", "INTRADAY").strip().upper(),
            max_entries_per_day=max(1, int(os.getenv(prefix + "MAX_ENTRIES_PER_DAY", "1"))),
            max_lots=max(1, int(os.getenv(prefix + "MAX_LOTS", "1"))),
            fill_timeout_sec=max(1.0, float(os.getenv(prefix + "FILL_TIMEOUT_SEC", "5"))),
            poll_interval_sec=max(0.05, float(os.getenv(prefix + "POLL_INTERVAL_MS", "100")) / 1000.0),
            request_timeout_sec=max(1.0, float(os.getenv(prefix + "REQUEST_TIMEOUT_SEC", "3"))),
            heartbeat_sec=max(2.0, float(os.getenv(prefix + "HEARTBEAT_SEC", "5"))),
            max_unrealized_loss=max(1.0, float(os.getenv(prefix + "MAX_UNREALIZED_LOSS", "500"))),
            force_exit_time=_clock(os.getenv(prefix + "FORCE_EXIT_TIME", "15:20")),
            mirror_state_exits=_env_bool(prefix + "MIRROR_STATE_EXITS", "0"),
            state_file=os.getenv(
                prefix + "STATE_FILE",
                "/var/lib/dhan-engine/live-orders/nifty-canary.json",
            ).strip(),
            api_base_url=os.getenv(prefix + "API_BASE_URL", "https://api.dhan.co/v2").rstrip("/"),
        )


class DhanOrderApi:
    def __init__(self, settings: NiftyLiveOrderSettings):
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "access-token": settings.access_token,
                "client-id": settings.client_id,
            }
        )

    def _request(self, method: str, path: str, *, payload: dict | None = None):
        started = time.perf_counter_ns()
        response = self.session.request(
            method,
            f"{self.settings.api_base_url}{path}",
            json=payload,
            timeout=self.settings.request_timeout_sec,
        )
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000.0
        try:
            body = response.json()
        except ValueError:
            body = {"raw": response.text[:500]}
        if not response.ok:
            raise RuntimeError(
                f"Dhan API {method} {path} failed status={response.status_code} body={body}"
            )
        return body, elapsed_ms

    def positions(self):
        return self._request("GET", "/positions")

    def orders(self):
        return self._request("GET", "/orders")

    def place_market_order(
        self, *, security_id: int, quantity: int, transaction_type: str, correlation_id: str
    ):
        payload = {
            "dhanClientId": self.settings.client_id,
            "correlationId": correlation_id,
            "transactionType": transaction_type,
            "exchangeSegment": "NSE_FNO",
            "productType": self.settings.product_type,
            "orderType": "MARKET",
            "validity": "DAY",
            "securityId": str(security_id),
            "quantity": int(quantity),
            "disclosedQuantity": 0,
            "price": 0,
            "triggerPrice": 0,
            "afterMarketOrder": False,
            "amoTime": "",
            "boProfitValue": "",
            "boStopLossValue": "",
        }
        return self._request("POST", "/orders", payload=payload)

    def order(self, order_id: str):
        return self._request("GET", f"/orders/{order_id}")

    def cancel(self, order_id: str):
        return self._request("DELETE", f"/orders/{order_id}")


class NiftyLiveOrderCanary:
    """One-lot, one-entry live canary driven only by NIFTY regime_v2."""

    def __init__(self, settings: NiftyLiveOrderSettings, api=None):
        self.settings = settings
        self.api = api or DhanOrderApi(settings)
        self._timezone = ZoneInfo("Asia/Kolkata")
        self._lock = threading.Lock()
        self._worker = ThreadPoolExecutor(max_workers=1, thread_name_prefix="nifty-live-order")
        self._stop_event = threading.Event()
        self._monitor_thread = None
        self._inflight = False
        self._last_heartbeat_mono = float("-inf")
        self._contracts: dict[str, dict] = {}
        self._state = self._load_state()
        self._roll_day()
        if settings.enabled and not settings.armed:
            logger.error(
                "NIFTY_LIVE_CANARY_DISARMED | reason=CONFIRMATION_MISMATCH | expected=%s",
                LIVE_CONFIRMATION,
            )
        elif settings.armed and (not settings.client_id or not settings.access_token):
            raise RuntimeError("NIFTY live orders require DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN")
        elif settings.armed and settings.product_type != "INTRADAY":
            raise RuntimeError("NIFTY live canary only supports product_type=INTRADAY")
        elif settings.armed and (
            settings.max_entries_per_day != 1 or settings.max_lots != 1
        ):
            raise RuntimeError("NIFTY live canary is hard-limited to one entry and one lot")
        logger.warning(
            "NIFTY_LIVE_CANARY_CONFIG | enabled=%s | armed=%s | product=%s | max_entries=%s | "
            "max_lots=%s | mirror_state_exits=%s | force_exit=%s | max_loss=%.2f",
            settings.enabled,
            settings.armed,
            settings.product_type,
            settings.max_entries_per_day,
            settings.max_lots,
            settings.mirror_state_exits,
            settings.force_exit_time.strftime("%H:%M"),
            settings.max_unrealized_loss,
        )
        if settings.armed:
            self._monitor_thread = threading.Thread(
                target=self._monitor_loop,
                name="nifty-live-safety-monitor",
                daemon=True,
            )
            self._monitor_thread.start()

    def register_contracts(self, selection: Mapping[str, Mapping]) -> None:
        with self._lock:
            self._contracts = {
                side: dict(contract)
                for side, contract in selection.items()
                if side in {"CE", "PE"} and int(contract.get("security_id", 0) or 0) > 0
            }
        logger.info(
            "NIFTY_LIVE_CONTRACTS | ce=%s | pe=%s",
            self._contracts.get("CE"),
            self._contracts.get("PE"),
        )

    def submit_entry(self, *, side: str, state: str, signal_ns: int) -> bool:
        if not self.settings.armed:
            return False
        self._roll_day()
        side = side.upper()
        with self._lock:
            contract = dict(self._contracts.get(side) or {})
            if self._inflight or self._state.get("active"):
                logger.warning("NIFTY_LIVE_ENTRY_BLOCKED | reason=ORDER_OR_POSITION_ACTIVE")
                return False
            if int(self._state.get("entries", 0)) >= self.settings.max_entries_per_day:
                logger.warning("NIFTY_LIVE_ENTRY_BLOCKED | reason=DAILY_ENTRY_LIMIT")
                return False
            lot_size = int(contract.get("lot_size", 0) or 0)
            if not contract or lot_size <= 0:
                logger.error("NIFTY_LIVE_ENTRY_BLOCKED | reason=CONTRACT_LOT_SIZE_MISSING")
                return False
            self._inflight = True
        self._worker.submit(self._place_entry, contract, side, state, signal_ns)
        return True

    def notify_paper_exit(self, reason: str) -> None:
        if not self.settings.armed:
            return
        if self.settings.mirror_state_exits:
            self.request_exit(reason=f"MIRROR:{reason}")
            return
        with self._lock:
            active = bool(self._state.get("active"))
            if active:
                self._state["paper_exit_reason"] = reason
                self._persist_state()
        if active:
            logger.warning(
                "NIFTY_LIVE_MANUAL_EXIT_REQUIRED | reason=%s | security_id=%s | quantity=%s",
                reason,
                self._state.get("security_id"),
                self._state.get("quantity"),
            )

    def request_exit(self, *, reason: str) -> bool:
        if not self.settings.armed:
            return False
        with self._lock:
            if self._inflight or not self._state.get("active"):
                return False
            self._inflight = True
        self._worker.submit(self._exit_position, reason)
        return True

    def heartbeat(self) -> None:
        if not self.settings.armed:
            return
        now_mono = time.monotonic()
        if now_mono - self._last_heartbeat_mono < self.settings.heartbeat_sec:
            return
        self._last_heartbeat_mono = now_mono
        with self._lock:
            if self._inflight or not self._state.get("active"):
                return
            self._inflight = True
        self._worker.submit(self._reconcile_and_protect)

    def health(self) -> dict:
        with self._lock:
            return {
                "enabled": self.settings.enabled,
                "armed": self.settings.armed,
                "inflight": self._inflight,
                "entries_today": int(self._state.get("entries", 0)),
                "active": bool(self._state.get("active")),
                "security_id": self._state.get("security_id"),
                "quantity": self._state.get("quantity"),
                "entry_order_id": self._state.get("entry_order_id"),
                "entry_fill_price": self._state.get("entry_fill_price"),
                "paper_exit_reason": self._state.get("paper_exit_reason"),
            }

    def close(self) -> None:
        self._stop_event.set()
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=2.0)
        deadline = time.monotonic() + self.settings.fill_timeout_sec + 2.0
        while self.health()["inflight"] and time.monotonic() < deadline:
            time.sleep(0.05)
        if self.health()["active"]:
            self.request_exit(reason="SERVICE_SHUTDOWN")
        self._worker.shutdown(wait=True, cancel_futures=False)
        if self.health()["active"]:
            with self._lock:
                self._inflight = True
            self._exit_position("SERVICE_SHUTDOWN_RETRY")

    def _monitor_loop(self) -> None:
        while not self._stop_event.wait(self.settings.heartbeat_sec):
            self.heartbeat()

    def _place_entry(self, contract: dict, side: str, state: str, signal_ns: int) -> None:
        try:
            positions, reconcile_ms = self.api.positions()
            if self._has_any_nifty_position(positions):
                logger.error(
                    "NIFTY_LIVE_ENTRY_BLOCKED | reason=BROKER_POSITION_ALREADY_OPEN | reconcile_ms=%.2f",
                    reconcile_ms,
                )
                return
            orders, orders_ms = self.api.orders()
            conflict = self._broker_order_conflict(orders)
            if conflict:
                logger.error(
                    "NIFTY_LIVE_ENTRY_BLOCKED | reason=%s | order_id=%s | order_status=%s | "
                    "orders_ms=%.2f",
                    conflict[0],
                    conflict[1].get("orderId"),
                    conflict[1].get("orderStatus"),
                    orders_ms,
                )
                return
            quantity = int(contract["lot_size"]) * self.settings.max_lots
            correlation_id = self._correlation_id("B", side)
            logger.warning(
                "NIFTY_LIVE_ENTRY_SUBMIT | side=%s | secid=%s | strike=%s | expiry=%s | "
                "quantity=%s | state=%s | correlation_id=%s",
                side,
                contract["security_id"],
                contract.get("strike"),
                contract.get("expiry"),
                quantity,
                state,
                correlation_id,
            )
            response, ack_ms = self.api.place_market_order(
                security_id=int(contract["security_id"]),
                quantity=quantity,
                transaction_type="BUY",
                correlation_id=correlation_id,
            )
            order_id = str(self._field(response, "orderId", "") or "")
            status = str(self._field(response, "orderStatus", "") or "").upper()
            signal_to_ack_ms = (time.perf_counter_ns() - signal_ns) / 1_000_000.0
            logger.warning(
                "NIFTY_LIVE_ORDER_ACK | action=BUY | order_id=%s | status=%s | "
                "http_ack_ms=%.2f | signal_to_ack_ms=%.2f",
                order_id,
                status,
                ack_ms,
                signal_to_ack_ms,
            )
            if not order_id:
                raise RuntimeError(f"Dhan order response missing orderId: {response}")
            final = self._wait_for_terminal(order_id)
            final_status = str(self._field(final, "orderStatus", status) or status).upper()
            filled_qty = int(float(self._field(final, "filledQty", 0) or 0))
            if final_status != "TRADED" and filled_qty <= 0:
                logger.error(
                    "NIFTY_LIVE_ENTRY_NOT_FILLED | order_id=%s | status=%s | error=%s",
                    order_id,
                    final_status,
                    self._field(final, "omsErrorDescription", ""),
                )
                return
            fill_price = float(self._field(final, "averageTradedPrice", 0.0) or 0.0)
            fill_qty = filled_qty or quantity
            signal_to_fill_ms = (time.perf_counter_ns() - signal_ns) / 1_000_000.0
            with self._lock:
                self._state.update(
                    active=True,
                    entries=int(self._state.get("entries", 0)) + 1,
                    security_id=int(contract["security_id"]),
                    side=side,
                    quantity=fill_qty,
                    entry_order_id=order_id,
                    entry_fill_price=fill_price,
                    entry_exchange_time=self._field(final, "exchangeTime", ""),
                    entry_correlation_id=correlation_id,
                    paper_exit_reason=None,
                )
                self._persist_state()
            logger.warning(
                "NIFTY_LIVE_ENTRY_FILLED | side=%s | order_id=%s | secid=%s | quantity=%s | "
                "fill_price=%.2f | signal_to_fill_ms=%.2f | exchange_time=%s",
                side,
                order_id,
                contract["security_id"],
                fill_qty,
                fill_price,
                signal_to_fill_ms,
                self._field(final, "exchangeTime", ""),
            )
        except Exception:
            logger.exception("NIFTY_LIVE_ENTRY_FAILED")
        finally:
            with self._lock:
                self._inflight = False

    def _reconcile_and_protect(self) -> None:
        exit_reason = ""
        try:
            positions, elapsed_ms = self.api.positions()
            security_id = int(self._state.get("security_id", 0) or 0)
            position = self._matching_position(positions, security_id)
            if position is None or int(float(position.get("netQty", 0) or 0)) <= 0:
                logger.warning(
                    "NIFTY_LIVE_POSITION_CLOSED_EXTERNALLY | secid=%s | reconcile_ms=%.2f",
                    security_id,
                    elapsed_ms,
                )
                with self._lock:
                    self._clear_active_state()
                    self._persist_state()
                return
            unrealized = float(position.get("unrealizedProfit", 0.0) or 0.0)
            now = datetime.now(self._timezone).time()
            logger.info(
                "NIFTY_LIVE_POSITION | secid=%s | net_qty=%s | buy_avg=%s | unrealized=%+.2f | "
                "paper_exit_reason=%s | reconcile_ms=%.2f",
                security_id,
                position.get("netQty"),
                position.get("buyAvg"),
                unrealized,
                self._state.get("paper_exit_reason"),
                elapsed_ms,
            )
            if unrealized <= -self.settings.max_unrealized_loss:
                exit_reason = "MAX_UNREALIZED_LOSS"
            elif now >= self.settings.force_exit_time:
                exit_reason = "FORCE_EXIT_TIME"
        except Exception:
            logger.exception("NIFTY_LIVE_RECONCILE_FAILED")
        finally:
            with self._lock:
                self._inflight = False
        if exit_reason:
            self.request_exit(reason=exit_reason)

    def _exit_position(self, reason: str) -> None:
        try:
            positions, reconcile_ms = self.api.positions()
            security_id = int(self._state.get("security_id", 0) or 0)
            position = self._matching_position(positions, security_id)
            if position is None:
                with self._lock:
                    self._clear_active_state()
                    self._persist_state()
                return
            quantity = max(0, int(float(position.get("netQty", 0) or 0)))
            if quantity <= 0:
                with self._lock:
                    self._clear_active_state()
                    self._persist_state()
                return
            signal_ns = time.perf_counter_ns()
            correlation_id = self._correlation_id("S", str(self._state.get("side", "X")))
            logger.error(
                "NIFTY_LIVE_EXIT_SUBMIT | reason=%s | secid=%s | quantity=%s | reconcile_ms=%.2f",
                reason,
                security_id,
                quantity,
                reconcile_ms,
            )
            response, ack_ms = self.api.place_market_order(
                security_id=security_id,
                quantity=quantity,
                transaction_type="SELL",
                correlation_id=correlation_id,
            )
            order_id = str(self._field(response, "orderId", "") or "")
            if not order_id:
                raise RuntimeError(f"Dhan exit response missing orderId: {response}")
            final = self._wait_for_terminal(order_id)
            status = str(self._field(final, "orderStatus", "") or "").upper()
            filled_qty = int(float(self._field(final, "filledQty", 0) or 0))
            fill_price = float(self._field(final, "averageTradedPrice", 0.0) or 0.0)
            elapsed_ms = (time.perf_counter_ns() - signal_ns) / 1_000_000.0
            logger.error(
                "NIFTY_LIVE_EXIT_RESULT | reason=%s | order_id=%s | status=%s | filled_qty=%s | "
                "fill_price=%.2f | http_ack_ms=%.2f | request_to_fill_ms=%.2f",
                reason,
                order_id,
                status,
                filled_qty,
                fill_price,
                ack_ms,
                elapsed_ms,
            )
            if status == "TRADED" or filled_qty >= quantity:
                with self._lock:
                    self._clear_active_state()
                    self._persist_state()
        except Exception:
            logger.exception("NIFTY_LIVE_EXIT_FAILED | reason=%s", reason)
        finally:
            with self._lock:
                self._inflight = False

    def _wait_for_terminal(self, order_id: str) -> dict:
        deadline = time.monotonic() + self.settings.fill_timeout_sec
        last = {}
        while time.monotonic() < deadline:
            last, poll_ms = self.api.order(order_id)
            status = str(self._field(last, "orderStatus", "") or "").upper()
            if status in TERMINAL_ORDER_STATES:
                logger.info(
                    "NIFTY_LIVE_ORDER_TERMINAL | order_id=%s | status=%s | poll_ms=%.2f",
                    order_id,
                    status,
                    poll_ms,
                )
                return last
            time.sleep(self.settings.poll_interval_sec)
        logger.error("NIFTY_LIVE_ORDER_TIMEOUT | order_id=%s | cancelling=true", order_id)
        try:
            self.api.cancel(order_id)
        except Exception:
            logger.exception("NIFTY_LIVE_ORDER_CANCEL_FAILED | order_id=%s", order_id)
        final, _ = self.api.order(order_id)
        return final

    def _matching_position(self, response, security_id: int) -> dict | None:
        positions = self._data(response)
        if isinstance(positions, dict):
            positions = [positions]
        for position in positions or []:
            if int(float(position.get("securityId", 0) or 0)) != security_id:
                continue
            if str(position.get("exchangeSegment", "")).upper() != "NSE_FNO":
                continue
            if int(float(position.get("netQty", 0) or 0)) != 0:
                return position
        return None

    def _has_any_nifty_position(self, response) -> bool:
        positions = self._data(response)
        if isinstance(positions, dict):
            positions = [positions]
        selected_ids = {
            int(contract.get("security_id", 0) or 0)
            for contract in self._contracts.values()
        }
        for position in positions or []:
            if str(position.get("exchangeSegment", "")).upper() != "NSE_FNO":
                continue
            if int(float(position.get("netQty", 0) or 0)) == 0:
                continue
            security_id = int(float(position.get("securityId", 0) or 0))
            symbol = str(position.get("tradingSymbol", "")).upper()
            if security_id in selected_ids or symbol.startswith("NIFTY"):
                return True
        return False

    def _broker_order_conflict(self, response):
        orders = self._data(response)
        if isinstance(orders, dict):
            orders = [orders]
        selected_ids = {
            int(contract.get("security_id", 0) or 0)
            for contract in self._contracts.values()
        }
        for order in orders or []:
            status = str(order.get("orderStatus", "")).upper()
            correlation = str(order.get("correlationId", ""))
            transaction = str(order.get("transactionType", "")).upper()
            security_id = int(float(order.get("securityId", 0) or 0))
            symbol = str(order.get("tradingSymbol", "")).upper()
            if correlation.startswith("NLC") and transaction == "BUY":
                return "PRIOR_CANARY_ORDER_TODAY", order
            is_nifty = security_id in selected_ids or symbol.startswith("NIFTY")
            if is_nifty and status in {"TRANSIT", "PENDING", "PART_TRADED"}:
                return "NIFTY_ORDER_ALREADY_PENDING", order
        return None

    @staticmethod
    def _data(response):
        return response.get("data", response) if isinstance(response, dict) else response

    @classmethod
    def _field(cls, response, name: str, default=None):
        data = cls._data(response)
        return data.get(name, default) if isinstance(data, dict) else default

    def _correlation_id(self, action: str, side: str) -> str:
        stamp = datetime.now(self._timezone).strftime("%y%m%d%H%M%S%f")[:16]
        return f"NLC{action}{side}{stamp}"[:30]

    def _roll_day(self) -> None:
        today = datetime.now(self._timezone).date().isoformat()
        with self._lock:
            if self._state.get("trade_date") == today:
                return
            if self._state.get("active"):
                logger.critical(
                    "NIFTY_LIVE_STATE_CARRIED_OVERNIGHT | previous_day=%s | secid=%s",
                    self._state.get("trade_date"),
                    self._state.get("security_id"),
                )
                return
            self._state = {"trade_date": today, "entries": 0, "active": False}
            self._persist_state()

    def _load_state(self) -> dict:
        path = Path(self.settings.state_file)
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            logger.exception("NIFTY_LIVE_STATE_LOAD_FAILED | path=%s", path)
            return {}

    def _persist_state(self) -> None:
        path = Path(self.settings.state_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(self._state, indent=2, sort_keys=True), encoding="utf-8")
        temporary.replace(path)

    def _clear_active_state(self) -> None:
        entries = int(self._state.get("entries", 0))
        trade_date = self._state.get("trade_date")
        self._state = {"trade_date": trade_date, "entries": entries, "active": False}
