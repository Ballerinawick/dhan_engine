from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EquityCharges:
    brokerage: float
    exchange: float
    stt: float
    sebi: float
    ipft: float
    stamp_duty: float
    gst: float

    @property
    def total(self) -> float:
        return (
            self.brokerage
            + self.exchange
            + self.stt
            + self.sebi
            + self.ipft
            + self.stamp_duty
            + self.gst
        )


class NseIntradayChargeCalculator:
    """Estimate Dhan NSE cash-intraday charges from executed turnover."""

    def __init__(
        self,
        *,
        brokerage_rate: float = 0.0003,
        brokerage_cap_per_order: float = 20.0,
        exchange_rate: float = 0.0000297,
        stt_sell_rate: float = 0.00025,
        sebi_rate: float = 0.000001,
        ipft_rate: float = 0.000001,
        stamp_buy_rate: float = 0.00003,
        gst_rate: float = 0.18,
    ):
        self.brokerage_rate = max(float(brokerage_rate), 0.0)
        self.brokerage_cap_per_order = max(float(brokerage_cap_per_order), 0.0)
        self.exchange_rate = max(float(exchange_rate), 0.0)
        self.stt_sell_rate = max(float(stt_sell_rate), 0.0)
        self.sebi_rate = max(float(sebi_rate), 0.0)
        self.ipft_rate = max(float(ipft_rate), 0.0)
        self.stamp_buy_rate = max(float(stamp_buy_rate), 0.0)
        self.gst_rate = max(float(gst_rate), 0.0)

    def estimate(self, buy_price: float, sell_price: float, qty: int) -> EquityCharges:
        quantity = max(int(qty), 0)
        buy_turnover = max(float(buy_price), 0.0) * quantity
        sell_turnover = max(float(sell_price), 0.0) * quantity
        turnover = buy_turnover + sell_turnover

        buy_brokerage = min(buy_turnover * self.brokerage_rate, self.brokerage_cap_per_order)
        sell_brokerage = min(sell_turnover * self.brokerage_rate, self.brokerage_cap_per_order)
        brokerage = buy_brokerage + sell_brokerage
        exchange = turnover * self.exchange_rate
        sebi = turnover * self.sebi_rate
        ipft = turnover * self.ipft_rate
        stt = sell_turnover * self.stt_sell_rate
        stamp_duty = buy_turnover * self.stamp_buy_rate
        gst = (brokerage + exchange + sebi + ipft) * self.gst_rate
        return EquityCharges(
            brokerage=brokerage,
            exchange=exchange,
            stt=stt,
            sebi=sebi,
            ipft=ipft,
            stamp_duty=stamp_duty,
            gst=gst,
        )

