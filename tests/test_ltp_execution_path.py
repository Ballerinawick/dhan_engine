import unittest
from types import SimpleNamespace

from dhan_engine.domain.market.ltp_execution_path import (
    LtpExecutionSample,
    sample_ltp_execution_path,
    summarize_ltp_execution_path,
)


def _sample(
    second: float,
    ltp: float,
    *,
    buy_qty: int = 0,
    sell_qty: int = 0,
    ask_consumed: int = 0,
    bid_consumed: int = 0,
    bid_replenished: int = 0,
    ask_replenished: int = 0,
    event_score: float = 0.0,
) -> LtpExecutionSample:
    return LtpExecutionSample(
        received_mono=second,
        ltp=ltp,
        mid=ltp,
        event_score=event_score,
        evidence_quality=0.9,
        aggressive_buy_qty=buy_qty,
        aggressive_sell_qty=sell_qty,
        bid_consumed_qty=bid_consumed,
        ask_consumed_qty=ask_consumed,
        bid_cancelled_qty=0,
        ask_cancelled_qty=0,
        bid_replenished_qty=bid_replenished,
        ask_replenished_qty=ask_replenished,
    )


class LtpExecutionPathTests(unittest.TestCase):
    def test_bullish_ltp_and_execution_path_forecasts_up(self) -> None:
        path = summarize_ltp_execution_path(
            [
                _sample(0.0, 24_200.0, buy_qty=80, ask_consumed=60, event_score=0.4),
                _sample(
                    1.0,
                    24_202.0,
                    buy_qty=120,
                    ask_consumed=90,
                    bid_replenished=40,
                    event_score=0.6,
                ),
                _sample(
                    2.0,
                    24_205.0,
                    buy_qty=150,
                    ask_consumed=110,
                    bid_replenished=60,
                    event_score=0.7,
                ),
            ]
        )

        self.assertEqual(path.ltp_now, 24_205.0)
        self.assertGreater(path.ltp_return_bps, 0)
        self.assertAlmostEqual(path.execution_imbalance, 1.0)
        self.assertGreater(path.liquidity_imbalance, 0)
        self.assertGreater(path.strength, 0)
        self.assertGreater(path.forecast_bps(30, pressure=path.strength), 0)

    def test_bearish_ltp_and_execution_path_forecasts_down(self) -> None:
        path = summarize_ltp_execution_path(
            [
                _sample(0.0, 24_205.0, sell_qty=80, bid_consumed=60, event_score=-0.4),
                _sample(
                    1.0,
                    24_202.0,
                    sell_qty=120,
                    bid_consumed=90,
                    ask_replenished=40,
                    event_score=-0.6,
                ),
                _sample(
                    2.0,
                    24_198.0,
                    sell_qty=150,
                    bid_consumed=110,
                    ask_replenished=60,
                    event_score=-0.7,
                ),
            ]
        )

        self.assertEqual(path.ltp_now, 24_198.0)
        self.assertLess(path.ltp_return_bps, 0)
        self.assertAlmostEqual(path.execution_imbalance, -1.0)
        self.assertLess(path.liquidity_imbalance, 0)
        self.assertLess(path.strength, 0)
        self.assertLess(path.forecast_bps(30, pressure=path.strength), 0)

    def test_sample_falls_back_to_mid_when_full_quote_is_absent(self) -> None:
        composite = SimpleNamespace(features=SimpleNamespace(mid=24_200.5))

        sample = sample_ltp_execution_path(10.0, composite)

        self.assertEqual(sample.ltp, 24_200.5)
        self.assertEqual(sample.mid, 24_200.5)


if __name__ == "__main__":
    unittest.main()
