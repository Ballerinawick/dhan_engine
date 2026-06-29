import unittest
from collections import defaultdict
from types import SimpleNamespace
from unittest.mock import Mock

from dhan_engine.application.runtime import TradingRuntimeCoordinator


class StaticPairRecoveryTests(unittest.TestCase):
    def test_static_pair_reconnects_same_contracts_without_reselection(self):
        runtime = TradingRuntimeCoordinator.__new__(TradingRuntimeCoordinator)
        runtime.pair_stale_resubscribe_sec = 45.0
        runtime.pair_stale_resubscribe_cooldown_sec = 45.0
        runtime._last_pair_resubscribe_ts = defaultdict(float)
        runtime.static_daily_option_pairs = True
        runtime.pair_stale_entry_quarantine_sec = 120.0
        runtime.option_quote_stream = Mock()
        runtime._set_tri_wave_data_quarantine = Mock()
        pair = SimpleNamespace(ce_id=79732, pe_id=79733)

        runtime._recover_stale_option_pair("NIFTY", pair, now=100.0)

        runtime._set_tri_wave_data_quarantine.assert_called_once_with(
            "NIFTY", "PAIR_STALE_RECOVERY", 120.0
        )
        runtime.option_quote_stream.reconnect_for_subscriptions.assert_called_once_with(
            [(79732, "NIFTY_CE"), (79733, "NIFTY_PE")],
            reason="static_pair_stale:NIFTY",
        )
        self.assertEqual(pair.ce_id, 79732)
        self.assertEqual(pair.pe_id, 79733)

    def test_static_pair_recovery_honors_cooldown(self):
        runtime = TradingRuntimeCoordinator.__new__(TradingRuntimeCoordinator)
        runtime.pair_stale_resubscribe_sec = 45.0
        runtime.pair_stale_resubscribe_cooldown_sec = 45.0
        runtime._last_pair_resubscribe_ts = defaultdict(float, {"NIFTY": 90.0})
        runtime.static_daily_option_pairs = True
        runtime.pair_stale_entry_quarantine_sec = 120.0
        runtime.option_quote_stream = Mock()
        runtime._set_tri_wave_data_quarantine = Mock()
        pair = SimpleNamespace(ce_id=79732, pe_id=79733)

        runtime._recover_stale_option_pair("NIFTY", pair, now=100.0)

        runtime.option_quote_stream.reconnect_for_subscriptions.assert_not_called()
        runtime._set_tri_wave_data_quarantine.assert_not_called()


if __name__ == "__main__":
    unittest.main()
