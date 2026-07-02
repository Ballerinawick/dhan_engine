import unittest
from collections import defaultdict
from types import SimpleNamespace
from unittest.mock import Mock

from dhan_engine.application.runtime import TradingRuntimeCoordinator


class StaticPairRecoveryTests(unittest.TestCase):
    def test_static_pair_refreshes_all_contracts_without_reselection(self):
        runtime = TradingRuntimeCoordinator.__new__(TradingRuntimeCoordinator)
        runtime.pair_stale_resubscribe_sec = 45.0
        runtime.pair_stale_resubscribe_cooldown_sec = 45.0
        runtime._last_pair_resubscribe_ts = defaultdict(float)
        runtime.static_daily_option_pairs = True
        runtime.pair_stale_entry_quarantine_sec = 120.0
        runtime.option_quote_stream = Mock()
        runtime.option_quote_stream.refresh_subscriptions.return_value = True
        runtime._set_tri_wave_data_quarantine = Mock()
        runtime._replace_tri_wave_data_quarantine = Mock()
        runtime._premium_rebuild_verify_remaining = Mock(return_value=0.0)
        runtime._should_rebuild_premium_stream = Mock(return_value=False)
        pair = SimpleNamespace(ce_id=79732, pe_id=79733)
        runtime.pairs = {
            "NIFTY": pair,
            "BANKNIFTY": SimpleNamespace(ce_id=75751, pe_id=75662),
        }

        runtime._recover_stale_option_pair("NIFTY", pair, now=100.0)

        runtime._set_tri_wave_data_quarantine.assert_called_once_with(
            "NIFTY", "PAIR_STALE_RECOVERY", 120.0
        )
        runtime.option_quote_stream.refresh_subscriptions.assert_called_once_with(
            [
                (79732, "NIFTY_CE"),
                (79733, "NIFTY_PE"),
                (75751, "BANKNIFTY_CE"),
                (75662, "BANKNIFTY_PE"),
            ],
            reason="static_pair_stale:NIFTY",
        )
        runtime.option_quote_stream.reconnect_for_subscriptions.assert_not_called()
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
        runtime._replace_tri_wave_data_quarantine = Mock()
        runtime._premium_rebuild_verify_remaining = Mock(return_value=0.0)
        runtime._should_rebuild_premium_stream = Mock(return_value=False)
        pair = SimpleNamespace(ce_id=79732, pe_id=79733)
        runtime.pairs = {"NIFTY": pair}

        runtime._recover_stale_option_pair("NIFTY", pair, now=100.0)

        runtime.option_quote_stream.reconnect_for_subscriptions.assert_not_called()
        runtime.option_quote_stream.refresh_subscriptions.assert_not_called()
        runtime._set_tri_wave_data_quarantine.assert_not_called()

    def test_repeated_static_staleness_rebuilds_whole_option_stream(self):
        runtime = TradingRuntimeCoordinator.__new__(TradingRuntimeCoordinator)
        runtime.pair_stale_resubscribe_sec = 45.0
        runtime.pair_stale_resubscribe_cooldown_sec = 45.0
        runtime._last_pair_resubscribe_ts = defaultdict(float)
        runtime.static_daily_option_pairs = True
        runtime.option_quote_stream = Mock()
        runtime._premium_rebuild_verify_remaining = Mock(return_value=0.0)
        runtime._should_rebuild_premium_stream = Mock(return_value=True)
        runtime._rebuild_option_quote_stream = Mock(return_value=True)
        pair = SimpleNamespace(ce_id=79732, pe_id=79733)
        runtime.pairs = {"NIFTY": pair}

        runtime._recover_stale_option_pair("NIFTY", pair, now=100.0)

        runtime._rebuild_option_quote_stream.assert_called_once_with(
            "NIFTY",
            reason="static_pair_repeated_stale:NIFTY",
            now=100.0,
        )
        runtime.option_quote_stream.reconnect_for_subscriptions.assert_not_called()

    def test_failed_in_place_refresh_falls_back_to_reconnect(self):
        runtime = TradingRuntimeCoordinator.__new__(TradingRuntimeCoordinator)
        runtime.pair_stale_resubscribe_sec = 45.0
        runtime.pair_stale_resubscribe_cooldown_sec = 45.0
        runtime._last_pair_resubscribe_ts = defaultdict(float)
        runtime.static_daily_option_pairs = True
        runtime.pair_stale_entry_quarantine_sec = 120.0
        runtime.option_quote_stream = Mock()
        runtime.option_quote_stream.refresh_subscriptions.return_value = False
        runtime._set_tri_wave_data_quarantine = Mock()
        runtime._replace_tri_wave_data_quarantine = Mock()
        runtime._premium_rebuild_verify_remaining = Mock(return_value=0.0)
        runtime._should_rebuild_premium_stream = Mock(return_value=False)
        pair = SimpleNamespace(ce_id=79732, pe_id=79733)
        runtime.pairs = {"NIFTY": pair}

        runtime._recover_stale_option_pair("NIFTY", pair, now=100.0)

        runtime.option_quote_stream.reconnect_for_subscriptions.assert_called_once_with(
            [(79732, "NIFTY_CE"), (79733, "NIFTY_PE")],
            reason="static_pair_refresh_failed:NIFTY",
        )

    def test_rebuild_verification_requires_ticks_from_new_generation(self):
        runtime = TradingRuntimeCoordinator.__new__(TradingRuntimeCoordinator)
        runtime.pairs = {"NIFTY": SimpleNamespace(ce_id=79732, pe_id=79733)}
        runtime.entry_pair_fresh_ltp_max_age_sec = 5.0
        runtime.premium_rebuild_verify_sec = 12.0
        runtime.last_premium_stream_rebuild_ts = 100.0
        runtime.option_ltp_last_ts_by_secid = {79732: 99.0, 79733: 99.0}
        runtime.premium_rebuild_pending_until = {"NIFTY": 112.0}
        runtime.premium_rebuild_generation = 1
        runtime.premium_stale_attempt_ts = defaultdict(list, {"NIFTY": [90.0, 100.0]})
        runtime._clear_tri_wave_data_quarantine = Mock()
        runtime._fmt_age = lambda value: f"{value:.1f}s"

        self.assertFalse(runtime._verify_premium_stream_for_index("NIFTY", now=105.0))
        runtime._clear_tri_wave_data_quarantine.assert_not_called()

        runtime.option_ltp_last_ts_by_secid = {79732: 101.0, 79733: 102.0}
        self.assertTrue(runtime._verify_premium_stream_for_index("NIFTY", now=105.0))
        runtime._clear_tri_wave_data_quarantine.assert_called_once_with("NIFTY", "PREMIUM_STREAM")
        self.assertEqual(runtime.premium_stale_attempt_ts["NIFTY"], [])

    def test_full_rebuild_registers_subscriptions_before_start(self):
        runtime = TradingRuntimeCoordinator.__new__(TradingRuntimeCoordinator)
        runtime.pairs = {
            "NIFTY": SimpleNamespace(ce_id=79732, pe_id=79733),
            "BANKNIFTY": SimpleNamespace(ce_id=75751, pe_id=75662),
        }
        runtime.settings = SimpleNamespace(indexes=("NIFTY", "BANKNIFTY"))
        runtime.premium_rebuild_generation = 0
        runtime.premium_rebuild_verify_sec = 12.0
        runtime.premium_rebuild_pending_until = defaultdict(float)
        runtime.last_premium_stream_rebuild_ts = 0.0
        runtime._replace_tri_wave_data_quarantine = Mock()
        old_stream = Mock()
        new_stream = Mock()
        runtime.option_quote_stream = old_stream
        runtime._make_option_quote_stream = Mock(return_value=new_stream)

        rebuilt = runtime._rebuild_option_quote_stream(
            "NIFTY", reason="test", now=100.0
        )

        self.assertTrue(rebuilt)
        old_stream.close.assert_called_once_with()
        self.assertEqual(new_stream.method_calls[0][0], "replace_subscriptions")
        self.assertEqual(
            new_stream.method_calls[0].kwargs["reason"],
            "premium_rebuild:test",
        )
        self.assertEqual(new_stream.method_calls[1][0], "start")


if __name__ == "__main__":
    unittest.main()
