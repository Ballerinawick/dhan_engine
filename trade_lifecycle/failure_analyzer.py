class FailureAnalyzer:
    def check(self, state, spread):
        if state.seconds_in_trade > 4 and state.mfe < spread * 0.6:
            print("⚠️ FAIL_FAST_EXIT")
            return {"exit": True, "reason": "FAIL_FAST"}

        if state.mae > spread * 1.5:
            print("⚠️ MAE_LIMIT_EXIT")
            return {"exit": True, "reason": "MAE_LIMIT"}

        if state.seconds_below_entry > 6:
            print("⚠️ NEGATIVE_DRIFT_EXIT")
            return {"exit": True, "reason": "NEGATIVE_DRIFT"}

        return None
