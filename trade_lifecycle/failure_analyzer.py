class FailureAnalyzer:
    """
    Handles only hard failure logic.
    Trailing/giveback logic must stay inside OptionsMomentumEngine
    because engine has current price + best_price context.
    """

    def check(self, state, spread):
        spread = max(float(spread), 0.05)

        # 1) Give first few seconds to breathe
        if state.seconds_in_trade < 3.0:
            return None

        # 2) Fail fast: after enough time, trade still did not expand
        if state.seconds_in_trade >= 5.0 and state.mfe < spread * 0.6:
            print(
                f"⚠️ FAIL_FAST_EXIT | "
                f"seconds_in_trade={state.seconds_in_trade:.2f} | "
                f"mfe={state.mfe:.3f} | spread={spread:.3f}"
            )
            return {"exit": True, "reason": "FAIL_FAST"}

        # 3) Hard adverse excursion
        if state.seconds_in_trade >= 3.0 and state.mae > spread * 2.0:
            print(
                f"⚠️ MAE_LIMIT_EXIT | "
                f"seconds_in_trade={state.seconds_in_trade:.2f} | "
                f"mae={state.mae:.3f} | spread={spread:.3f}"
            )
            return {"exit": True, "reason": "MAE_LIMIT"}

        # 4) Stayed weak below entry too long without proper expansion
        if state.seconds_below_entry > 8.0 and state.mfe < spread * 0.5:
            print(
                f"⚠️ NEGATIVE_DRIFT_EXIT | "
                f"seconds_below={state.seconds_below_entry:.2f} | "
                f"mfe={state.mfe:.3f} | spread={spread:.3f}"
            )
            return {"exit": True, "reason": "NEGATIVE_DRIFT"}

        return None
