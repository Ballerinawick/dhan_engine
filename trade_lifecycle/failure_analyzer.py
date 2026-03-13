class FailureAnalyzer:
    """
    Handles only hard failure logic.
    Trailing/giveback logic must stay inside OptionsMomentumEngine
    because engine has current price + best_price context.
    """

    MIN_WIGGLE = 2.0        # allow normal option movement
    MIN_FAIL_TIME = 15.0     # allow discovery time

    def check(self, state, spread):
        spread = max(float(spread), 0.05)

        # ---------------------------------------------------
        # 1) Give initial breathing time
        # ---------------------------------------------------
        if state.seconds_in_trade < 4.0:
            return None

        # ---------------------------------------------------
        # 2) Fail fast if trade never expands
        # ---------------------------------------------------
        if state.seconds_in_trade >= self.MIN_FAIL_TIME and state.mfe < spread * 0.5:
            print(
                f"⚠️ FAIL_FAST_EXIT | "
                f"seconds_in_trade={state.seconds_in_trade:.2f} | "
                f"mfe={state.mfe:.3f} | spread={spread:.3f}"
            )
            return {"exit": True, "reason": "FAIL_FAST"}

        # ---------------------------------------------------
        # 3) Hard adverse excursion (true failure)
        # Allow realistic option wiggle
        # ---------------------------------------------------
        mae_limit = max(spread * 5.0, self.MIN_WIGGLE)

        if state.mae > mae_limit:
            print(
                f"⚠️ MAE_LIMIT_EXIT | "
                f"seconds_in_trade={state.seconds_in_trade:.2f} | "
                f"mae={state.mae:.3f} | "
                f"mae_limit={mae_limit:.3f} | "
                f"spread={spread:.3f}"
            )
            return {"exit": True, "reason": "MAE_LIMIT"}

        # ---------------------------------------------------
        # 4) Negative drift without expansion
        # ---------------------------------------------------
        if state.seconds_in_trade > 15.0 and state.seconds_below_entry > 10.0 and state.mfe < spread * 0.5:
            print(
                f"⚠️ NEGATIVE_DRIFT_EXIT | "
                f"seconds_below={state.seconds_below_entry:.2f} | "
                f"mfe={state.mfe:.3f} | spread={spread:.3f}"
            )
            return {"exit": True, "reason": "NEGATIVE_DRIFT"}

        return None
