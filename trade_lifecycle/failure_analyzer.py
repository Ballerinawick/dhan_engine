class FailureAnalyzer:

    def check(self, state, spread):

        # ------------------------------------------------
        # 1️⃣ FAIL FAST
        # Trade did not move at all after entry
        # ------------------------------------------------
        if state.seconds_in_trade > 5 and state.mfe < spread * 0.6:
            print("⚠️ FAIL_FAST_EXIT")
            return {"exit": True, "reason": "FAIL_FAST"}


        # ------------------------------------------------
        # 2️⃣ EARLY BREATHING ROOM
        # Give trade time to stabilize
        # ------------------------------------------------
        if state.seconds_in_trade < 3:
            return None


        # ------------------------------------------------
        # 3️⃣ MAE PROTECTION
        # Only exit if loss becomes dangerous
        # ------------------------------------------------
        if state.mae > spread * 2.0 and state.seconds_in_trade > 3:
            print("⚠️ MAE_LIMIT_EXIT")
            return {"exit": True, "reason": "MAE_LIMIT"}


        # ------------------------------------------------
        # 4️⃣ NEGATIVE DRIFT
        # Price stays below entry too long
        # ------------------------------------------------
        if state.seconds_below_entry > 8 and state.mfe < spread * 0.5:
            print("⚠️ NEGATIVE_DRIFT_EXIT")
            return {"exit": True, "reason": "NEGATIVE_DRIFT"}


        # ------------------------------------------------
        # 5️⃣ MFE PULLBACK PROTECTION
        # If trade moved well but retraced heavily
        # ------------------------------------------------
        if state.mfe > spread * 2:
            giveback = state.mfe - (state.mfe - state.mae)

            if giveback > state.mfe * 0.7:
                print("⚠️ MFE_GIVEBACK_EXIT")
                return {"exit": True, "reason": "MFE_GIVEBACK"}


        return None