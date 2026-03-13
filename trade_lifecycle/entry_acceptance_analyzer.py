class EntryAcceptanceAnalyzer:
    """
    Determines whether the market is accepting the entry price.
    Must allow early discovery phase so momentum can develop.
    """

    DISCOVERY_SEC = 8.0
    MAX_BELOW_ENTRY = 10.0
    MAX_RETESTS = 4

    def evaluate(self, state):

        # --------------------------------------------
        # 1. Discovery phase — never reject early
        # --------------------------------------------
        if state.seconds_in_trade < self.DISCOVERY_SEC:
            return "NEUTRAL"

        # --------------------------------------------
        # 2. Too much time below entry (true rejection)
        # --------------------------------------------
        if state.seconds_below_entry > self.MAX_BELOW_ENTRY:
            print(
                f"⚠️ ENTRY_REJECT | reason=below_entry_time | "
                f"seconds_below={state.seconds_below_entry:.2f}"
            )
            return "REJECTED"

        # --------------------------------------------
        # 3. Too many structure retests
        # --------------------------------------------
        if state.retests > self.MAX_RETESTS:
            print(
                f"⚠️ ENTRY_REJECT | reason=too_many_retests | "
                f"retests={state.retests}"
            )
            return "REJECTED"

        # --------------------------------------------
        # 4. Market accepted entry
        # --------------------------------------------
        if state.accepted:
            return "ACCEPTED"

        return "NEUTRAL"