class EntryAcceptanceAnalyzer:
    def evaluate(self, state):
        if state.seconds_below_entry > 4.0:
            print(
                f"⚠️ ENTRY_REJECT | reason=below_entry_time | "
                f"seconds_below={state.seconds_below_entry:.2f}"
            )
            return "REJECTED"

        if state.retests > 3:
            print(
                f"⚠️ ENTRY_REJECT | reason=too_many_retests | "
                f"retests={state.retests}"
            )
            return "REJECTED"

        if state.accepted:
            return "ACCEPTED"

        return "NEUTRAL"