class MomentumPhaseManager:
    """
    Momentum lifecycle controller.

    Prevents early exits during momentum discovery.
    """

    PROBE_SEC = 15
    EXPANSION_SEC = 180

    def get_phase(self, state):
        if state.seconds_in_trade < self.PROBE_SEC:
            return "PROBE"
        if state.seconds_in_trade < self.EXPANSION_SEC:
            return "EXPANSION"
        return "EXHAUSTION"

    def allow_failure_exit(self, state):
        """
        Failure exits must always be allowed.
        Capital protection cannot be blocked by phase gating.
        """
        return True

    def allow_turn_exit(self, state):
        """
        Allow turn exits once trade has demonstrated expansion.
        Prevent noise exits immediately after entry.
        """
        if state.seconds_in_trade < 3:
            return False

        if state.mfe <= 0:
            return False

        return True

    def allow_trailing_exit(self, state):
        """
        Trailing exit should activate once trade has expanded.
        Do NOT block trailing based on time phase.
        """
        if state.mfe > 0:
            return True
        return False

    def allow_acceptance_reject_exit(self, state):
        """
        If entry acceptance fails the trade must exit immediately.
        """
        return True
