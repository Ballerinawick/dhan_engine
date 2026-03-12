class MomentumPhaseManager:
    """
    Momentum lifecycle controller.

    Prevents early exits during momentum discovery.
    """

    PROBE_SEC = 180
    EXPANSION_SEC = 600

    def get_phase(self, state):
        if state.seconds_in_trade < self.PROBE_SEC:
            return "PROBE"
        if state.seconds_in_trade < self.EXPANSION_SEC:
            return "EXPANSION"
        return "EXHAUSTION"

    def allow_failure_exit(self, state):
        return self.get_phase(state) != "PROBE"

    def allow_turn_exit(self, state):
        return self.get_phase(state) != "PROBE"

    def allow_trailing_exit(self, state):
        return self.get_phase(state) in ("EXPANSION", "EXHAUSTION")

    def allow_acceptance_reject_exit(self, state):
        return self.get_phase(state) != "PROBE"
