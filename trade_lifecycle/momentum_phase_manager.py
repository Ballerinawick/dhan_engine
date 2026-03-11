class MomentumPhaseManager:

    """
    Momentum lifecycle controller.

    Prevents early exits during momentum discovery.
    """

    PROBE_SEC = 15
    EXPANSION_SEC = 120

    def get_phase(self, state):

        if state.seconds_in_trade < self.PROBE_SEC:
            return "PROBE"

        if state.seconds_in_trade < self.EXPANSION_SEC:
            return "EXPANSION"

        return "EXHAUSTION"

    def allow_failure_exit(self, state):

        phase = self.get_phase(state)

        if phase == "PROBE":
            return False

        return True

    def allow_turn_exit(self, state):

        phase = self.get_phase(state)

        if phase == "PROBE":
            return False

        return True

    def allow_trailing_exit(self, state):

        phase = self.get_phase(state)

        if phase in ("EXPANSION", "EXHAUSTION"):
            return True

        return False
