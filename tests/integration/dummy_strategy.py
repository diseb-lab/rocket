"""This module contains a dummy strategy. It is used for testing purposes."""

from rocket_controller.strategies.strategy import Strategy


class DummyStrategy(Strategy):
    """
    Dummy strategy to be used for tests.

    It disables the available automatic processes in strategy as well as initializing the strategy with a NoneIteration.
    """

    def __init__(self, iteration_type):
        """Initialize the dummy strategy."""
        super().__init__(
            auto_partition=False,
            auto_parse_identical=False,
            auto_parse_subsets=False,
            keep_action_log=False,
            iteration_type=iteration_type,
        )

    def handle_packet(self, packet):
        """Return the packet as is."""
        return packet.data, 0, 1

    def setup(self):
        """Ignore setup."""
        pass
