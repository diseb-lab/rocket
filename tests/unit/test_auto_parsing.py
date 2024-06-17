"""Test the functionality which automatically parses identical subsequent messages."""

import pytest

from protos import packet_pb2
from tests.unit.test_strategy import node_0, node_1, node_2, node_3
from xrpl_controller.strategies import RandomFuzzer

# Ports of the imported nodes are 10, 11, 12, 13 respectively


def test_auto_parsing():
    """Test the automatic parsing of identical subsequent messages."""
    strategy = RandomFuzzer()
    strategy.update_network([node_0, node_1, node_2])
    strategy.set_message_action(0, 1, b"test", b"mutated", 42)
    res = strategy.check_previous_message(0, 1, b"notest")
    assert not res[0]
    assert res[1] == (b"mutated", 42)

    res2 = strategy.check_previous_message(0, 1, b"test")
    assert res2[0]
    assert res2[1] == (b"mutated", 42)

    res3 = strategy.check_previous_message(0, 2, b"test")
    assert not res3[0]
    assert res3[1] == (b"", -1)

    with pytest.raises(ValueError):
        strategy.set_message_action(0, 0, b"test", b"mutated", 42)


def test_auto_parsing_false():
    """Test whether attributes do not get saved when boolean is false."""
    strategy = RandomFuzzer(auto_parse_identical=False, auto_parse_subsets=False)

    strategy.update_network([node_0, node_1, node_2])

    with pytest.raises(ValueError):
        strategy.set_message_action(0, 1, b"test", b"mutated", 42)

    with pytest.raises(ValueError):
        strategy.check_previous_message(0, 1, b"test")

    packet_ack = packet_pb2.Packet(data=b"test", from_port=10, to_port=11)
    strategy.process_packet(packet_ack)


def test_auto_parsing_subsets():
    """Test auto parsing subsets functionality."""
    strategy = RandomFuzzer()
    strategy.update_network([node_0, node_1, node_2])

    # Node 2 will sends same messages to node 0 and node 1 if possible
    strategy.set_subsets_dict({2: [0, 1]})
    assert strategy.subsets_dict == {0: [], 1: [], 2: [0, 1]}
    strategy.set_message_action(2, 0, b"test", b"mutated", 42)
    assert strategy.check_subsets(2, 1, b"test") == (True, (b"mutated", 42))
    assert strategy.check_subsets(2, 1, b"test2") == (False, (b"mutated", 42))

    # Entry is now wrapped in another list
    strategy.set_subsets_dict({2: [[0, 1]]})
    assert strategy.subsets_dict == {0: [], 1: [], 2: [[0, 1]]}
    strategy.set_message_action(2, 0, b"test2", b"mutated", 42)
    assert strategy.check_subsets(2, 1, b"test2") == (True, (b"mutated", 42))
    assert strategy.check_subsets(2, 1, b"testF") == (False, (b"mutated", 42))

    packet_ack = packet_pb2.Packet(data=b"test", from_port=10, to_port=11)
    strategy.process_packet(packet_ack)


def test_auto_parsing_subsets_4_nodes():
    """Test edge cases where there are multiple subsets to be checked."""
    strategy = RandomFuzzer()
    strategy.update_network([node_0, node_1, node_2, node_3])
    strategy.set_subsets_dict({2: [[0], [1, 3]]})
    assert strategy.subsets_dict == {0: [], 1: [], 2: [[0], [1, 3]], 3: []}

    strategy.set_message_action(2, 0, b"test", b"mutated", 42)
    strategy.set_message_action(2, 1, b"test", b"mutated2", 42)
    assert strategy.check_subsets(2, 3, b"test") == (True, (b"mutated2", 42))
