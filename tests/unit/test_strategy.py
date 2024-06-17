"""Tests for Strategy class."""

from encodings.utf_8 import encode
from unittest.mock import MagicMock

from protos import packet_pb2, ripple_pb2
from tests.unit.test_random_fuzzer import create_test_config, remove_test_config
from xrpl_controller.core import MAX_U32
from xrpl_controller.iteration_type import LedgerBasedIteration, TimeBasedIteration
from xrpl_controller.strategies import RandomFuzzer
from xrpl_controller.strategies.encoder_decoder import PacketEncoderDecoder
from xrpl_controller.validator_node_info import (
    SocketAddress,
    ValidatorKeyData,
    ValidatorNode,
)

node_0 = ValidatorNode(
    SocketAddress("test_peer", 10),
    SocketAddress("test-ws-pub", 20),
    SocketAddress("test-ws-adm", 30),
    SocketAddress("test-rpc", 40),
    ValidatorKeyData("status", "key", "K3Y", "PUB", "T3ST"),
)

node_1 = ValidatorNode(
    SocketAddress("test_peer", 11),
    SocketAddress("test-ws-pub", 21),
    SocketAddress("test-ws-adm", 31),
    SocketAddress("test-rpc", 41),
    ValidatorKeyData("status", "key", "K3Y", "PUB", "T3ST"),
)

node_2 = ValidatorNode(
    SocketAddress("test_peer", 12),
    SocketAddress("test-ws-pub", 22),
    SocketAddress("test-ws-adm", 32),
    SocketAddress("test-rpc", 42),
    ValidatorKeyData("status", "key", "K3Y", "PUB", "T3ST"),
)

status_msg = ripple_pb2.TMStatusChange(
    newStatus=2,
    newEvent=1,
    ledgerSeq=3,
    ledgerHash=b"abcdef",
    ledgerHashPrevious=b"123456",
    networkTime=1000,
    firstSeq=0,
    lastSeq=2,
)

ping_message = ripple_pb2.TMPing(type=0, seq=1, pingTime=9000, netTime=8999)


def test_init():
    """Test whether Strategy attributes get initialized correctly."""
    strategy = RandomFuzzer()
    assert strategy.validator_node_list == []
    assert strategy.public_to_private_key_map == {}
    assert strategy.node_amount == 0
    assert strategy.port_dict == {}
    assert strategy.communication_matrix == []
    assert strategy.auto_partition
    assert strategy.auto_parse_identical
    assert strategy.prev_message_action_matrix == []
    assert strategy.keep_action_log


def test_update_network():
    """Test whether Strategy attributes get updated correctly with update_network function."""
    time_iter = TimeBasedIteration(5, 30)
    time_iter.start_timer = MagicMock()

    strategy = RandomFuzzer(iteration_type=time_iter)
    strategy.update_network([node_0, node_1, node_2])
    assert strategy.validator_node_list == [node_0, node_1, node_2]
    assert strategy.node_amount == 3
    assert strategy.port_dict == {10: 0, 11: 1, 12: 2}
    assert strategy.communication_matrix == [
        [False, True, True],
        [True, False, True],
        [True, True, False],
    ]

    assert len(strategy.prev_message_action_matrix) == 3
    for row in strategy.prev_message_action_matrix:
        assert len(row) == 3
        for item in row:
            assert item.initial_message == b""
            assert item.final_message == b""
            assert item.action == -1

    time_iter.start_timer.assert_called_once()


def test_process_message():
    """Test for process_message function."""
    create_test_config("TEST_PROCESS_MESSAGE", 0, 0.6, 10, 150, 10)
    strategy = RandomFuzzer(strategy_config_file="TEST_PROCESS_MESSAGE")
    strategy.update_network([node_0, node_1, node_2])
    packet_ack = packet_pb2.Packet(data=b"testtest", from_port=10, to_port=11)
    assert strategy.process_packet(packet_ack) == (b"testtest", 119)

    # Check whether action differs from previous one, could be flaky, but we used a seed
    packet_ack = packet_pb2.Packet(data=b"testtest2", from_port=10, to_port=11)
    assert strategy.process_packet(packet_ack) == (b"testtest2", 13)

    # Check whether set_message gets modified
    assert strategy.prev_message_action_matrix[0][1].initial_message == b"testtest2"
    assert strategy.prev_message_action_matrix[0][1].action == 13
    assert strategy.prev_message_action_matrix[0][1].final_message == b"testtest2"

    # Check whether messages get dropped automatically through auto partition
    strategy.partition_network([[10, 11], [12]])
    for i in range(100):
        msg = encode("testtest" + str(i))[0]  # Just arbitrary encoding
        packet_ack = packet_pb2.Packet(data=msg, from_port=10, to_port=12)
        assert strategy.process_packet(packet_ack) == (msg, MAX_U32)

    # Check whether result will always stay the same with auto parse identical messages
    packet_ack = packet_pb2.Packet(data=b"testtest", from_port=10, to_port=12)
    result = strategy.process_packet(packet_ack)
    for _ in range(100):
        packet_ack = packet_pb2.Packet(data=b"testtest", from_port=10, to_port=12)
        assert strategy.process_packet(packet_ack) == result

    remove_test_config("TEST_PROCESS_MESSAGE")


def test_process_message_type_34():
    """Test whether processing a packet of type 34 (StatusChange) calls the correct methods."""
    ledger_iter = LedgerBasedIteration(5, 10)
    ledger_iter.update_iteration = MagicMock()

    strategy = RandomFuzzer(iteration_type=ledger_iter)
    strategy.update_network([node_0, node_1, node_2])
    # strategy.update_status = MagicMock()

    packet_data = PacketEncoderDecoder.encode_message(status_msg, 34)
    packet = packet_pb2.Packet(from_port=10, to_port=11, data=packet_data)
    strategy.process_packet(packet)
    ledger_iter.update_iteration.assert_called_once()


def test_negative_process_message_type_34():
    """Test whether processing a packet of type 34 (StatusChange), with time iteration should not call method."""
    time_iter = TimeBasedIteration(5, 30)
    time_iter.start_timer = MagicMock()
    time_iter.update_iteration = MagicMock()

    strategy = RandomFuzzer(iteration_type=time_iter)
    strategy.update_network([node_0, node_1, node_2])
    # strategy.update_status = MagicMock()

    packet_data = PacketEncoderDecoder.encode_message(status_msg, 34)
    packet = packet_pb2.Packet(from_port=10, to_port=11, data=packet_data)
    strategy.process_packet(packet)
    time_iter.update_iteration.assert_not_called()


def test_process_message_type_not_supported():
    """Test whether processing a packet of type 60, does not call update_iteration."""
    ledger_iter = LedgerBasedIteration(5, 10)
    ledger_iter.update_iteration = MagicMock()

    strategy = RandomFuzzer(iteration_type=ledger_iter)
    strategy.update_network([node_0, node_1, node_2])
    strategy.update_status = MagicMock()

    packet_data = PacketEncoderDecoder.encode_message(ping_message, 60)
    packet = packet_pb2.Packet(from_port=10, to_port=11, data=packet_data)

    strategy.process_packet(packet)
    strategy.update_status.assert_not_called()
