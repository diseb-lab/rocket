"""Tests for network partitions functionality."""

from tests.unit.test_strategy import node_0, node_1, node_2
from xrpl_controller.network_manager import NetworkManager

configs = (
    {
        "base_port_peer": 60000,
        "base_port_ws": 61000,
        "base_port_ws_admin": 62000,
        "base_port_rpc": 63000,
        "number_of_nodes": 3,
        "network_partition": [[0, 1, 2]],
    },
    {
        "delay_probability": 0.6,
        "drop_probability": 0,
        "min_delay_ms": 10,
        "max_delay_ms": 150,
        "seed": 10,
    },
)
# Ports of the imported nodes are 10, 11, 12 respectively


def test_custom_connections():
    """Test whether Strategy attributes get updated correctly when connect_nodes is called."""
    network = NetworkManager()

    network.update_network([node_0, node_1, node_2])
    network.disconnect_nodes(0, 1)
    assert network.communication_matrix == [
        [False, False, True],
        [False, False, True],
        [True, True, False],
    ]

    assert not network.check_communication(0, 1)
    assert not network.check_communication(1, 0)

    network.connect_nodes(0, 1)
    assert network.communication_matrix == [
        [False, True, True],
        [True, False, True],
        [True, True, False],
    ]

    assert network.check_communication(0, 1)
    assert network.check_communication(1, 0)

    try:
        network.check_communication(0, 0)
        raise AssertionError()
    except ValueError:
        pass

    try:
        network.disconnect_nodes(0, 0)
        raise AssertionError()
    except ValueError:
        pass

    try:
        network.connect_nodes(1, 1)
        raise AssertionError()
    except ValueError:
        pass


def test_reset_communications():
    """Test whether Strategy attributes resets communication matrix correctly when reset_communications is called."""
    network = NetworkManager()
    network.update_network([node_0, node_1, node_2])
    network.partition_network([[0], [1, 2]])
    network.reset_communications()
    assert network.communication_matrix == [
        [False, True, True],
        [True, False, True],
        [True, True, False],
    ]


def test_partition_network_0():
    """Test whether Strategy attributes get updated correctly when partition_network is called. Formation 0."""
    network = NetworkManager()
    network.update_network([node_0, node_1, node_2])
    network.partition_network([[0], [1, 2]])
    assert network.communication_matrix == [
        [False, False, False],
        [False, False, True],
        [False, True, False],
    ]


def test_partition_network_1():
    """Test whether Strategy attributes get updated correctly when partition_network is called. Formation 1."""
    network = NetworkManager()

    network.update_network([node_0, node_1, node_2])
    network.partition_network([[0, 1, 2]])
    assert network.communication_matrix == [
        [False, True, True],
        [True, False, True],
        [True, True, False],
    ]


def test_partition_network_2():
    """Test whether Strategy attributes get updated correctly when partition_network is called."""
    network = NetworkManager()

    network.update_network([node_0, node_1, node_2])
    network.partition_network([[0], [1], [2]])
    assert network.communication_matrix == [
        [False, False, False],
        [False, False, False],
        [False, False, False],
    ]


def test_partition_network_invalid_partitions():
    """Test whether invalid partitions get rejected. Missing port."""
    network = NetworkManager()
    network.update_network([node_0, node_1, node_2])
    try:
        network.partition_network([[0], [2]])
        raise AssertionError()
    except ValueError:
        pass


def test_partition_network_invalid_amount():
    """Test whether invalid partitions get rejected. Duplicated port."""
    network = NetworkManager()
    network.update_network([node_0, node_1, node_2])
    try:
        network.partition_network([[0], [1, 1, 2]])
        raise AssertionError()
    except ValueError:
        pass


def test_apply_partition():
    """Test whether partitions get applied correctly."""
    network = NetworkManager()
    network.update_network([node_0, node_1, node_2])
    network.partition_network([[0, 1], [2]])
    assert network.check_communication(0, 1)
    assert network.check_communication(0, 1)
    assert not network.check_communication(0, 2)
    assert not network.check_communication(2, 0)
    assert not network.check_communication(1, 2)

    # Test whether exception gets raised when ports are equal
    try:
        assert not network.check_communication(2, 2)
        raise AssertionError()
    except ValueError:
        pass

    assert network.check_communication(1, 0)
