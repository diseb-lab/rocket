"""This module is responsible for defining the Strategy interface."""

from abc import ABC, abstractmethod
from typing import Tuple, List, Dict
from xrpl_controller.core import flatten
from xrpl_controller.validator_node_info import ValidatorNode

MAX_U32 = 2**32 - 1


class Strategy(ABC):
    """Class that defines the Strategy interface."""

    def __init__(self, auto_partition: bool = True):
        """
        Initialize the Strategy interface with needed attributes.

        Args:
            auto_partition (bool, optional): Whether the strategy will auto-apply network partitions. Defaults to True.
        """
        self.validator_node_list: List[ValidatorNode] = []
        self.node_amount: int = 0
        self.network_partitions: list[list[int]] = []
        self.port_dict: Dict[int, int] = {}
        self.communication_matrix: list[list[bool]] = []
        self.auto_partition = auto_partition

    def partition_network(self, partitions: list[list[int]]):
        """
        Set the network partition and update the communication matrix.

        Args:
            partitions (list[list[int]]): List containing the network partitions (as lists of port numbers).
        """
        flattened_partitions = flatten(partitions)
        if (
            set(flattened_partitions) != set(flatten(self.network_partitions))
            or len(flattened_partitions) != self.node_amount
        ):
            raise ValueError(
                "The given network partition is not valid for the current network."
            )

        self.network_partitions = partitions
        self.communication_matrix = [
            [False for _ in range(self.node_amount)] for _ in range(self.node_amount)
        ]

        for partition in partitions:
            for i in partition:
                for j in partition:
                    self.communication_matrix[self.port_dict[i]][self.port_dict[j]] = (
                        True
                    )

    def apply_network_partition(
        self, action: int, peer_from_port: int, peer_to_port: int
    ):
        """
        Apply the network partition to an action with its related ports.

        Args:
            action (int): The action to apply the network partition on.
            peer_from_port (int): The peer port from where the message was sent.
            peer_to_port (int): The peer port to where the message was sent.
        """
        return (
            action
            if self.communication_matrix[self.port_dict[peer_from_port]][
                self.port_dict[peer_to_port]
            ]
            else MAX_U32
        )

    def update_network(self, validator_node_list: List[ValidatorNode]):
        """
        Update the strategy's attributes.

        Args:
            validator_node_list (list[ValidatorNode]): The list with (new) validator node information
        """
        print("Updating the strategy's network information")
        self.validator_node_list = validator_node_list
        self.node_amount = len(validator_node_list)

        # TODO: node.rpc.port should become node.peer.port when functionality for this becomes available
        self.network_partitions = [[node.rpc.port for node in validator_node_list]]
        self.port_dict = {
            port: index for index, port in enumerate(self.network_partitions[0])
        }
        self.communication_matrix = [
            [True for _ in range(self.node_amount)] for _ in range(self.node_amount)
        ]

    @abstractmethod
    def handle_packet(self, packet: bytes) -> Tuple[bytes, int]:
        """
        This method is responsible for returning a possibly mutated packet and an action.

        Args:
            packet: the original packet.

        Returns:
        Tuple[bytes, int]: the new packet and the action.
            action 0: send immediately without delay
            action MAX: drop the packet
            action 0<x<MAX: delay the packet x ms
        """
        pass
