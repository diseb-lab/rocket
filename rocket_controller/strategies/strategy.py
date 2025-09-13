"""This module is responsible for defining the Strategy interface."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Tuple

from loguru import logger

from protos import packet_pb2, ripple_pb2
from rocket_controller.encoder_decoder import (
    DecodingNotSupportedError,
    PacketEncoderDecoder,
)
from rocket_controller.helper import (
    MAX_U32,
    format_datetime,
    yaml_to_dict,
)
from rocket_controller.iteration_type import LedgerBasedIteration, TimeBasedIteration
from rocket_controller.network_manager import NetworkManager
from rocket_controller.validator_node_info import ValidatorNode


class Strategy(ABC):
    """Class that defines the Strategy interface."""

    def __init__(
        self,
        network_config_path: str | None = None,
        strategy_config_path: str | None = None,
        auto_partition: bool = True,
        auto_parse_identical: bool = True,
        auto_parse_subsets: bool = True,
        keep_action_log: bool = True,
        iteration_type: TimeBasedIteration | None = None,
        log_dir: str | None = None,
        network_overrides: Dict[str, Any] | None = None,
        strategy_overrides: Dict[str, Any] | None = None,
    ):
        """
        Initialize the Strategy interface with necessary fields.

        Args:
            network_config_path (str, optional): The path of a network configuration file
            strategy_config_path (str, optional): The path of the strategy configuration file
            auto_partition (bool, optional): Whether the strategy will auto-apply network partitions.
            auto_parse_identical (bool, optional): Whether the strategy will perform same actions on identical messages.
            auto_parse_subsets (bool, optional): Whether the strategy will perform same actions on defined subsets.
            keep_action_log (bool, optional): Whether the strategy will keep an action log. Defaults to True.
            iteration_type (TimeBasedIteration, optional): Type of iteration logic to use.
            network_overrides (dict, optional): A dictionary containing parameter names and values which override the network config.
            strategy_overrides (dict, optional): A dictionary containing parameter names and values which override the strategy config.
        """
        if strategy_config_path is None:
            strategy_config_path = f"./config/default_{self.__class__.__name__}.yaml"
        if network_config_path is None:
            network_config_path = "./config/network/default_network.yaml"

        self.network = NetworkManager(
            auto_parse_identical=auto_parse_identical,
            auto_parse_subsets=auto_parse_subsets,
        )
        self.auto_partition: bool = auto_partition
        self.auto_parse_identical = auto_parse_identical
        self.auto_parse_subsets = auto_parse_subsets
        self.keep_action_log = keep_action_log
        self.network.network_config, self.params = self.init_configs(
            network_config_path, strategy_config_path
        )

        if network_overrides:
            for parameter_name in network_overrides:
                self.network.network_config[parameter_name] = network_overrides[
                    parameter_name
                ]
        if strategy_overrides:
            for parameter_name in strategy_overrides:
                self.params[parameter_name] = type(self.params[parameter_name])(
                    strategy_overrides[parameter_name]
                )

        logger.debug(f"Initialized final strategy parameters:" f"\n\t{self.params}")
        logger.debug(
            f"Initialized final strategy network configuration:"
            f"\n\t{self.network.network_config}"
        )

        self.start_datetime: datetime = datetime.now()
        self.iteration_type = (
            LedgerBasedIteration(
                self.params.get("iterations", 10),
                self.params.get("max_ledger_seq", 10),
                self.params.get("timeout", 65),
            )
            if iteration_type is None
            else iteration_type
        )
        if log_dir is not None:
            self.iteration_type.set_log_dir(log_dir)
        else:
            self.iteration_type.set_log_dir(format_datetime(self.start_datetime))

    @staticmethod
    def init_configs(
        network_config_path: str, strategy_config_path: str
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Initialize the strategy and network configuration from the given paths.

        Args:
            network_config_path: Path of the network config file.
            strategy_config_path: Path of the strategy config path.

        Returns:
            Tuple[Dict[str, Any], Dict[str, Any]]: Tuple containing the config files transformed to dictionaries.
        """
        params = yaml_to_dict(strategy_config_path)
        network_config = yaml_to_dict(network_config_path)
        return network_config, params

    def update_network(self, validator_node_list: List[ValidatorNode]):
        """
        Update the strategy's attributes.

        Args:
            validator_node_list (list[ValidatorNode]): The list with (new) validator node information.
        """
        logger.info("Updating the strategy's network information")
        self.network.update_network(validator_node_list)
        self.iteration_type.set_validator_nodes(validator_node_list)
        self.setup()

    def update_status(self, packet: packet_pb2.Packet):
        """
        Update the iteration's state variables, when a new TMStatusChange or TMProposeSet is received.

        Args:
            packet: The packet to check for a possible status update.
        """
        try:
            message, _ = PacketEncoderDecoder.decode_packet(packet)
            if isinstance(message, ripple_pb2.TMStatusChange):
                self.iteration_type.on_status_change(
                    message,
                    self.network.port_to_id(packet.from_port),
                    self.network.port_to_id(packet.to_port),
                )
            elif isinstance(message, ripple_pb2.TMTransaction):
                self.iteration_type.on_transaction(
                    message,
                    self.network.port_to_id(packet.from_port),
                    self.network.port_to_id(packet.to_port),
                )
        except DecodingNotSupportedError:
            pass

    def process_packet(
        self,
        packet: packet_pb2.Packet,
    ) -> Tuple[bytes, int, int]:
        """
        Process an incoming packet, applies automatic processes if applicable.

        Args:
            packet: The packet to process.

        Returns:
            Tuple[bytes, int, int]: The processed packet as bytes, the action and the send amount.
        """
        peer_from_id = self.network.port_to_id(packet.from_port)
        peer_to_id = self.network.port_to_id(packet.to_port)

        # Check for identical previous messages or for identical messages within broadcasts.
        # This uses booleans to check whether the functionality has to be applied automatically.
        # First check whether we want to automatically parse re-sent messages,
        # then we check whether we want to perform identical actions for defined subsets of processes/peers.
        if (
            self.auto_parse_identical
            and (
                result := self.network.check_previous_message(
                    peer_from_id, peer_to_id, packet.data
                )
            )[0]
        ) or (
            self.auto_parse_subsets
            and (
                result := self.network.check_subsets(
                    peer_from_id, peer_to_id, packet.data
                )
            )[0]
        ):
            # If result[0] is True, then result[1] will contain usable data
            (final_data, action) = result[1]
            send_amount = 1

        # Handle the packet regularly
        else:
            # If no communication is allowed by partitions, then we drop immediately
            if self.auto_partition and not self.network.check_communication(
                peer_from_id, peer_to_id
            ):
                (final_data, action, send_amount) = (packet.data, MAX_U32, 1)
            else:
                (final_data, action, send_amount) = self.handle_packet(packet)

            # This is needed to keep track of previously sent messages
            if self.auto_parse_identical or self.auto_parse_subsets:
                self.network.set_message_action(
                    peer_from_id, peer_to_id, packet.data, final_data, action
                )

        self.update_status(packet)
        return final_data, action, send_amount

    @abstractmethod
    def setup(self):  # pragma: no cover
        """
        Setup method to be implemented by implementations of Strategy, not required.

        This method gets called at the end of update_network to initialize starting values.
        """
        pass

    @abstractmethod
    def handle_packet(
        self, packet: packet_pb2.Packet
    ) -> Tuple[bytes, int, int]:  # pragma: no cover
        """
        This method is responsible for returning a possibly mutated packet and an action.

        Args:
            packet: The original packet.

        Returns:
            Tuple[bytes, int]: The new packet, action and send amount..
        """
        pass
