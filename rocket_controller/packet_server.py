"""This module is responsible for receiving the incoming packets from the interceptor and returning a response."""

import datetime
from concurrent import futures
from typing import List

import grpc
from typeguard import TypeCheckError, check_type  # type: ignore

from protos import packet_pb2, packet_pb2_grpc
from protos.packet_pb2 import Packet
from rocket_controller.csv_logger import ActionLogger
from rocket_controller.encoder_decoder import PacketEncoderDecoder
from rocket_controller.helper import validate_ports_or_ids
from rocket_controller.strategies.strategy import Strategy
from rocket_controller.validator_node_info import (
    SocketAddress,
    ValidatorKeyData,
    ValidatorNode,
)

HOST = "localhost"


class PacketService(packet_pb2_grpc.PacketServiceServicer):
    """This class is responsible for receiving the incoming packets from the interceptor and returning a response."""

    def __init__(self, strategy: Strategy):
        """
        Constructor for the PacketService class.

        Args:
            strategy: The Strategy to use while serving packets.
        """
        self.strategy = strategy
        self.logger: ActionLogger | None = None

    def send_packet(
        self, request, context: grpc.ServicerContext
    ) -> packet_pb2.PacketAck:
        """
        This function receives the packet from the interceptor and passes it to the controller.

        Every action taken by the defined strategy will be logged in ../execution_logs.

        Args:
            request: Packet containing intercepted data.
            context: gRPC context.

        Returns:
            The possibly modified packet and an action.

        Raises:
            ValueError: If request.from_port == request.to_port or if any is negative.
        """
        timestamp = int(datetime.datetime.now().timestamp() * 1000)
        validate_ports_or_ids(request.from_port, request.to_port)

        (new_data, action, send_amount) = self.strategy.process_packet(request)

        if not self.strategy.keep_action_log:
            return packet_pb2.PacketAck(
                data=new_data, action=action, send_amount=send_amount
            )

        if not self.logger:
            raise RuntimeError("Logger was not initialized")

        original_packet_decoded = PacketEncoderDecoder.decode_packet(request)
        new_packet = Packet(
            data=new_data, from_port=request.from_port, to_port=request.to_port
        )
        new_packet_decoded = PacketEncoderDecoder.decode_packet(new_packet)

        self.logger.log_action(
            action=action,
            send_amount=send_amount,
            from_node_id=self.strategy.network.port_to_id(request.from_port),
            to_node_id=self.strategy.network.port_to_id(request.to_port),
            message_type=PacketEncoderDecoder.message_type_map[
                original_packet_decoded[1]
            ].__name__,
            original_data=original_packet_decoded[0].__str__().replace("\n", "; "),
            possibly_mutated_data=new_packet_decoded[0].__str__().replace("\n", "; "),
            custom_timestamp=timestamp,
            sent_timestamp=int(datetime.datetime.now().timestamp() * 1000),
        )

        return packet_pb2.PacketAck(
            data=new_data, action=action, send_amount=send_amount
        )

    def send_validator_node_info(
        self,
        request_iterator: List[packet_pb2.ValidatorNodeInfo],
        context: grpc.ServicerContext,
    ) -> packet_pb2.ValidatorNodeInfoAck:
        """
        This function receives the validator node info from the interceptor and passes it to the controller.

        Args:
            request_iterator: Iterator of validator node info.
            context: gRPC context.

        Returns:
            ValidatorNodeInfoAck: An acknowledgement.
        """
        validator_node_list: List[ValidatorNode] = []
        for request in request_iterator:
            validator_node_list.append(
                ValidatorNode(
                    name=request.name,
                    peer=SocketAddress(
                        host=HOST,
                        port=request.peer_port,
                    ),
                    ws_public=SocketAddress(
                        host=HOST,
                        port=request.ws_public_port,
                    ),
                    ws_admin=SocketAddress(
                        host=HOST,
                        port=request.ws_admin_port,
                    ),
                    rpc=SocketAddress(
                        host=HOST,
                        port=request.rpc_port,
                    ),
                    validator_key_data=ValidatorKeyData(
                        status=request.status,
                        validation_key=request.validation_key,
                        validation_private_key=request.validation_private_key,
                        validation_public_key=request.validation_public_key,
                        validation_seed=request.validation_seed,
                    ),
                )
            )
        self.strategy.update_network(validator_node_list)

        if self.strategy.keep_action_log:
            self.logger = ActionLogger(
                f"{self.strategy.iteration_type.get_log_dir()}/iteration-{self.strategy.iteration_type.cur_iteration}",
                validator_node_list,
                f"action-{self.strategy.iteration_type.cur_iteration}",
                f"node_info-{self.strategy.iteration_type.cur_iteration}",
            )

        return packet_pb2.ValidatorNodeInfoAck(status="Received validator node info")

    def get_config(self, request, context):
        """
        This function sends the network config specified in the self.strategy field to the interceptor.

        Args:
            request: The request containing the network config.
            context: gRPC context.

        Returns:
            Config: The Config object.

        Raises:
            ValueError: If an expected field is not set in the config file.
            TypeError: If the type of one of the fields in the config file does not match the expected type.
        """
        config = self.strategy.network.network_config

        config_values_types = {
            "prefix": str,
            "network_partition": List[List[int]],
            "base_port_peer": int,
            "base_port_ws": int,
            "base_port_ws_admin": int,
            "base_port_rpc": int,
            "number_of_nodes": int,
        }

        for name, name_type in config_values_types.items():
            value = config.get(name)
            if value is None:
                raise ValueError(f"{name} was not specified in the network config")
            try:
                check_type(value, name_type)
            except TypeCheckError as err:
                raise (
                    TypeError(
                        f"The type for {name} in network config should be {name_type.__name__} but was {type(value).__name__}"
                    )
                ) from err

        net_partitions = map(
            lambda x: packet_pb2.Partition(nodes=x),
            config.get("network_partition"),
        )

        unl_partitions = map(
            lambda x: packet_pb2.Partition(nodes=x),
            config.get("unl_partition"),
        )

        return packet_pb2.Config(
            prefix=config.get("prefix"),
            base_port_peer=config.get("base_port_peer"),
            base_port_ws=config.get("base_port_ws"),
            base_port_ws_admin=config.get("base_port_ws_admin"),
            base_port_rpc=config.get("base_port_rpc"),
            number_of_nodes=config.get("number_of_nodes"),
            net_partitions=net_partitions,
            unl_partitions=unl_partitions,
        )


def serve(strategy: Strategy):
    """This function starts the server and listens for incoming packets."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1000))
    # TODO: should be unlimited messages in the queue which could be done by removing max_workers=10
    # However, this is still limited by the number of cores in your computer
    # And defaults to max_workers = min(32, (os.cpu_count() or 1) + 4) by ThreadPoolExecutor
    # How many messages should we expect to receive at once? The number of number of messages
    # buffered in the queue is deterimined by the dispatch interval
    packet_pb2_grpc.add_PacketServiceServicer_to_server(PacketService(strategy), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    strategy.iteration_type.set_server(server)
    strategy.iteration_type.set_network(strategy.network)
    strategy.iteration_type.add_iteration()

    return server
