import hashlib
import random
import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from queue import Empty, Queue
from typing import Callable, Tuple

from loguru import logger
from xrpl.clients import JsonRpcClient
from xrpl.core.binarycodec import decode

from protos import packet_pb2, ripple_pb2
from rocket_controller.csv_logger import StateCoverageLogger
from rocket_controller.encoder_decoder import (
    DecodingNotSupportedError,
    PacketEncoderDecoder,
)
from rocket_controller.helper import MAX_U32
from rocket_controller.strategies.byzzql_agent import ByzzQLAgent
from rocket_controller.strategies.strategy import Strategy

# TODO: check how we learn from the RL agent, how often we update the Q-values. Are most values 0.0 or not?


def generate_random_partition(node_count: int) -> list[set[int]]:
    nodes = list(range(node_count))
    partition = []
    while nodes:
        subset_size = random.randint(1, len(nodes))
        subset = set(random.sample(nodes, subset_size))
        partition.append(subset)
        nodes = [node for node in nodes if node not in subset]
    return partition


def generate_random_subset(node_count: int) -> set[int]:
    # uniformly choose a subset of nodes
    nodes = list(range(node_count))
    return set(node for node in nodes if random.choice([True, False]))


class ByzzQLStrategy(Strategy):
    def __init__(
        self,
        network_config_path: str | None = None,
        strategy_config_path: str | None = None,
        auto_partition: bool = True,
        auto_parse_identical: bool = True,
        auto_parse_subsets: bool = True,
        keep_action_log: bool = True,
        log_dir: str | None = None,
        network_overrides=None,
        strategy_overrides=None,
    ):
        super().__init__(
            network_config_path,
            strategy_config_path,
            auto_partition,
            auto_parse_identical,
            auto_parse_subsets,
            keep_action_log,
            None,
            log_dir,
            network_overrides,
            strategy_overrides,
        )
        self.node_amount = self.network.network_config["number_of_nodes"]
        self.running = True
        self.dispatch_thread = threading.Thread(target=self.dispatch_loop, daemon=True)

        # Initialize RL Agent and State Abstraction
        if self.params["abstraction"] == "MessageInbox":
            self.state = MessageInboxAbstraction(
                log_dir=self.iteration_type.get_log_dir() or "missing_dir",
                strategy=self,
            )
        elif self.params["abstraction"] == "Trace":
            self.state = TraceAbstraction(
                log_dir=self.iteration_type.get_log_dir() or "missing_dir",
                strategy=self,
            )
        else:
            raise ValueError(f"Invalid abstraction type: {self.params['abstraction']}")

        # Uncomment to silence the debug printouts
        # logger.remove()
        # logger.add(sys.stderr, level="INFO")  # or "WARNING" to silence info logs too
        self.clients = {}

        # initialize process faults (mutations)
        self.iteration_type.register_callback(self.init_faults)
        self.iteration_type.register_after_iteration_callback(
            lambda: self.state.log_state(self.iteration_type.cur_iteration)
        )

        self.old_proposals = []
        self.old_validations = []

    def init_faults(self) -> None:
        self.process_faults_list = []

        # For each round generate a random receiver subset and seed
        # this ensures that mutations are deterministic for each test run
        for round_number in range(1, self.iteration_type._max_ledger_seq + 1):
            # Choose a random subset of receivers uniformly from all possible subsets
            receiver_subset = generate_random_subset(self.node_amount)
            mutation_seed = random.randint(0, 1000000)
            self.process_faults_list.append(
                (round_number, receiver_subset, mutation_seed)
            )

        logger.debug("Process faults (round, receiver subset, and seed):")
        for round_num, subset, seed in self.process_faults_list:
            logger.debug(f"Round {round_num}: receivers={subset}, seed={seed}")

    def setup(self):
        if self.dispatch_thread.is_alive():
            logger.info("Joining dispatch thread")
            self.running = False
            self.dispatch_thread.join()
            logger.info("Dispatch thread joined")
        self.state.reset()
        self.running = True
        self.dispatch_thread = threading.Thread(target=self.dispatch_loop, daemon=True)
        self.dispatch_thread.start()

        for peer_id in range(self.network.node_amount):
            validator = self.network.validator_node_list[peer_id]
            rpc_address = f"http://{validator.rpc.as_url()}/"
            self.clients[peer_id] = JsonRpcClient(rpc_address)

    def handle_packet(self, packet: packet_pb2.Packet) -> Tuple[bytes, int, int]:
        """Store message in queue and wait for dispatch"""

        try:
            message, message_type_no = PacketEncoderDecoder.decode_packet(packet)
        except DecodingNotSupportedError:
            logger.error("Decoding of message is not supported")
            return packet.data, 0, 1

        self.save_packet_for_mutation(message)

        # only process certain message types: TMValildation, TMProposeSet, TMTransaction
        if not (
            isinstance(message, ripple_pb2.TMValidation)
            or isinstance(message, ripple_pb2.TMProposeSet)
            or isinstance(message, ripple_pb2.TMTransaction)
        ):
            return packet.data, 0, 1

        # Get receiving node's current round number
        current_round = self.get_current_round_of_node(
            self.network.port_to_id(packet.to_port)
        )

        # Get string representation of message to use in the state abstraction
        extracted_value = self.state.extract_message(message, packet, current_round)

        event = threading.Event()
        start_time = time.time()
        result_container = [packet.data, 0, 1]  # default: deliver

        self.state.get_queue().put((packet, event, extracted_value, result_container))
        queue_size = self.state.get_queue().qsize()
        logger.debug(
            f"Queued packet from {packet.from_port} to {packet.to_port}, queue size: {queue_size}"
        )
        event.wait(timeout=30.0)

        end_time = time.time()
        delay_ms = (end_time - start_time) * 1000

        logger.debug(
            f"Resumed packet from {packet.from_port} to {packet.to_port} after {delay_ms:.1f}ms delay"
        )
        return result_container[0], result_container[1], result_container[2]

    def dispatch_loop(self):
        while self.running:
            try:
                self.state.run(
                    self.apply_rl_action,
                )
            except Exception as e:
                logger.error(f"Error in dispatch loop: {e}")
                continue

    def apply_rl_action(self, action, packet, event, result_container):
        peer_from_id = self.network.port_to_id(packet.from_port)
        peer_to_id = self.network.port_to_id(packet.to_port)
        if action == "DROP":
            result_container[0] = packet.data
            result_container[1] = MAX_U32
            result_container[2] = 1
            logger.debug(
                f"RL Action: DROP - packet from {peer_from_id} to {peer_to_id} will be dropped."
            )
            event.set()
        elif action == "MUTATE":
            if peer_from_id in self.iteration_type._byzantine_nodes:
                # Find the appropriate mutation seed for this round and receiver
                current_round = self.get_current_round_of_node(peer_from_id)
                mutation_applied = False

                for (
                    round_num,
                    receiver_subset,
                    mutation_seed,
                ) in self.process_faults_list:
                    if round_num == current_round and peer_to_id in receiver_subset:
                        logger.debug(
                            f"RL Action: MUTATE - packet from {peer_from_id} to {peer_to_id}, round: {current_round}"
                        )
                        mutated_data, delay, action_type = self.corrupt_message(
                            packet, mutation_seed
                        )
                        result_container[0] = mutated_data
                        result_container[1] = delay
                        result_container[2] = action_type
                        mutation_applied = True
                        break

                if not mutation_applied:
                    logger.debug(
                        "RL Action: MUTATE requested but no mutation rule applies - delivering normally"
                    )
            event.set()
        else:
            event.set()  # default: deliver

    def stop(self):
        self.running = False
        self.dispatch_thread.join()

    def save_packet_for_mutation(self, message) -> None:
        if isinstance(message, ripple_pb2.TMProposeSet):
            self.old_proposals.append(message.currentTxHash)
        elif isinstance(message, ripple_pb2.TMValidation):
            self.old_validations.append(message.validation)

    def get_current_round_of_node(self, node_id: int) -> int:
        """Get the current round (ledger sequence) of a specific node."""
        for _node_id, entry in self.iteration_type.ledger_validation_map.items():
            if node_id == _node_id:
                return entry["seq"]
        return -1

    def corrupt_message(
        self, packet: packet_pb2.Packet, seed: int
    ) -> tuple[bytes, int, int]:
        try:
            message, message_type_no = PacketEncoderDecoder.decode_packet(packet)
        except DecodingNotSupportedError:
            logger.error("Decoding of message not supported")
            return packet.data, 0, 1

        if isinstance(message, ripple_pb2.TMProposeSet):
            return self._corrupt_TMProposeSet(message, message_type_no, seed)
        elif isinstance(message, ripple_pb2.TMValidation):
            return self._corrupt_TMValidation(message, message_type_no, seed)
        elif isinstance(message, ripple_pb2.TMTransaction):
            return self._corrupt_TMTransaction(message, message_type_no, seed)

        return packet.data, 0, 1

    def _corrupt_TMProposeSet(
        self, message, message_type_no: int, seed: int
    ) -> tuple[bytes, int, int]:
        rng = random.Random(seed)
        if rng.choice([True, False]):
            logger.debug(f"Corrupting proposeSeq: {message.proposeSeq}")
            # proposeSeq may never be below 0 (throws)
            lower_bound = -1 if message.proposeSeq > 1 else 1
            message.proposeSeq += rng.choice([lower_bound, 1])
            logger.debug(f"New proposeSeq: {message.proposeSeq}")
        else:
            logger.debug(f"Corrupting currentTxHash: {message.currentTxHash.hex()}")
            message.currentTxHash = rng.choice(self.old_proposals)
            logger.debug(f"New currentTxHash: {message.currentTxHash.hex()}")

        signed_message = PacketEncoderDecoder.sign_message(
            message, self.network.public_to_private_key_map[message.nodePubKey.hex()]
        )
        return (
            PacketEncoderDecoder.encode_message(signed_message, message_type_no),
            0,
            1,
        )

    def _corrupt_TMValidation(
        self, message, message_type_no: int, seed: int
    ) -> tuple[bytes, int, int]:
        rng = random.Random(seed)
        logger.debug(f"Corrupting TMValidation: {message.validation.hex()}")
        message.validation = rng.choice(self.old_validations)
        logger.debug(f"New TMValidation: {message.validation.hex()}")
        return PacketEncoderDecoder.encode_message(message, message_type_no), 0, 1

    def _corrupt_TMTransaction(
        self, message, message_type_no: int, seed: int
    ) -> tuple[bytes, int, int]:
        rng = random.Random(seed)
        logger.debug(
            f"Corrupting TMTransaction message which was {message.rawTransaction.hex()}"
        )
        transaction_hash_set = [tx[3] for tx in self.iteration_type.to_be_validated_txs]
        # rng.choice throws on empty sequence, return original message (TODO: What should be correct behavior here?)
        if not transaction_hash_set:
            return PacketEncoderDecoder.encode_message(message, message_type_no), 0, 1
        message.rawTransaction = self.iteration_type.reverse_compute_tx_hash(
            rng.choice(transaction_hash_set)
        )
        logger.debug(f"New TMTransaction message is {message.rawTransaction.hex()}")
        return PacketEncoderDecoder.encode_message(message, message_type_no), 0, 1


class StateAbstraction(ABC):
    def __init__(self, log_dir: str, strategy: ByzzQLStrategy) -> None:
        self.log_dir = log_dir
        self.strategy = strategy

        self.message_queue = Queue()

        # Initialize RL agent with two action spaces
        self.action_space_with_mutation = ["DROP", "MUTATE", "DELIVER"]
        self.action_space_without_mutation = ["DROP", "DELIVER"]

        self.rl_agent = ByzzQLAgent(action_space=self.action_space_with_mutation)
        self.logger = StateCoverageLogger(log_dir)

    def get_queue(self):
        return self.message_queue

    def log_state(self, cur_iteration: int):
        self.logger.log_state_coverage(
            unique_states=len(self.rl_agent.q_table.keys()),
            iteration=cur_iteration - 1,
            timestamp=time.time(),
        )

    def get_available_action_space(self, packet):
        """
        Determine available action space based on whether mutation is possible
        Returns appropriate action space for the current packet context
        """
        peer_from_id = self.strategy.network.port_to_id(packet.from_port)
        peer_to_id = self.strategy.network.port_to_id(packet.to_port)

        # Check if mutation is possible for this packet
        can_mutate = False
        if peer_from_id in self.strategy.iteration_type._byzantine_nodes:
            current_round = self.strategy.get_current_round_of_node(peer_from_id)
            for (
                round_num,
                receiver_subset,
                mutation_seed,
            ) in self.strategy.process_faults_list:
                if round_num == current_round and peer_to_id in receiver_subset:
                    can_mutate = True
                    break

        if can_mutate:
            logger.debug(
                f"Using action_space_with_mutation for packet {peer_from_id}->{peer_to_id}"
            )
            return self.action_space_with_mutation
        else:
            logger.debug(
                f"Using action_space_without_mutation for packet {peer_from_id}->{peer_to_id}"
            )
            return self.action_space_without_mutation

    @abstractmethod
    def extract_message(
        self,
        message: ripple_pb2.TMProposeSet
        | ripple_pb2.TMValidation
        | ripple_pb2.TMTransaction,
        packet: packet_pb2.Packet,
        current_round: int,
    ) -> str:
        pass

    @abstractmethod
    def get_hash(self) -> str:
        pass

    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    def run(
        self,
        apply_action_callback: Callable,
    ):
        pass


class MessageInboxAbstraction(StateAbstraction):
    def __init__(self, log_dir: str, strategy: ByzzQLStrategy) -> None:
        super().__init__(log_dir, strategy)

        # Fields for dynamic rate
        self.sensitivity_ratio = float(strategy.params.get("sensitivity_ratio", 1.2))
        self.target_inbox = int(strategy.params.get("target_inbox", 30))
        self.overflow_factor = float(strategy.params.get("overflow_factor", 1.2))
        self.underflow_factor = float(strategy.params.get("underflow_factor", 0.8))
        self.max_events = int(
            strategy.params.get("max_events", 300)
        )  # TODO: figure this out
        self.r = self.max_events / 2

    def get_hash(self):
        """
        For Inbox State Abstraction, use extracted key fields instead of just hashing the raw serialized
        data to get a more meaningful state representation:
        - TMProposeSet: proposeSeq, currentTxHash
        - TMValidation: validation
        - TMTransaction: rawTransaction
        We sort, concatenate  and hash these extracted values from messages currently queued for
        processing at each replica.
        """
        queue_contents = list(self.message_queue.queue)
        extracted_values = [item[2] for item in queue_contents if item[2] is not None]
        sorted_values = sorted(extracted_values)
        concatenated = "|".join(sorted_values)
        return hashlib.sha256(concatenated.encode()).hexdigest()

    def reset(self):
        # No manual reset needed between iterations
        pass

    def extract_message(
        self,
        message: ripple_pb2.TMProposeSet
        | ripple_pb2.TMValidation
        | ripple_pb2.TMTransaction,
        packet: packet_pb2.Packet,
        current_round: int,
    ) -> str:
        # TODO: This method being called for every message causes very high CPU usage
        # This method might need to be optimized.
        extracted_value = None
        if isinstance(message, ripple_pb2.TMProposeSet):
            extracted_value = f"ProposeSet:{packet.from_port}:{packet.to_port}:{message.proposeSeq}:{message.currentTxHash.hex()}"
        elif isinstance(message, ripple_pb2.TMValidation):
            # We should use transactions here to extract values.
            ledger_hash, transactions, validated, ledger_index = (
                self.strategy.network.get_transactions(
                    "closed", self.strategy.network.port_to_id(packet.from_port)
                )
            )
            extracted_value = f"Validation:{packet.from_port}:{packet.to_port}:{', '.join(transactions) if transactions else ''}"
        elif isinstance(message, ripple_pb2.TMTransaction):
            decoded = decode(message.rawTransaction.hex())
            # Decoded TMTransaction: {'TransactionType': 'Payment', 'Flags': 0, 'Sequence': 2, 'LastLedgerSequence': 25, 'Amount': '100001000000', 'Fee': '10', 'SigningPubKey': '0330E7FC9D56BB25D6893BA3F317AE5BCF33B3291BD63DB32654A313222F7FD020', 'TxnSignature': '304402200BC25C59B3B22D01B9563D5CF0AB3ECD16B158916D885C26A60DA98CAE35757402207DEA9F34944AA7FAB26C8CC13FB9CD18A2F73EAF556CDEC6CCD0E216F4160A3D', 'Account': 'rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh', 'Destination': 'rMAyDK9H3z3CM6YhTcdGCYUC2RGcDtaGCY'}
            extracted_value = f"Transaction:{packet.from_port}:{packet.to_port}:{decoded['TransactionType']}:{decoded['Sequence']}:{decoded['Amount']}"
        return extracted_value

    def run(
        self,
        apply_action_callback: Callable,
    ):
        try:
            packet, event, extracted_value, result_container = self.message_queue.get(
                timeout=1.0
            )
        except Empty:
            return

        # Following code inspired by PriorityStrategy, with some small changes.
        # Applies a dynamic dispatch rate as defined in http://doi.org/10.1109/ICSE-SEIP58684.2023.00009
        inbox_size = self.message_queue.qsize()

        # Adjust rate r based on inbox size
        if inbox_size > self.target_inbox * self.overflow_factor:
            self.r = min(self.r * self.sensitivity_ratio, inbox_size)
        elif inbox_size < self.target_inbox * self.underflow_factor:
            self.r = max(self.r / self.sensitivity_ratio, inbox_size / 6)
        # else: r doesn't change

        # Rate is clamped |events|/6 <= r <= |events|
        self.r = int(max(inbox_size / 6, min(self.r, inbox_size)))

        logger.debug(f"RATE {self.r}")

        # Only apply rate if r is not zero
        if self.r > 0:
            interval = 1.0 / self.r
            time.sleep(interval)

        # 2. Now we have a collection window - make RL decision
        current_state = self.get_hash()
        available_action_space = self.get_available_action_space(packet)
        action = self.rl_agent.choose_action(current_state, available_action_space)

        # 3. Apply RL action (additional processing based on state)
        apply_action_callback("DELIVER", packet, event, result_container)

        # 4. Update RL learning
        next_state = self.get_hash()
        self.rl_agent.update_q_value(current_state, action, next_state)


class TraceAbstraction(StateAbstraction):
    def __init__(self, log_dir: str, strategy: ByzzQLStrategy) -> None:
        super().__init__(log_dir, strategy)

        # Initialise trace_log as a dict that stores received messages per node (=per process)
        self.trace_log: dict[int, list[str]] = defaultdict(list)

    def record_event(self, node_port: int, extracted_value: str):
        self.trace_log[node_port].append(extracted_value)

    def get_hash(self) -> str:
        concatenated = "|".join(
            [
                item
                for node in sorted(self.trace_log.keys())
                for item in self.trace_log[node]
            ]
        )
        return hashlib.sha256(concatenated.encode()).hexdigest()

    def get_hash_per_node(self, node_port: int) -> str:
        concatenated = "|".join(self.trace_log[node_port])
        return hashlib.sha256(concatenated.encode()).hexdigest()

    def reset(self):
        """
        Resets the current trace log when dispatch thread is no longer active (in between iterations),
        This needs to be done manually since the trace abstraction uses a seperate list, and not the queue (which is emptied by design).
        """
        for k, v in self.trace_log.items():
            logger.debug(f"Trace Len {k}: {len(v)}")
        self.trace_log = defaultdict(list)

    def extract_message(
        self,
        message: ripple_pb2.TMProposeSet
        | ripple_pb2.TMValidation
        | ripple_pb2.TMTransaction,
        packet: packet_pb2.Packet,
        current_round: int,
    ) -> str:
        extracted_value = None
        if isinstance(message, ripple_pb2.TMProposeSet):
            extracted_value = (
                f"{current_round}-ProposeSet:{packet.from_port}:{packet.to_port}"
            )
        elif isinstance(message, ripple_pb2.TMValidation):
            extracted_value = (
                f"{current_round}-Validation:{packet.from_port}:{packet.to_port}"
            )
        elif isinstance(message, ripple_pb2.TMTransaction):
            extracted_value = (
                f"{current_round}-Transaction:{packet.from_port}:{packet.to_port}"
            )
        return extracted_value

    def run(
        self,
        apply_action_callback: Callable,
    ):
        try:
            packet, event, extracted_value, result_container = self.message_queue.get(
                timeout=1.0
            )
        except Empty:
            return

        # NOTE: It was unclear to me whether to implement the state hashing
        # per process (node), or for the whole system, since it mentions 'per process'
        # in the paper. However it would not be logical to do this, since QLearning needs
        # information from the whole system to find bugs that arise from complex interactions
        # between different nodes. The state space however is huge with this method.
        # (but I guess that is expected from the 'trace' abstraction)
        #
        # I do keep the states for each process in a separate list, and combine them only for
        # the hashing.
        #
        # current_state = self.get_hash_node(packet.to_port)
        current_state = self.get_hash()

        # RL agent chooses action based on current trace state
        available_action_space = self.get_available_action_space(packet)
        action = self.rl_agent.choose_action(current_state, available_action_space)

        # Record action in the trace log and apply it
        self.record_event(packet.to_port, extracted_value)
        apply_action_callback(action, packet, event, result_container)

        # Update Q-learning with new trace state
        next_state = self.get_hash()
        self.rl_agent.update_q_value(current_state, action, next_state)
