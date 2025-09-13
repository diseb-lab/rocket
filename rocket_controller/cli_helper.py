"""Various helper functions for the command-line interface."""

import argparse
import ast
import sys
from typing import Any, Dict, List, Type

from rocket_controller.strategies import Strategy


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        An argparse namespace object, containing the parsed arguments.
    """
    parser = argparse.ArgumentParser(
        prog="python -m rocket_controller",
        description="A tool for testing the XRP Ledger consensus algorithm at the system-level, "
        "using easily adaptable fuzzing-based techniques.",
    )
    parser.add_argument(
        "strategy",
        type=str,
        default="RandomFuzzer",
        help="The name of the Strategy Class to use.",
    )
    parser.add_argument(
        "-n",
        "--network_config",
        type=str,
        default=None,
        help="The relative path to the network configuration file to use. "
        "Defaults to ./config/default_network.yaml.",
        metavar="PATH",
    )
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default=None,
        help="The relative path to the configuration file to use. Defaults to ./config/default_NAME.yaml, "
        "e.g. for the RandomFuzzer, the default would be ./config/default_RandomFuzzer.yaml",
        metavar="PATH",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=None,
        help="The amount of XRPL validator nodes to configure in the network. "
        "If set, overrides the amount of nodes specified in the network configuration file.",
        metavar="AMOUNT",
    )
    parser.add_argument(
        "--partition",
        type=check_valid_partition,
        default=None,
        help="The network partition of the nodes. "
        "If set, overrides the partition specified in the network configuration file.",
        metavar="PARTITION",
    )
    parser.add_argument(
        "--nodes_unl",
        type=check_valid_partition,
        default=None,
        help="The UNL (trusted nodes) configuration. "
        "If set, overrides the UNL partition specified in the network configuration file.",
        metavar="UNL",
    )
    parser.add_argument(
        "--encoding",
        type=check_valid_encoding,
        default=None,
        help="The encoding of the numbers to be used by the evolutionary strategy. "
        "If set, overrides the encoding specified in the strategy configuration file.",
        metavar="ENCODING",
    )
    parser.add_argument(
        "--overrides",
        type=check_valid_strategy_overrides,
        default=None,
        help="A way to override certain values found in the strategy configuration file. "
        "Format: PARAM1=VALUE1,PARAM2=VALUE2...",
        metavar="VALUES",
    )
    parser.add_argument(
        "--log_dir",
        type=str,
        default=None,
        help="The directory where the logs should be stored. Defaults to ./logs/timestamp",
        metavar="PATH",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=None,
        help="The number of rounds to run in the strategy. Overrides the configuration file.",
        metavar="ROUNDS",
    )
    parser.add_argument(
        "--network_faults",
        type=int,
        default=None,
        help="The number of network faults to simulate. Overrides the configuration file.",
        metavar="NETWORK_FAULTS",
    )
    parser.add_argument(
        "--process_faults",
        type=int,
        default=None,
        help="The number of process faults to simulate. Overrides the configuration file.",
        metavar="PROCESS_FAULTS",
    )
    parser.add_argument(
        "--small_scope",
        type=bool,
        default=None,
        help="Whether to use a small scope for the strategy. Overrides the configuration file.",
        metavar="SMALL_SCOPE",
    )
    parser.add_argument(
        "--drop_probability",
        type=float,
        default=None,
        help="The probability of dropping a message. Overrides the configuration file.",
        metavar="DROP_PROBABILITY",
    )
    parser.add_argument(
        "--corrupt_probability",
        type=float,
        default=None,
        help="The probability of corrupting a message. Overrides the configuration file.",
        metavar="CORRUPT_PROBABILITY",
    )

    return parser.parse_args()


def str_to_strategy(classname: str) -> Type[Strategy]:
    """
    Returns a Strategy class type based on the name of the Strategy.

    Args:
        classname: The name of the Strategy class.

    Returns:
        A Strategy class type.
    """
    return getattr(sys.modules["rocket_controller.strategies"], classname)


def check_valid_partition(partition: str) -> List[List[int]]:
    """
    Checks whether the string format of a network partition is valid.

    Args:
        partition: The string containing the network partition.

    Returns:
        A 2d list containing the network partition.
    """

    def valid_2d_array(array) -> bool:
        if isinstance(array, list):
            return all(isinstance(i, list) for i in array)
        return False

    try:
        parsed_array = ast.literal_eval(partition)
        if valid_2d_array(parsed_array):
            return parsed_array
        raise ValueError
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"not a valid partition: {partition!r}") from e


def check_valid_strategy_overrides(overrides_str: str) -> Dict[str, str]:
    """
    Checks whether the string format of network parameter overrides is valid.

    Args:
        overrides_str: The string containing the network parameter overrides.

    Returns:
        A dictionary containing the key-value pairs for the network parameter overrides.
    """
    result = {}
    items = overrides_str.split(", ")
    for item in items:
        separated_items = item.split("=")
        if len(separated_items) != 2:
            raise argparse.ArgumentTypeError(f"not a valid override: {item!r}")
        result[separated_items[0]] = separated_items[1]
    return result


def check_valid_encoding(encoding: str) -> List[int]:
    try:
        parsed_array = ast.literal_eval(encoding)
        if isinstance(parsed_array, list):
            return parsed_array
        raise ValueError
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"not a valid encoding: {encoding!r}") from e


def process_args(args: argparse.Namespace) -> Dict[str, Any]:
    """
    Parses the command line arguments.

    Args:
        args: The command line arguments.

    Returns:
        A dictionary containing the (overridden) class parameters for a Strategy instance.
    """
    params_dict = {}
    network_overrides = {}
    strategy_overrides = {}

    if args.nodes:
        network_overrides["number_of_nodes"] = args.nodes
    if args.partition:
        network_overrides["network_partition"] = args.partition
    if args.nodes_unl:
        network_overrides["unl_partition"] = args.nodes_unl

    if args.encoding:
        strategy_overrides["encoding"] = args.encoding
    if args.rounds:
        strategy_overrides["rounds"] = args.rounds
    if args.network_faults:
        strategy_overrides["network_faults"] = args.network_faults
    if args.process_faults:
        strategy_overrides["process_faults"] = args.process_faults
    if args.small_scope is not None:
        strategy_overrides["small_scope"] = args.small_scope
    if args.drop_probability:
        strategy_overrides["drop_probability"] = args.drop_probability
    if args.corrupt_probability:
        strategy_overrides["corrupt_probability"] = args.corrupt_probability
    if args.overrides and len(args.overrides.keys()) > 0:
        strategy_overrides += args.overrides

    if args.network_config:
        params_dict["network_config_path"] = args.network_config
    if args.config:
        params_dict["strategy_config_path"] = args.config
    if args.log_dir:
        params_dict["log_dir"] = args.log_dir
    if len(network_overrides.keys()) > 0:
        params_dict["network_overrides"] = network_overrides
    if len(strategy_overrides.keys()) > 0:
        params_dict["strategy_overrides"] = strategy_overrides

    return params_dict
