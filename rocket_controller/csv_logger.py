"""This module contains a base class to log csv files and two fine-tuned classes extended from said base class."""

import atexit
import csv
from datetime import datetime
from pathlib import Path
from typing import Any, List

from rocket_controller.validator_node_info import ValidatorNode

action_log_columns = [
    "timestamp",
    "action",
    "send_amount",
    "from_node_id",
    "to_node_id",
    "message_type",
    "original_data",
    "possibly_mutated_data",
]

result_log_columns = [
    "ledger_count",
    "goal_ledger_count",
    "time_to_consensus",
    "close_times",
    "ledger_hashes",
    "ledger_indexes",
]

spec_check_columns = [
    "iteration",
    "reached_goal_ledger",
    "same_ledger_hashes",
    "same_ledger_indexes",
]


class CSVLogger:
    """CSVLogger class which can be utilized to log to a csv file."""

    def __init__(self, filename: str, columns: list[Any], directory: str = ""):
        """
        Initialize CSVLogger class.

        Args:
            filename: The name of the log file.
            columns: The columns to be used in the log.
            directory: The directory to store the log file in.
        """
        Path("./logs/" + directory).mkdir(parents=True, exist_ok=True)

        filename = filename if filename.endswith(".csv") else filename + ".csv"

        self.filepath = "./logs/" + directory + "/" + filename
        self.csv_file = open(self.filepath, mode="w", newline="")
        self.writer = csv.writer(self.csv_file)
        self.columns = [col.__str__ for col in columns]
        self.writer.writerow(columns)
        atexit.register(self.close)

    def close(self):
        """Close the CSV file."""
        self.csv_file.close()

    def flush(self):
        """Flush the CSV file."""
        self.csv_file.flush()

    def log_row(self, row: list[Any]):
        """
        Log an arbitrary row.

        Args:
            row (list[str]): Row to be logged.

        Raises:
            ValueError: If length of row is not equal to the amount of columns.
        """
        if len(self.columns) != len(row):
            raise ValueError(
                f"Wrong number of column entries in the given row, required columns are: {self.columns}"
            )
        self.writer.writerow(row)

    def log_rows(self, rows: list[list[Any]]):
        """
        Log multiple arbitrary rows.

        Args:
            rows (list[list[str]]): Rows to be logged.

        Raises:
            ValueError: If length of any given row is not equal to the amount of columns.
        """
        for row in rows:
            self.log_row(row)


class ActionLogger(CSVLogger):
    """CSVLogger child class which is dedicated to handle the logging of taken actions."""

    def __init__(
        self,
        sub_directory: str,
        validator_node_list: list[ValidatorNode],
        action_log_filename: str | None = None,
        node_log_filename: str | None = None,
    ):
        """
        Initialize ActionLogger class.

        Args:
            sub_directory: Sub-directory to store the log files under.
            validator_node_list: List of validator nodes in the network.
            action_log_filename: Name of the action log file.
            node_log_filename: Name of the node log file.
        """
        final_filename = (
            action_log_filename if action_log_filename is not None else "action_log.csv"
        )
        directory = sub_directory

        node_logger = CSVLogger(
            filename=node_log_filename
            if node_log_filename is not None
            else "node_info",
            columns=["validator_node_info"],
            directory=directory,
        )
        node_logger.log_rows([[node] for node in validator_node_list])
        node_logger.close()

        super().__init__(
            filename=final_filename,
            columns=action_log_columns,
            directory=directory,
        )

    def log_action(
        self,
        action: int,
        send_amount: int,
        from_node_id: int,
        to_node_id: int,
        message_type: str,
        original_data: str,
        possibly_mutated_data: str,
        custom_timestamp: int | None = None,
    ):
        """
        Log an action according to a specific column format.

        Args:
            action: Action to be logged.
            send_amount: The amount of times the messages should be sent.
            from_node_id: ID of the node who sent the message.
            to_node_id: ID of the node who is supposed to receive the message.
            message_type: The message type as defined in the ripple.proto file.
            original_data: The message's original data.
            possibly_mutated_data: The message's possibly mutated data.
            custom_timestamp: A custom timestamp to log if desired.
        """
        # Note: timestamp is milliseconds since epoch (January 1, 1970)
        self.writer.writerow(
            [
                int(datetime.now().timestamp() * 1000)
                if custom_timestamp is None
                else custom_timestamp,
                action,
                send_amount,
                from_node_id,
                to_node_id,
                message_type,
                original_data,
                possibly_mutated_data,
            ]
        )


class ResultLogger(CSVLogger):
    """CSVLogger child class which is dedicated to handle the logging of results."""

    def __init__(
        self,
        sub_directory: str,
        result_log_filename: str | None = None,
    ):
        """
        Initialize ResultLogger class.

        Args:
            sub_directory: The subdirectory to store the results in.
            result_log_filename: The name of the log file to store the results in.
        """
        final_filename = (
            result_log_filename if result_log_filename is not None else "result_log.csv"
        )
        directory = sub_directory
        super().__init__(
            filename=final_filename,
            columns=result_log_columns,
            directory=directory,
        )

    def log_result(
        self,
        ledger_count: int,
        goal_ledger_count: int,
        time_to_consensus: float,
        close_times: List[int],
        ledger_hashes: List[str],
        ledger_indexes: List[int],
    ):
        """
        Log a result row to the CSV file.

        Args:
            ledger_count: Ledger count of the iteration.
            goal_ledger_count: Goal ledger index of the iteration.
            time_to_consensus: Time taken to reach consensus.
            ledger_hashes: Ledger hashes of the nodes to be logged.
            ledger_indexes: Ledger indexes of the nodes to be logged.
            close_times: Close times of the nodes to be logged.
        """
        self.writer.writerow(
            [
                ledger_count,
                goal_ledger_count,
                f"{time_to_consensus:.6f}",
                close_times,
                ledger_hashes,
                ledger_indexes,
            ]
        )


class SpecCheckLogger(CSVLogger):
    """CSVLogger child class which is dedicated to handle the logging of specification checks."""

    def __init__(
        self,
        sub_directory: str,
    ):
        """
        Initialize SpecCheckLogger class.

        Args:
            sub_directory: The subdirectory to store the spec check results in.
        """
        super().__init__(
            filename="spec_check_log.csv",
            columns=spec_check_columns,
            directory=sub_directory,
        )
        self.close()

    def log_spec_check(
        self,
        iteration: int,
        reached_goal_ledger: bool | str,
        same_ledger_hashes: bool | str,
        same_ledger_indexes: bool | str,
    ):
        """
        Log a spec check row to the CSV file.

        Args:
            iteration: The current iteration.
            reached_goal_ledger: Whether the goal ledger was reached.
            same_ledger_hashes: Whether the ledger hashes were the same.
            same_ledger_indexes: Whether the ledger indexes were the same.
        """
        with open(self.filepath, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    iteration,
                    reached_goal_ledger,
                    same_ledger_hashes,
                    same_ledger_indexes,
                ]
            )
