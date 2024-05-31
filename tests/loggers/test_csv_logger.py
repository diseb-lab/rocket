"""Tests for CSVLogger."""

import csv
import os

from xrpl_controller.csv_logger import CSVLogger

test_dir = "TEST_LOG_DIR"


# NOTE: Only run this test from the xrpl_controller module, tox does this automatically
# You are able to run this test regularly, but it will create the logs directory in the wrong location
def test_construction():
    """Test CSVLogger construction."""
    logger = CSVLogger("TEST", [], test_dir)
    logger.close()
    path = "./logs/" + test_dir + "/TEST.csv"
    assert os.path.isfile(path)
    os.remove(path)
    os.rmdir("./logs/" + test_dir)


def test_columns():
    """Test columns."""
    cols = ["col1", "col2"]
    logger = CSVLogger("TEST_COLS", cols, test_dir)
    logger.close()

    path = "./logs/" + test_dir + "/TEST_COLS.csv"
    with open(path) as file:
        csv_reader = csv.reader(file)
        first_line = next(csv_reader)
        assert first_line == cols
        os.remove(path)
        os.rmdir("./logs/" + test_dir)


def test_flush():
    """Test flush method."""
    cols = ["col1", "col2"]
    logger = CSVLogger("TEST_FLUSH", cols, test_dir)
    logger.flush()

    path = "./logs/" + test_dir + "/TEST_FLUSH.csv"
    with open(path) as file:
        csv_reader = csv.reader(file)
        first_line = next(csv_reader)
        assert first_line == cols
        os.remove(path)
        os.rmdir("./logs/" + test_dir)

    logger.close()


def test_rows():
    """Test writing of rows."""
    cols = ["col1"]
    logger = CSVLogger("TEST_ROWS", cols, test_dir)
    logger.log_row(["1"])
    logger.log_rows([["2"], ["3"]])
    logger.close()

    path = "./logs/" + test_dir + "/TEST_ROWS.csv"
    with open(path) as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        assert next(csv_reader) == ["1"]
        assert next(csv_reader) == ["2"]
        assert next(csv_reader) == ["3"]

    os.remove(path)
    os.rmdir("./logs/" + test_dir)


def test_wrong_amount():
    """Test failure on wrong column amount."""
    cols = ["col1"]
    logger = CSVLogger("TEST_INVALID", cols, test_dir)
    try:
        logger.log_row(["1", "2"])
        raise AssertionError()
    except ValueError:
        pass

    path = "./logs/" + test_dir + "/TEST_INVALID.csv"
    os.remove(path)
    os.rmdir("./logs/" + test_dir)
