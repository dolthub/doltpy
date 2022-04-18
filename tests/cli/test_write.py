from doltpy.cli.write import write_pandas, CREATE, UPDATE
from doltpy.cli.read import read_rows
from .helpers import compare_rows
import numpy as np
import pandas as pd


# Note that we use string values here as serializing via CSV does preserve type information in any meaningful way
TEST_ROWS = [
    {"name": "Anna", "adjective": "tragic", "id": "1", "date_of_death": "1877-01-01"},
    {"name": "Vronksy", "adjective": "honorable", "id": "2", "date_of_death": ""},
    {"name": "Oblonksy", "adjective": "buffoon", "id": "3", "date_of_death": ""},
]

def test_write_pandas(init_empty_test_repo):
    dolt = init_empty_test_repo
    write_pandas(dolt, "characters", pd.DataFrame(TEST_ROWS), CREATE, ["id"])
    actual = read_rows(dolt, "characters")
    expected = pd.DataFrame(TEST_ROWS).to_dict("records")
    compare_rows(expected, actual, "id")


def test_write_pandas_accept_nulls_on_update(init_empty_test_repo):
    NULL_ROWS = [
        {"name": "Anna", "adjective": "tragic", "id": "1", "date_of_death": "1877-01-01"},
        {"name": "Vronksy", "adjective": "honorable", "id": "2", "date_of_death": ""},
        {"name": "Oblonsky", "adjective": "buffoon", "id": "3", "date_of_death": np.NaN},
    ]
    dolt = init_empty_test_repo
    write_pandas(dolt, "characters", pd.DataFrame(NULL_ROWS[:2]), CREATE, ["id"])
    write_pandas(dolt, "characters", pd.DataFrame(NULL_ROWS[2:]), UPDATE)
    actual = read_rows(dolt, "characters")
    expected = pd.DataFrame(NULL_ROWS).to_dict("records")
    compare_rows(expected, actual, "id")

