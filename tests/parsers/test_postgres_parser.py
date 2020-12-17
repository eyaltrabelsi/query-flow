import json
import os

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from query_flow.parsers.postgres_parser import PostgresParser


def assert_dataframe_almost_acual(right, left):
    NON_FLAKY_COLUMNS = ["source", "target", "operation_type",
                         "actual_rows", "label", "label_metadata"]
    right = right[NON_FLAKY_COLUMNS].reset_index(drop=True).fillna("")
    left = left[NON_FLAKY_COLUMNS].reset_index(drop=True).fillna("")
    assert_frame_equal(right, left)


@pytest.mark.parametrize('use_case', os.listdir("data/"))
def test_parse(use_case):
    if use_case in ["ineffective_operation", "missing_records"]:
        pytest.skip(f"Not implemented yet - {use_case}")

    p = PostgresParser()
    query = json.loads(open(f"data/{use_case}/execution_plan.json", "r").read())
    actual_cardinality_df = p.parse(query)
    expected_cardinality_df = pd.read_csv(f"data/{use_case}/cardinality.csv")
    assert_dataframe_almost_acual(actual_cardinality_df, expected_cardinality_df)
