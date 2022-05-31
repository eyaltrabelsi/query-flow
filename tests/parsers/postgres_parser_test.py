import json
import pathlib

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from query_flow.parsers.postgres_parser import PostgresParser


def assert_dataframe_almost_acual(right, left):
    NON_FLAKY_COLUMNS = ['source', 'target', 'operation_type', 'actual_rows', 'label']
    right = right[NON_FLAKY_COLUMNS].reset_index(drop=True).fillna('')
    left = left[NON_FLAKY_COLUMNS].reset_index(drop=True).fillna('')
    assert_frame_equal(right, left)


@pytest.mark.parametrize('use_case', (pathlib.Path(__file__).parent / 'data' / 'postgres' / 'parse').iterdir())
def test_parse(use_case):
    p = PostgresParser()
    query = [json.loads(open(f'{use_case}/execution_plan.json').read())]
    actual_flow_df = p.parse(query)
    expected_flow_df = pd.read_csv(f'{use_case}/cardinality.csv')
    assert_dataframe_almost_acual(
        actual_flow_df,
        expected_flow_df,
    )


@pytest.mark.parametrize('use_case', (pathlib.Path(__file__).parent / 'data' / 'postgres' / 'multi_parse').iterdir())
def test_parse_multi(use_case):
    p = PostgresParser()
    queries = [json.loads(open(query_f).read()) for query_f in use_case.glob('*.json')]
    actual_flow_df = p.parse(queries)
    expected_flow_df = pd.read_csv(f'{use_case}/cardinality.csv')
    assert_dataframe_almost_acual(
        actual_flow_df,
        expected_flow_df,
    )
