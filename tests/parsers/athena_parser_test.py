# import json
# import pathlib
#
# import pandas as pd
# import pytest
# from pandas.testing import assert_frame_equal
#
# from query_flow.parsers.athena_parser import AthenaParser
#
# # TODO refactor this to have regular output
# def assert_dataframe_almost_acual(right, left):
#     NON_FLAKY_COLUMNS = [
#         'source', 'target', 'operation_type', 'label'
#     ]
#     right = right[NON_FLAKY_COLUMNS].reset_index(drop=True).fillna('')
#     left = left[NON_FLAKY_COLUMNS].reset_index(drop=True).fillna('')
#     assert_frame_equal(right, left)
#
#
# @pytest.mark.parametrize('use_case', (pathlib.Path(__file__).parent / 'data' / 'athena'/ 'parse').iterdir())
# def test_parse(use_case):
#     p = AthenaParser()
#     query = [json.loads(open(f'{use_case}/execution_plan.json').read())]
#     actual_flow_df = p.parse(query)
#     expected_flow_df = pd.read_csv(f'{use_case}/cardinality.csv')
#     assert_dataframe_almost_acual(
#         actual_flow_df, expected_flow_df,
#     )
