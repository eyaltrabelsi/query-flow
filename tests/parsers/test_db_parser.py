import pandas as pd

from pandas.testing import assert_frame_equal
from query_flow.parsers.db_parser import DBParser
from query_flow.parsers.postgres_parser import PostgresParser


def test_get_next_id():
    parser = PostgresParser()
    given_new_node = {"execution_node": {'Node Type': 'Hash Join'},
                      "target_id": 1,
                      "specific_attrs": {'label': 'JOIN', 'label_metadata': "Hash Cond"}}
    actual_new_node = parser._get_next_id(**given_new_node)
    assert actual_new_node == parser.max_supported_nodes - 1
    assert len(parser.label_to_id_dict) == 1

    actual_existing_node = parser._get_next_id(**given_new_node)
    assert actual_existing_node == parser.max_supported_nodes - 1
    assert len(parser.label_to_id_dict) == 1


def test_align_source_target_ids():
    given = pd.DataFrame({"source": [10, 11, 13, 12],
                          "target": [11, 12, 12, 14]})
    actual = DBParser.align_source_target_ids(given)
    expected = pd.DataFrame({"source": [0, 1, 2, 3],
                             "target": [1, 2, 4, 2]})
    assert_frame_equal(actual, expected)

