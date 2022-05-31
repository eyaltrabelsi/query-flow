import pandas as pd
from pandas.testing import assert_frame_equal

from query_flow.parsers.db_parser import DBParser
from query_flow.parsers.postgres_parser import PostgresParser


def test_not_compact_get_next_id():
    parser = PostgresParser(is_compact=False)
    given_new_node = {
        'execution_node': {'Node Type': 'Hash Join'},
        'specific_attrs': {'label': 'JOIN', 'label_metadata': 'Hash Cond'},
    }
    current_hash = parser._get_hash(**given_new_node)
    actual_new_node = parser._get_next_id(current_hash)
    assert actual_new_node == parser.max_supported_nodes - 1

    current_hash = parser._get_hash(**given_new_node)
    actual_existing_node = parser._get_next_id(current_hash)
    assert actual_existing_node == parser.max_supported_nodes - 2


def test_compact_get_next_id():
    parser = PostgresParser(is_compact=True)
    given_new_node = {
        'execution_node': {'Node Type': 'Hash Join'},
        'specific_attrs': {'label': 'JOIN', 'label_metadata': 'Hash Cond'},
    }
    current_hash = parser._get_hash(**given_new_node)
    actual_new_node = parser._get_next_id(current_hash)
    assert actual_new_node == parser.max_supported_nodes - 1
    assert len(parser.label_to_id_dict) == 1

    current_hash = parser._get_hash(**given_new_node)
    actual_existing_node = parser._get_next_id(current_hash)
    assert actual_existing_node == parser.max_supported_nodes - 1
    assert len(parser.label_to_id_dict) == 1


def test_align_source_target_ids():
    given = pd.DataFrame(
        {'source': [10, 11, 13, 12], 'target': [11, 12, 12, 14], 'operation_type': ['scan', 'scan', 'scan', 'scan']}
    )
    actual = DBParser.align_source_target_ids(given)
    expected = pd.DataFrame(
        {'source': [0, 1, 2, 3], 'target': [1, 2, 4, 2], 'operation_type': ['scan', 'scan', 'scan', 'scan']}
    )
    assert_frame_equal(actual, expected)
