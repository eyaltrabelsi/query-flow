import json
import os

import pandas as pd
import pytest
from query_flow.parsers.postgres_parser import PostgresParser


@pytest.mark.parametrize('use_case', os.listdir("data/"))
def test_parse(use_case):
    p = PostgresParser()
    query = json.load(open(f"data/{use_case}/execution_plan.json", "r").read())
    actual_cardinality_df = p.parse(query)
    exepected_cardinality_df = pd.read_csv(f"data/{use_case}/cardinality.csv")
    assert actual_cardinality_df == exepected_cardinality_df

