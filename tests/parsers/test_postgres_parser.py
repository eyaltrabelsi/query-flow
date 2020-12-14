import pandas as pd

from query_flow.parsers.postgres_parser import PostgresParser


def test_parse():

    p = PostgresParser()
    for f in ["proposal/data/fixed_identifying_duplications_query_flow_representation.csv","proposal/data/fixed_ineffective_operation_query_flow_representation.csv","proposal/data/ineffective_operation_query_flow_representation.csv", "proposal/data/missing_records_query_flow_representation.csv"]:
        df = pd.read_csv(f"/Users/etrabelsi/IdeaProjects/thesis/{f}")

    pass


