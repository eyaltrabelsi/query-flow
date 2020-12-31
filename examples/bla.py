from query_flow.parsers import postgres_parser
from query_flow.vizualizers import query_vizualizer

if __name__ == '__main__':
    query_renderer = query_vizualizer.QueryVizualizer(
        parser=postgres_parser.PostgresParser(execute_query=False))
    query = """
    SELECT titles.title_id
    FROM titles
    INNER JOIN crew ON crew.title_id = titles.title_id
    INNER JOIN people ON people.person_id = crew.person_id
    WHERE genres like '%Comedy%'
      AND name in ('Owen Wilson', 'Adam Sandler', 'Jason Segel')

    """
    cardinality_df = query_renderer.get_cardinality_df(
        query, con_str='postgresql:///etrabelsi_thesis',
    )
    query_renderer.vizualize(
        cardinality_df, title='Missing Records in Where Clause', metrics=['plan_rows'],
        open_=False,
    )
