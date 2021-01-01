from query_flow.parsers import postgres_parser
from query_flow.vizualizers import query_vizualizer

if __name__ == '__main__':
    query_renderer = query_vizualizer.QueryVizualizer(
        parser=postgres_parser.PostgresParser(is_compact=False))

    query1 = """
    SELECT titles.title_id
    FROM titles
    INNER JOIN crew ON crew.title_id = titles.title_id
    INNER JOIN people ON people.person_id = crew.person_id
    WHERE genres = 'Comedy'
      AND name in ('Owen Wilson', 'Adam Sandler', 'Jason Segel')
    """

    query2 = """
    SELECT titles.title_id
    FROM titles
    WHERE genres = 'Comedy'
    UNION
    SELECT titles.title_id
    FROM titles
    WHERE genres = 'Action'
    """

    flow_df = query_renderer.get_flow_df(
        [query1, query2], con_str='postgresql:///etrabelsi_thesis')

    query_renderer.vizualize(
        # , "actual_duration"
        flow_df, title='Missing Records in Where Clause', metrics=['actual_rows'],
        open_=False,
    )
