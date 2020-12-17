from query_flow.parsers import PostgresParser
from query_flow.vizualizers import QueryVizualizer

if __name__ == "__main__":
    query_renderer = QueryVizualizer(parser=PostgresParser())
    query = """
    SELECT titles.title_id
    FROM titles
    INNER JOIN crew ON crew.title_id = titles.title_id
    INNER JOIN people ON people.person_id = crew.person_id
    WHERE genres like '%Comedy%' 
      AND name in ('Owen Wilson', 'Adam Sandler', 'Jason Segel')
          
    """
    cardinality_df = query_renderer.get_cardinality_df(query, con_str='postgresql:///etrabelsi_thesis')
    query_renderer.vizualize(cardinality_df, title="Missing Records in Where Clause", metrics=["actual_rows"],
                             open_=False)
