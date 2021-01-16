import hashlib
import logging
import sys

import pandas as pd

from query_flow.parsers import postgres_parser
from query_flow.vizualizers import query_vizualizer

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def benchmark_tpch(conn_str):
    all_queries = get_queries()
    logging.info("Creating Sanky diagrams for every query separately.")
    single_queries_stats = render_each_query_flow(all_queries, conn_str)
    logging.info("Creating Sanky diagrams for every query together.")
    render_all_queries_flow(all_queries, conn_str, single_queries_stats)
    logging.info("Done.")


def get_queries():
    with open('queries.sql') as qf:
        return [q.strip() for q in qf.read().split(';')]


def render_all_queries_flow(queries, conn_str, single_queries_stats):
    output_prefix = "data/all-query"
    run_query_flow(queries, conn_str, title=output_prefix)
    pass
    stats = pd.DataFrame([{"query_number": ", ".join(single_queries_stats.query_number.astype("str")),
                           "duration": single_queries_stats.duration.sum(),
                           "estimated_cost_flow": f"{output_prefix}-estimated_cost.html",
                           "actual_duration": f"{output_prefix}-actual_duration.html",
                           "actual_rows": f"{output_prefix}-actual_rows.html"}])
    stats.to_csv("all_queries_info.csv", index=False)


def run_query_flow(queries, conn_str, title):
    parser = postgres_parser.PostgresParser(is_compact=False, execute_query=True)
    query_renderer = query_vizualizer.QueryVizualizer(parser)
    flow_df = query_renderer.get_flow_df(queries, con_str=conn_str)
    query_renderer.vizualize(flow_df,
                             title=title,
                             metrics=['actual_duration', 'actual_rows', 'estimated_cost'],
                             plot_together=False)
    return flow_df.actual_total_time.max() / 1000


def render_each_query_flow(queries, conn_str):
    output_prefix = "data/query-{}"
    res = []
    for i, query in enumerate(queries, 1):
        duration = run_query_flow([query], conn_str, title=output_prefix.format(i))
        logging.info(f"Creating Sanky diagrams for query  {i} took {duration:.2f} Seoncd.")
        res.append({"query_number": i,
                    "hash": hashlib.md5(query.encode('utf-8')).hexdigest(),
                    "duration": duration,
                    "query": query,
                    "estimated_cost_flow": f"{output_prefix}-estimated_cost.html",
                    "actual_duration": f"{output_prefix}-actual_duration.html",
                    "actual_rows": f"{output_prefix}-actual_rows.html"})
    stats = pd.DataFrame(res)
    stats.to_csv("single_query_info.csv", index=False)
    return stats


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create TPC-H dataset.')
    parser.add_argument(
        'conn_str', type=str,
        help='SQLAlchemy connection string.',
    )
    args = parser.parse_args()
    benchmark_tpch(args.conn_str)
