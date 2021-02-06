import logging
import sys
import time

import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm

from query_flow.parsers import postgres_parser
from query_flow.vizualizers import query_vizualizer

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.getLogger('matplotlib').setLevel(logging.WARNING)

BASELINE_QUERIES = 'queries.sql'
BASELINE_FOLDER = "baseline"
OPTIMIZED_QUERIES = 'optimized_queries.sql'
OPTIMIZED_FOLDER = 'optimized'
SLOW_QUERIES_INDICES = [3, 6, 8, 9, 11, 14, 15]


def benchmark_tpch(conn_str, repeat=10):
    baseline_queries = get_queries(BASELINE_QUERIES)
    benchmark_queries(baseline_queries, conn_str, BASELINE_FOLDER, repeat)
    generate_sankey_plots(baseline_queries, conn_str, BASELINE_FOLDER)

    optimizations(conn_str)
    optimized_queries = get_queries(OPTIMIZED_QUERIES)
    benchmark_queries(optimized_queries, conn_str, OPTIMIZED_FOLDER, repeat)
    generate_sankey_plots(optimized_queries, conn_str, OPTIMIZED_FOLDER)
    cleanup(conn_str)


def optimizations(conn_str):
    logging.info("Optimizing database.")
    queries = get_queries("optimizations.sql")
    with create_engine(conn_str).connect() as con:
        for q in queries:
            con.execute(q)


def cleanup(conn_str):
    logging.info("Restoring database state before optimizations.")
    queries = get_queries("cleanup.sql")
    with create_engine(conn_str).connect() as con:
        for q in queries:
            con.execute(q)


def benchmark_queries(queries, conn_str, folder, repeat):
    stats = []
    output_prefix = f"data/{folder}"
    logging.info(f"Run benchmark {repeat} times on {folder}.")
    with create_engine(conn_str).connect() as con:
        pbar = tqdm(total=repeat * len(queries))
        for i in range(repeat):
            for j, q in enumerate(queries):
                start = time.time()
                con.execute(q.replace("%", "%%"))
                duration = time.time() - start
                stats.append({"iteration": i + 1,
                              "query": q,
                              "duration": duration})
                pbar.set_description(f"Iteration {i + 1}: query {j + 1}")
                # pbar.update()

    stats = pd.DataFrame(stats)
    stats.to_csv(f"{output_prefix}/queries_stats.csv", index=False)

    stats.groupby('query').agg({'duration': "mean"})['duration']\
         .plot.hist(grid=True, rwidth=0.9, color='#607c8e')\
         .figure.savefig(f"{output_prefix}/queries_mean.png")


def generate_sankey_plots(queries, conn_str, folder):
    logging.info(f"Creating {folder} Sanky diagrams for every query separately.")
    output_prefix = f"data/{folder}/"
    for i, query in enumerate(queries, 1):
        run_query_flow([query], conn_str, title=f"{output_prefix}query-{i}")

    logging.info(f"Creating {folder} Sanky diagrams for all queries together.")
    run_query_flow(queries, conn_str, title=f"{output_prefix}/all-queries")

    logging.info(f"Creating {folder} Sanky diagrams for all slow queries together.")
    queries = [queries[i] for i in SLOW_QUERIES_INDICES]
    run_query_flow(queries, conn_str, title=f"{output_prefix}/all-slow-queries")


def get_queries(path):
    with open(path) as qf:
        return [q.strip() for q in qf.read().split(';') if q.strip()]


def run_query_flow(queries, conn_str, title):
    # TODO
    parser = postgres_parser.PostgresParser(is_compact=False, execute_query=True)
    query_renderer = query_vizualizer.QueryVizualizer(parser)
    flow_df = query_renderer.get_flow_df(queries, con_str=conn_str)
    query_renderer.vizualize(flow_df,
                             title=title,
                             metrics=['actual_duration'],
                             plot_together=False)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create TPC-H dataset.')
    parser.add_argument(
        'conn_str', type=str,
        help='SQLAlchemy connection string.',
    )
    args = parser.parse_args()
    benchmark_tpch(args.conn_str)
