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
BASELINE_FOLDER = 'baseline'
OPTIMIZED_QUERIES = 'optimized_queries.sql'
OPTIMIZED_FOLDER = 'optimized'


def benchmark_tpch(conn_str, repeat=10, run_statistics=True, generate_sankey=True):
    logging.info('Restoring database state before optimizations.')
    baseline(conn_str)
    baseline_queries = get_queries(BASELINE_QUERIES)
    if run_statistics:
        benchmark_queries(baseline_queries, conn_str, BASELINE_FOLDER, repeat)
    if generate_sankey:
        generate_sankey_plots(baseline_queries, conn_str, BASELINE_FOLDER)

    logging.info('Optimizing database.')
    optimizations(conn_str)
    if run_statistics:
        benchmark_queries(baseline_queries, conn_str, OPTIMIZED_FOLDER, repeat)
    if generate_sankey:
        generate_sankey_plots(baseline_queries, conn_str, OPTIMIZED_FOLDER)


def optimizations(conn_str):
    baseline(conn_str)
    queries = get_queries('optimizations.sql')
    with create_engine(conn_str).connect() as con:
        for q in queries:
            con.execute(q)
        con.execute('commit')
        con.execute("alter system set work_mem  = '1GB'")
        con.execute('commit')
        con.execute('alter system set max_parallel_workers  = 8')


def baseline(conn_str):
    cleanup(conn_str)
    queries = get_queries('indexes.sql')
    with create_engine(conn_str).connect() as con:
        con.execute('commit')
        con.execute("alter system set work_mem  = '2MB'")
        con.execute('commit')
        con.execute('alter system set max_parallel_workers  = 4')

        for q in queries:
            con.execute(q)


def cleanup(conn_str):
    query = get_queries('indexes_removal.sql')[0].replace('%', '%%')
    with create_engine(conn_str).connect() as con:
        queries = con.execute(query).fetchall()
        for q in queries:
            con.execute(q[0])


def benchmark_queries(queries, conn_str, folder, repeat):
    stats = []
    output_prefix = f"data/{folder}/{conn_str.split('/')[-1]}"
    logging.info(f'Run benchmark {repeat} times on {folder}.')
    with create_engine(conn_str).connect() as con:
        for i in range(repeat):
            for j, q in enumerate(queries):
                start = time.time()
                con.execute(q.replace('%', '%%'))
                duration = time.time() - start
                stats.append({'iteration': i + 1,
                              'query_number': j + 1,
                              'query_text': q,
                              'duration': duration})

    stats = pd.DataFrame(stats)
    stats.to_csv(f'{output_prefix}/queries_stats.csv', index=False)
    logging.info(stats.duration.sum())
    slow_queries = (stats
                    .groupby('query_text').agg({'duration': 'mean'})
                    .nlargest(7, ['duration'])
                    )
    logging.info(slow_queries)
    # stats.groupby('query_text').agg({'duration': "mean"})['duration']\
    #      .plot.hist(grid=True, rwidth=0.9, color='#607c8e')\
    #      .figure.savefig(f"{output_prefix}/queries_mean.png")


def generate_sankey_plots(queries, conn_str, folder):
    logging.info(
        f'Creating {folder} Sanky diagrams for every query separately.')
    output_prefix = f"data/{folder}/{conn_str.split('/')[-1]}"
    # for i, query in enumerate(queries, 1):
    #     run_query_flow([query], conn_str, title=f"{output_prefix}/query-{i}")
    logging.info(f'Creating {folder} Sanky diagrams for all queries together.')
    run_query_flow(queries, conn_str, title=f'{output_prefix}/all-queries')

    logging.info(
        f'Creating {folder} Sanky diagrams for all slow queries together.')
    slow_queries = (pd.read_csv(f'{output_prefix}/queries_stats.csv')
                      .groupby('query_text').agg({'duration': 'mean'})
                      .nlargest(7, ['duration'])
                    )
    logging.info(slow_queries)
    logging.info(pd.read_csv(
        f'{output_prefix}/queries_stats.csv').duration.sum())
    run_query_flow(slow_queries.index.tolist(), conn_str,
                   title=f'{output_prefix}/all-slow-queries')


def get_queries(path):
    with open(path) as qf:
        return [q.strip() for q in qf.read().split(';') if q.strip()]


def run_query_flow(queries, conn_str, title):
    parser = postgres_parser.PostgresParser(
        is_compact=False, execute_query=True)
    query_renderer = query_vizualizer.QueryVizualizer(parser)
    flow_df = query_renderer.get_flow_df(queries, con_str=conn_str)
    query_renderer.vizualize(flow_df,
                             title=title,
                             metrics=['actual_duration'],
                             plot_together=False)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create TPC-H dataset.')
    parser.add_argument('conn_str', type=str,
                        help='SQLAlchemy connection string.')
    parser.add_argument('repeats', type=int, help='number of time to be executed',
                        )
    parser.add_argument('--run_statistics', default=False, action='store_true')
    parser.add_argument('--generate_sankey',
                        default=False, action='store_true')
    args = parser.parse_args()
    benchmark_tpch(args.conn_str,
                   args.repeats,
                   args.run_statistics,
                   args.generate_sankey)
