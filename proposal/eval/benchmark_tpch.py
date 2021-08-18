import json
import logging
import sys
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from query_flow.parsers import postgres_parser
from query_flow.vizualizers import query_vizualizer

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.getLogger('matplotlib').setLevel(logging.WARNING)

BASELINE_FOLDER = 'baseline'
BASELINE2_FOLDER = 'baseline'
OPTIMIZED_FOLDER = 'optimized'
OPTIMIZED2_FOLDER = 'optimized_with_queries'


def benchmark_tpch(benchmark, repeat=10, run_statistics=True, generate_sankey=True):
    logging.info('Restoring database state before optimizations.')
    unoptimized_conn_str = f"postgresql://localhost/{benchmark}"
    optimized_conn_str = f"{unoptimized_conn_str}-optimized"
    baseline_queries = get_queries(f'/Users/etrabelsi/{benchmark}/queries/queries.sql')
    optimized_queries = get_optimized_queries(f'/Users/etrabelsi/{benchmark}/queries/queries.sql')

    # First run
    for queries, folder, conn_str in [(baseline_queries, BASELINE_FOLDER, unoptimized_conn_str),
                                      (baseline_queries, BASELINE2_FOLDER, optimized_conn_str),
                                      (optimized_queries, OPTIMIZED_FOLDER, unoptimized_conn_str),
                                      (optimized_queries, OPTIMIZED2_FOLDER, optimized_conn_str)]:


        engine = create_engine(conn_str)
        connection = engine.raw_connection()
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        cursor.execute("VACUUM FULL")

        if run_statistics:
            benchmark_queries(queries, conn_str, folder, repeat)

        if generate_sankey:
            generate_sankey_plots(queries, conn_str, folder)


def benchmark_queries(queries, conn_str, folder, repeat):
    stats = []
    output_prefix = f"data/{folder}_{conn_str.split('/')[-1]}"

    logging.info(f'Run benchmark {repeat} times on {folder}.')
    with create_engine(conn_str).connect() as con:

        for i in range(repeat):
            iteration_start = time.time()
            logging.info(f'Starting iteration {i}.')
            for j, q in enumerate(queries):
                start = time.time()
                con.execute(q.replace('%', '%%'))
                duration = time.time() - start

                stats.append({'iteration': i + 1,
                              'query_number': j + 1,
                              'query_text': q,
                              'duration': duration})
            logging.info(f'Iteration {i} took {time.time() - iteration_start}.')

    Path(output_prefix).mkdir(parents=True, exist_ok=True)
    stats = pd.DataFrame(stats)
    stats.to_csv(f'{output_prefix}/queries_stats.csv', index=False)


def generate_sankey_plots(queries, conn_str, folder):
    output_prefix = f"data/{folder}_{conn_str.split('/')[-1]}"
    logging.info(f'Creating {folder} Sanky diagrams for all queries together.')
    run_query_flow(queries, conn_str, title=f'{output_prefix}/all-queries')

    logging.info(f'Creating Sanky diagrams for all slow queries together.')
    slow_queries = (pd.read_csv(f'{output_prefix}/queries_stats.csv')
                      .groupby(['query_text', 'query_number']).agg({'duration': 'mean'})
                      .nlargest(4, ['duration'])

                    )

    slow_queries = [_[0] for _ in slow_queries.index.tolist()]
    parser = postgres_parser.PostgresParser(is_compact=False, execute_query=True)
    query_renderer = query_vizualizer.QueryVizualizer(parser)

    logging.info(f'Creating Sanky diagrams for every query separately.')
    for i, query in enumerate(slow_queries, 1):
        title = f"{output_prefix}/query-{i}"
        query_prefix = query.split("select")[1].split(",")[0].replace("\\t", " ")
        with open(f'{title}_{query_prefix}_execution_plan.json', 'w') as f:
            execution_plans = query_renderer.parser.from_query(query, conn_str)
            json.dump(execution_plans, f)
            run_query_flow(query, conn_str, title=title)

    run_query_flow(slow_queries, conn_str, title=f'{output_prefix}/all-slow-queries')


def get_queries(path):
    with open(path) as qf:
        queries = [q.strip() for q in qf.read().split(';') if q.strip()]
    res = []
    for q in queries:
        if "drop view" in q or "create view" in q:
            continue
        if 'select\n\ts_suppkey,\n\ts_name,\n\ts_address,' in q:
            q = "with revenue0 as (\nselect\n\tl_suppkey as supplier_no,\n\tsum(l_extendedprice * (1 - l_discount)) as total_revenue\nfrom\n\tlineitem\nwhere\n\tl_shipdate >= date '1993-03-01'\n\tand l_shipdate < date '1993-03-01' + interval '3' month\ngroup by l_suppkey)\n" + q

        res.append(q)
    return res


def get_optimized_queries(path):
    baseline_queries = get_queries(path)
    res = []
    for q in baseline_queries:

        # Change join order for query number 2
        if "part,\n\tsupplier,\n\tpartsupp,\n\t" in q:
            q = q.replace("part,\n\tsupplier,\n\tpartsupp,\n\t", "part,\n\tpartsupp,\n\tsupplier,\n\t")
        if "l_linestatus,\n\tl_returnflag" in q:
            q = q.replace("l_linestatus,\n\tl_returnflag", "l_returnflag,\n\tl_linestatus")
        res.append(q)
    return res


def run_query_flow(queries, conn_str, title):
    parser = postgres_parser.PostgresParser(is_compact=False, execute_query=True)
    query_renderer = query_vizualizer.QueryVizualizer(parser)
    flow_df = query_renderer.get_flow_df(queries, con_str=conn_str)
    flow_df.to_csv(f"{title}.csv")
    query_renderer.vizualize(flow_df,
                             title=title,
                             metrics=['actual_duration'],
                             plot_together=False)


def listify(val):
    if type(val) in [str, int, float, dict]:
        return [val]
    return val


def main():
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
    # benchmark_tpch(args.conn_str,
    #                args.repeats,
    #                args.run_statistics,
    #                args.generate_sankey)



if __name__ == '__main__':
    main()
