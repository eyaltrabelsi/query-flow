import logging
import sys
import time
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from sqlalchemy import create_engine
from datetime import datetime

from query_flow.parsers import postgres_parser
from query_flow.vizualizers import query_vizualizer

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.getLogger('matplotlib').setLevel(logging.WARNING)


def listify(val):
    if type(val) in [str, int, float, dict]:
        return [val]
    return val


def run_query_flow(queries, conn_str, title):
    parser = postgres_parser.PostgresParser(is_compact=False, execute_query=True)
    query_renderer = query_vizualizer.QueryVizualizer(parser)
    flow_df = query_renderer.get_flow_df(queries, con_str=conn_str)
    flow_df.to_csv(f"{title}.csv")
    query_renderer.vizualize(flow_df,
                             title=title,
                             metrics=['actual_duration'],
                             plot_together=False)


def get_queries(path):
    with open(path) as qf:
        return [q.strip() for q in qf.read().split(';') if q.strip()]


def get_all_combinations(lst):
    import itertools
    combs = []

    for i in range(1, len(lst) + 1):
        combs.extend([list(x) for x in itertools.combinations(lst, i)])
    return combs


def check_change(con, queries, changes, revert_changes, more_then, times=2):
    for change in changes:
        con.execute(change)
    start = time.time()
    for j in range(times):
        for i, q in enumerate(queries):
            con.execute(q.replace("%", "%%"))
    duration = (time.time() - start) / times
    print(f"{datetime.now()}: {change} took {duration}")
    if revert_changes:
        for revert_change in revert_changes:
            con.execute(revert_change)
    if duration < more_then * 0.9:
        return duration


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

    # queries = [q for i, q in enumerate(get_queries(f"/tmp/{args.conn_str}/queries/queries.sql"))]
    queries = ["""select

	l_returnflag,

	l_linestatus,

	sum(l_quantity) as sum_qty,

	sum(l_extendedprice) as sum_base_price,

	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,

	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,

	avg(l_quantity) as avg_qty,

	avg(l_extendedprice) as avg_price,

	avg(l_discount) as avg_disc,

	count(*) as count_order

from

	lineitem

where

	l_shipdate <= date '1998-12-01' - interval '94' day

group by

	l_returnflag,

	l_linestatus

order by

	l_returnflag,

	l_linestatus
    """
               ]
    conn_str = f"postgresql://localhost/{args.conn_str}"
    #
    #     optimizations = []
    #     res = {}
    engine = create_engine(conn_str)
    connection = engine.raw_connection()
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()
    cursor.execute("VACUUM FULL")
    with create_engine(conn_str).connect() as con:

        potential_optimizations = [
            (('CREATE INDEX line_4 ON lineitem using brin(l_shipdate)'), ('DROP INDEX line_4')),
            # (('CREATE INDEX line_5 ON lineitem using brin(l_returnflag, l_linestatus)'), ('DROP INDEX line_5')),
            # (('CREATE INDEX line_6 ON lineitem using brin(l_shipdate, l_returnflag, l_linestatus)'),
            #  ('DROP INDEX line_6'))

        ]

        for i, q in enumerate(queries):
            start = time.time()
            con.execute(q.replace("%", "%%"))
            expected_time = time.time() - start
            print(f"query {i}: took {expected_time}")
            run_query_flow([q], conn_str, f"{i}")

        optimizations = []
        for opt, revert in potential_optimizations:
            ans = check_change(con, queries, listify(opt), listify(revert), expected_time, args.repeats)
            run_query_flow([q], conn_str, "optimized")
            if ans:
                optimizations.append((opt, revert))


if __name__ == '__main__':
    main()
