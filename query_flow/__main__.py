import argparse
import sys
import traceback
from typing import Optional
from typing import Sequence

from query_flow.parsers import athena_parser
from query_flow.parsers import postgres_parser
from query_flow.vizualizers import query_vizualizer

if sys.version_info < (3, 8):  # pragma: no cover (<PY38)
    import importlib_metadata
else:  # pragma: no cover (PY38+)
    import importlib.metadata as importlib_metadata


def visualize(args):
    queries = [open(p).read() for p in args.queries]
    parser = postgres_parser.PostgresParser(is_compact=args.is_compact,
                                            execute_query=args.execute_query)
    query_renderer = query_vizualizer.QueryVizualizer(parser)
    flow_df = query_renderer.get_flow_df(queries, con_str=args.con_str)
    query_renderer.vizualize(flow_df, title=args.title, metrics=args.metrics, open_=True)


def main(argv: Optional[Sequence[str]] = None) -> int:

    parser = argparse.ArgumentParser(prog='query-flow')

    # https://stackoverflow.com/a/8521644/812183
    version = importlib_metadata.version('query-flow')
    parser.add_argument('-V', '--version', action='version', version=f'%(prog)s {version}')

    subparsers = parser.add_subparsers(dest='command')
    visualize_parser = subparsers.add_parser(
        'visualize', help='QueryFlow, is a query visualization tool that provides insights into common problems in your SQL query.')
    visualize_parser.add_argument('--con_str', action='store', help='Connection string to the database')
    visualize_parser.add_argument('--queries', nargs='+', help='Paths to queries to be used')
    visualize_parser.add_argument('--title', action='store_true', default='', help='Title of the visualization')
    visualize_parser.add_argument('--metrics', nargs='+', help='Metrics to be visualized.')
    visualize_parser.add_argument('--parser', action='store_true', help='Which Database should be used')
    visualize_parser.add_argument('--is_compact', action='store_true', default=False,
                                  help='Should represent query in a more compact manner')
    visualize_parser.add_argument('--execute_query', action='store_false', default=True,
                                  help='Should run the query for more accurate statistics')

    args = parser.parse_args(argv)
    try:
        if args.command == 'visualize':
            visualize(args)
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        return 0
    else:
        return 1


if __name__ == '__main__':
    # exit(main())
    #             limit looks wierd       limit looks wierd
    for plan in ['execution_plan_1.json', 'execution_plan_2.json',
                 'execution_plan_3.json', 'execution_plan_4.json', 'execution_plan_5.json', 'execution_plan_6.json', 'execution_plan_7.json', 'execution_plan_8.json']:
        execution_plan = open(f'../tests/parsers/data/athena/parse/{plan}').read()


        # conn_str = "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}"
        p = athena_parser.AthenaParser(execute_query=False)
        flow_df = p.parse([execution_plan])
        query_renderer = query_vizualizer.QueryVizualizer(p)
        # query_renderer.vizualize(flow_df, title=plan, metrics=["nodeOutputDataSize"], open_=True)
        query_renderer.vizualize(flow_df, title=plan, metrics=['nodeCpuTime'], open_=True)
