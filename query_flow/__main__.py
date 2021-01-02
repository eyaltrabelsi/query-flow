import argparse
import sys
from typing import Optional
from typing import Sequence

from query_flow.parsers import postgres_parser
from query_flow.vizualizers import query_vizualizer

if sys.version_info < (3, 8):  # pragma: no cover (<PY38)
    import importlib_metadata
else:  # pragma: no cover (PY38+)
    import importlib.metadata as importlib_metadata


def main(argv: Optional[Sequence[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(prog='pre-commit')

    # https://stackoverflow.com/a/8521644/812183
    version = importlib_metadata.version('query-flow')
    parser.add_argument(
        '-V', '--version',
        action='version',
        version=f'%(prog)s {version}',
    )

    subparsers = parser.add_subparsers(dest='command')
    visualize_parser = subparsers.add_parser(
        'visualize',
        help='TODO.md',
    )

    visualize_parser.add_argument(
        '--con_str', action='store_true',
        help='TODO.md',
    )

    visualize_parser.add_argument(
        '--execution_plans', action='store_true',
        help='TODO.md',
    )

    visualize_parser.add_argument(
        '--queries', action='store_true',
        help='TODO.md',
    )

    visualize_parser.add_argument(
        '--title', action='store_true',
        help='TODO.md',
    )

    visualize_parser.add_argument(
        '--metrics', action='store_true',
        help='TODO.md',
    )

    visualize_parser.add_argument(
        '--parser', action='store_true',
        help='TODO.md',
    )

    visualize_parser.add_argument(
        '--is_verbose', action='store_true',
        help='TODO.md',
    )

    visualize_parser.add_argument(
        '--is_compact', action='store_true',
        help='TODO.md',
    )

    visualize_parser.add_argument(
        '--execute_query', action='store_true',
        help='TODO.md',
    )
    return 1


def main2():
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
        flow_df, title='Missing Records in Where Clause', metrics=['actual_duration'],
        open_=False,
    )


if __name__ == '__main__':
    exit(main2())

    # exit(main())
