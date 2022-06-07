import logging
from operator import itemgetter

import numpy as np
from sqlalchemy.engine import create_engine

try:
    from .db_parser import DBParser
except ImportError:
    # Support running doctests not as a module
    from db_parser import DBParser  # type: ignore

__all__ = ['AthenaParser']


class AthenaParser(DBParser):
    explain_prefix = 'EXPLAIN (FORMAT JSON)'
    explain_analyze_prefix = 'EXPLAIN ANALYZE (FORMAT JSON)'
    query_prefix = None
    next_operator_indicator = 'children'
    last_fragment_id = None
    supported_metrics = frozenset(['nodeCpuTime', 'nodeCpuFraction', 'nodeOutputRows', 'nodeOutputDataSize'])
    redundent_operation_names = frozenset(['Where', 'Filter'])
    verbose_ops = {}

    description_dict = {
        'PartialSort': 'Sorts a record set based on the specified sort key.',
        'TopN': '',
        'TopNPartial': '',
        'Unnest': '',
        'Limit': 'Returns a specified number of rows from a record set.',
        'LimitPartial': 'Returns a specified number of rows from a record set.',
        'DistinctLimit': '',
        'DistinctLimitPartial': '',
        'Distinct': '',
        'TableScan': '',
        'ScanFilterProject': '',
        'ScanProject': '',
        'Values': '',
        'Window': '',
        'LocalExchange': '',
        'Project': '',
        'RemoteSource': '',
        'Filter': '',
        'GroupId': '',
        'LocalMerge': '',
        'InnerJoin': '',
        'Aggregate': '',
        'Gather Merge': '',
        'Sort': 'Sorts a record set based on the specified sort key.',
    }

    def __init__(self, is_compact=False, execute_query=True):
        self.query_prefix = self.explain_analyze_prefix if execute_query else self.explain_prefix
        assert execute_query, "AthenaParser doesn't support logical plans"
        super().__init__(is_compact)

    def node_type_extractor(self, node):
        return node['name'].split('(')[0]

    def execution_plan_extractor(self, node):
        return eval(node.replace('Query Plan', ''))

    def filter_indicator(self, node):
        return 'Filtered' in node.get('details', '')

    def normalize_metric(self, metric):
        return metric

    def from_query(self, query, con_str):
        with create_engine(con_str).connect() as con:
            # SQLALCHEMY doesn't handle % as a regular SQL client so one need to add additional %
            explain_analyze_query = f"{self.query_prefix} {query.replace('%', '%%')}"

            # Grab the execution plan string in case its returned as a multiple rows
            execution_plan = "\n".join([line[0] for line in con.execute(explain_analyze_query).fetchall()])

            return self.execution_plan_extractor(execution_plan)

    @property
    def strategy_dict(self):
        return {
            'TableScan': self.parse_scan,
            'ScanFilterProject': self.parse_scan,
            'ScanProject': self.parse_scan,
            'Values': self.parse_scan,
            'Project': self.parse_scan,
            'RemoteSource': self.parse_remote_source,
        }

    @DBParser.parse_filterable_node_decor
    def parse_scan(self):
        def parse_where(execution_node):
            identifier = (
                f"-{execution_node['identifier'].split('tableName=')[1].split(',')[0]}"
                if execution_node['identifier'] not in ('[]', '')
                else ''
            )
            return {
                'label': f"{execution_node.get('name')}{identifier}*",
                'label_metadata': f"identifier: {execution_node['identifier']}\n details: {execution_node['details']}",
                'operation_type': 'Where',
            }

        def parse_naive_scan(execution_node):
            identifier = (
                f"-{execution_node['identifier'].split('tableName=')[1].split(',')[0]}"
                if execution_node['identifier'] not in ('[]', '')
                else ''
            )
            res = {
                'label': f"{execution_node.get('name')}{identifier}",
                'label_metadata': f"identifier: {execution_node['identifier']}\n details: {execution_node['details']}",
            }
            if "Filtered" in execution_node["details"] and "0.00" not in execution_node["details"]:
                relevant_line = list(filter(lambda x: x.startswith("Input"), execution_node['details'].split("\n")))[0]
                # TODO fix the rest of the columns
                _, row_amount, row_meric_name, size, _, _ = relevant_line.split(" ")
                res['nodeOutputRows'] = f"{row_amount} {row_meric_name}"
                res['nodeOutputDataSize'] = size[1:-2]
            return res

        yield parse_where
        yield parse_naive_scan

    @DBParser.parse_default_decor
    def parse_base(self, execution_node):
        res = {
            'label': self.node_type_extractor(execution_node),
            'label_metadata': f"identifier: {execution_node['identifier']}\n details: {execution_node['details']}",
        }
        return res

    @DBParser.parse_default_decor
    def parse_remote_source(self, execution_node):
        res = {'label': f'remote_source {execution_node.get("identifier")}', 'label_metadata': ''}
        return res

    def add_supported_metrics(self, parsed_node, execution_node):
        for metric in self.supported_metrics:
            if 'distributedNodeStats' in execution_node and metric in execution_node.get('distributedNodeStats'):
                if metric not in parsed_node:
                    parsed_node[metric] = execution_node.get('distributedNodeStats')[metric]
        return parsed_node

    @staticmethod
    def normalize_data_size(size_str):
        scale_dict = {'B': 1.0 / 3 ** 10, '.kB': 1.0 / 2 ** 10, '.MB': 1.0, '.GB': 1.0 * 2 ** 10, '.TB': 1.0 * 2 ** 10}
        scale = ''.join(i for i in size_str if not i.isdigit())
        number = float(''.join(i for i in size_str if i.isdigit()))

        return number * scale_dict[scale]

    @staticmethod
    def normalize_cpu_time(time_str):
        scale_dict = {'.ns': 1.0 / 3 ** 10, '.ms': 1.0 / 2 ** 10, '.s': 1.0, '.m': 1.0 * 60, '.h': 1.0 * 60 * 60}
        scale = ''.join(i for i in time_str if not i.isdigit())
        number = float(''.join(i for i in time_str if i.isdigit()))
        return number * scale_dict[scale]

    def enrich_stats(self, df):

        df['redundent_operation'] = False
        df['nodeOutputRows'] = df['nodeOutputRows'].map(lambda x: ''.join(i for i in x if i.isdigit()))
        df['nodeOutputDataSize'] = df['nodeOutputDataSize'].map(AthenaParser.normalize_data_size)
        df['nodeCpuTime'] = df['nodeCpuTime'].map(AthenaParser.normalize_cpu_time)

        for i, row in df.iterrows():
            relevant_ops = df.query(f"target=={row['source']} & query_hash=='{row['query_hash']}'")
            if row.operation_type in self.redundent_operation_names:
                df.loc[i, 'redundent_operation'] = sum(relevant_ops.actual_rows) == row.actual_rows

            if any(op in row.label.split(' ') for op in self.label_replacement.keys()):
                df.loc[i, 'label'] = self.label_replacement[row.label].join(relevant_ops.label)

        return df


if __name__ == '__main__':
    import doctest

    doctest.testmod()
