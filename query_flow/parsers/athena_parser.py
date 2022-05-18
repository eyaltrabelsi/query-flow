import logging
from operator import itemgetter

import numpy as np

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

    node_type_extractor = lambda _, node: node['name'].split("(")[0]
    next_operator_indicator = 'children'
    filter_indicator = lambda _, node: 'Filtered' in node.get('details', '')
    last_fragment_id = None
    supported_metrics = frozenset([
        'nodeCpuTime', 'nodeCpuFraction', 'nodeOutputRows', 'nodeOutputDataSize'
    ])
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
    'Sort': 'Sorts a record set based on the specified sort key.'
    }

    def __init__(self, is_compact=False, execute_query=True):
        self.query_prefix = self.explain_analyze_prefix if execute_query else self.explain_prefix

        super().__init__(is_compact)

    def not_implemented(self, *args, **kwargs):
        pass

    def normalize_metric(self, metric):
        return metric

    @property
    def strategy_dict(self):
        # TODO check
        return {
            'PartialSort': self.parse_base,
            'TopN': self.parse_base,
            'Unnest': self.parse_base,
            'TopNPartial': self.parse_base,
            'Limit': self.parse_base,
            'LimitPartial': self.parse_base,
            'DistinctLimit': self.parse_base,
            'DistinctLimitPartial': self.parse_base,
            'Distinct': self.parse_base,
            'TableScan': self.parse_scan,
            'ScanFilterProject': self.parse_scan,
            'ScanProject': self.parse_scan,
            'Values': self.parse_scan,
            'Window': self.parse_base,
            'LocalExchange': self.parse_base,
            'Project': self.parse_scan,
            'RemoteSource': self.parse_remote_source,
            'Filter': self.parse_base,
            'GroupId': self.parse_base,
            'LocalMerge': self.parse_base,
            'InnerJoin': self.parse_base,
            'Aggregate': self.parse_base,
            'Gather Merge': self.parse_base,
            'Sort': self.parse_base
        }

    def from_query(self, query, con_str): #TODO add boto
        pass

    @DBParser.parse_filterable_node_decor
    def parse_scan(self):
        """
        >>> p = AthenaParser(True)
        >>> p.parse_scan(1000, {"name" : "ScanProject", "identifier" : "[table = awsdatacatalog:HiveTableHandle{schemaName=temp_tables, tableName=app_bac_repo, analyzePartitionValues=Optional.empty}, grouped = false]",  "distributedNodeStats" : {"nodeCpuTime" : "31.98s","nodeCpuFraction" : "0.78%","nodeOutputRows" : "9202005 rows", "nodeOutputDataSize" : "360.19MB", "operatorInputRowsStats" : [ {"nodeInputRows" : "161438.68", "nodeInputRowsStdDev" : "6.23%"})
        """

        def parse_where(execution_node):
            identifier = f"-{execution_node['identifier'].split('tableName=')[1].split(',')[0]}" if execution_node["identifier"] not in ("[]","") else ""
            return {
                'label': f"{execution_node.get('name')}{identifier}*",
                'label_metadata': f"identifier: {execution_node['identifier']}\n details: {execution_node['details']}",
                'operation_type': 'Where', #todo add calculations FROM DETAILS INPUTS AND FILTERS
            }

        def parse_naive_scan(execution_node):
            identifier = f"-{execution_node['identifier'].split('tableName=')[1].split(',')[0]}" if execution_node["identifier"] not in ("[]","") else ""
            res = {
                'label': f"{execution_node.get('name')}{identifier}",
                'label_metadata': f"identifier: {execution_node['identifier']}\n details: {execution_node['details']}"
            }
            # TODO partitions
            return res

        yield parse_where
        yield parse_naive_scan

    @DBParser.parse_default_decor
    def parse_base(self, execution_node):
        res = {
            'label': self.node_type_extractor(execution_node),
            'label_metadata': f"identifier: {execution_node['identifier']}\n details: {execution_node['details']}"
        }
        return res

    @DBParser.parse_default_decor
    def parse_remote_source(self, execution_node):
        res = {
            'label': f'remote_source {execution_node.get("identifier")}',
            'label_metadata': ""
        }
        return res

    def add_supported_metrics(self, parsed_node, execution_node):
        for metric in self.supported_metrics:
            if metric in execution_node.get('distributedNodeStats'):
                normalized_key = metric.replace(" ", "_")
                parsed_node[normalized_key] = execution_node.get('distributedNodeStats')[metric]
        return parsed_node

    @staticmethod
    def normalize_data_size(size_str):
        scale_dict = {"B": 1.0/3**10, ".kB": 1.0/2**10, ".MB": 1.0, ".GB": 1.0 * 2**10, ".TB": 1.0 * 2**10}
        scale = ''.join(i for i in size_str if not i.isdigit())
        number = float(''.join(i for i in size_str if i.isdigit()))

        return number * scale_dict[scale]

    @staticmethod
    def normalize_cpu_time(time_str):
        scale_dict = {".ns": 1.0 / 3 ** 10, ".ms": 1.0 / 2 ** 10, ".s": 1.0, ".m": 1.0 * 60, '.h': 1.0 * 60 * 60}
        scale = ''.join(i for i in time_str if not i.isdigit())
        number = float(''.join(i for i in time_str if i.isdigit()))
        return number * scale_dict[scale]

    def enrich_stats(self, df):

        df["redundent_operation"] = False
        df["nodeOutputRows"] = df["nodeOutputRows"].map(lambda x: ''.join(i for i in x if i.isdigit()))
        df["nodeOutputDataSize"] = df["nodeOutputDataSize"].map(AthenaParser.normalize_data_size)
        df["nodeCpuTime"] = df["nodeCpuTime"].map(AthenaParser.normalize_cpu_time)
        return df

    def clean_cache(self):
        logging.warning('Currently cache cleaning is not supported')


if __name__ == '__main__':
    import doctest
    doctest.testmod()
