from operator import itemgetter

import numpy as np

try:
    from .db_parser import DBParser
except ImportError:
    # Support running doctests not as a module
    from db_parser import DBParser  # type: ignore

__all__ = ['PostgresParser']


def calc_precentage(series, comsum_series):
    return series / comsum_series * 100


def calc_ratio(df, column_a, column_b):
    def calc_row(this, other):
        if this == other:
            return 1
        elif this == 0 or other == 0:
            return np.inf
        else:
            return max([this, other]) / min([this, other])

    return df.apply(lambda x: calc_row(x[column_a], x[column_b]), axis=1)


class PostgresParser(DBParser):
    explain_prefix = 'EXPLAIN(COSTS, VERBOSE, FORMAT JSON)'
    explain_analyze_prefix = 'EXPLAIN(ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON)'
    query_prefix = None

    node_type_indicator = 'Node Type'
    next_operator_indicator = 'Plans'
    first_operator_indicator = 'Plan'
    filter_indicator = 'Filter'

    supported_metrics = frozenset([
        'Actual Rows', 'Actual Total Time', 'Plan Rows', 'Plan Width', 'Total Cost',
        'Actual Startup Time', 'Actual Loops', 'Shared Read Blocks',
        'Shared Hit Blocks', 'Shared Dirtied Blocks', 'Shared Written Blocks',
        'Local Hit Blocks', 'Local Dirtied Blocks', 'Local Read Blocks',
        'Local Written Blocks', 'Temp Written Blocks', 'Temp Read Blocks',
    ])
    verbose_ops = {'Hash', 'Gather', 'Gather Merge', 'Sort', 'WindowAgg', }

    description_dict = {
        'Append': 'Used in a UNION to merge multiple record sets by appending them together.',
        'Limit': 'Returns a specified number of rows from a record set.',
        'Hash Join': 'Joins to record sets by hashing one of them (using a Hash Scan).',
        'Aggregate': 'Groups records together based on a GROUP BY or aggregate function (e.g. sum()).',
        'Hashaggregate': 'Groups records together based on a GROUP BY or aggregate function (e.g. sum()). Hash Aggregate uses a hash to first organize the records by a key.',
        'Seq Scan': 'Finds relevant records by sequentially scanning the input record set. When reading from a table, Seq Scans (unlike Index Scans) perform a single read operation (only the table is read).',
        'Where': 'Filter relation to hold only relevant records.',
        'Having': 'Filter relation to hold only relevant records.',
        'Sort': 'Sorts a record set based on the specified sort key.',
        'Nested Loop': 'Merges two record sets by looping through every record in the first set and trying to find a match in the second set. All matching records are returned.',
        'Merge Join': 'Merges two record sets by first sorting them on a join key.',
        'Hash': 'Generates a hash table from the records in the input recordset. Hash is used by Hash Join.',
        'Index Scan': 'Finds relevant records based on an Index. Index Scans perform 2 read operations: one to read the index and another to read the actual value from the table.',
        'Bitmap Heap Scan': 'Searches through the pages returned by the Bitmap Index Scan for relevant rows.',
        'Bitmap Index Scan': 'Uses a Bitmap Index (index which uses 1 bit per page) to find all relevant pages. Results of this node are fed to the Bitmap Heap Scan.',
        'Index Only Scan': 'Finds relevant records based on an Index. Index Only Scans perform a single read operation from the index and do not read from the corresponding table.',
        'Gather': 'Collect relevant records from the workers.',
        'Gather Merge': 'Collect relevant records from the workers in ordered manner.',
        'Unique': 'Remove duplicated rows from a record set.',
        'WindowAgg': 'Calculate window function according to the OVER statements.',
        'Result': 'A Relation primitive',
        'Subquery Scan': 'A Subquery Scan is for scanning the output of a sub-query in the range table.'
    }

    def __init__(self, is_verbose=False, is_compact=False, execute_query=True):
        self.query_prefix = self.explain_analyze_prefix if execute_query else self.explain_prefix

        super().__init__(is_verbose, is_compact)

    @property
    def strategy_dict(self):
        return {
            'Limit': self.parse_limit,
            'Seq Scan': self.parse_scan,
            'Subquery Scan': self.parse_subquery,
            'Bitmap Heap Scan': self.parse_scan,
            'Bitmap Index Scan': self.parse_scan,
            'Index Scan': self.parse_scan,
            'Index Only Scan': self.parse_scan,
            'Append': self.parse_append,
            'Hash Join': self.parse_join,
            'Merge Join': self.parse_join,
            'Nested Loop': self.parse_join,
            'Aggregate': self.parse_aggregate,
            'Hashaggregate': self.parse_aggregate,
            'Hash': self.parse_hash,
            'Gather': self.parse_gather,
            'Gather Merge': self.parse_gather,
            'Sort': self.parse_sort,
            'Unique': self.parse_unique,
            'Result': self.parse_result,
            'WindowAgg': self.parse_window,
        }

    @DBParser.parse_default_decor
    def parse_limit(self, execution_node):
        """
        >>> p = PostgresParser(True)
        >>> p.parse_limit(1000, {"Node Type": "Limit", "Actual Rows": 5})
        ([{'source': 9999, 'target': 1000, 'operation_type': 'Limit', 'actual_rows': 5, 'label': 'LIMIT 5', 'label_metadata': 'LIMIT: 5'}], 9999)
        """
        return {
            'label': f"LIMIT {execution_node['Actual Rows']}",
            'label_metadata': f"LIMIT: {execution_node['Actual Rows']}",
        }

    @DBParser.parse_default_decor
    def parse_sort(self, execution_node):
        """
        >>> p = PostgresParser(True)
        >>> p.parse_sort(1000, {"Node Type": "Sort", "Sort Key": ["crew.title_id"], "Sort Method": "quicksort", "Sort Space Used": 128, "Sort Space Type": "Memory"})
        ([{'source': 9999, 'target': 1000, 'operation_type': 'Sort', 'label': 'SORT', 'label_metadata': "Sort Space Type: Memory\\nSort Space Used: 128\\nSort Method: quicksort\\nSort Key: ['crew.title_id']\\n"}], 9999)
        """

        return {
            'label': 'SORT',
            'label_metadata': f"Sort Space Type: {execution_node['Sort Space Type']}\n"
                              f"Sort Space Used: {execution_node['Sort Space Used']}\n"
                              f"Sort Method: {execution_node['Sort Method']}\n"
                              f"Sort Key: {itemgetter('Sort Key')(execution_node)}\n",
        }

    @DBParser.parse_default_decor
    def parse_append(self, execution_node):
        """
        >>> p = PostgresParser(True)
        >>> p.parse_append(1000, {"Node Type": "Append"})
        ([{'source': 9999, 'target': 1000, 'operation_type': 'Append', 'label': 'UNION ALL', 'label_metadata': ''}], 9999)
        """
        return {
            'label': 'UNION ALL',
            'label_metadata': '',
        }

    @DBParser.parse_default_decor
    def parse_window(self, execution_node):
        """
        >>> p = PostgresParser(True)
        >>> p.parse_window(1000, {"Node Type": "WindowAgg"})
        ([{'source': 9999, 'target': 1000, 'operation_type': 'WindowAgg', 'label': 'WINDOW', 'label_metadata': ''}], 9999)
        """
        return {
            'label': 'WINDOW',
            'label_metadata': '',
        }

    @DBParser.parse_default_decor
    def parse_unique(self, execution_node):
        """
        >>> p = PostgresParser(True)
        >>> p.parse_unique(1000, {"Node Type": "Unique"})
        ([{'source': 9999, 'target': 1000, 'operation_type': 'Unique', 'label': 'Unique', 'label_metadata': ''}], 9999)
        """
        return {
            'label': 'Unique',
            'label_metadata': '',
        }

    @DBParser.parse_default_decor
    def parse_result(self, execution_node):
        """
        >>> p = PostgresParser(True)
        >>> p.parse_result(1000, {"Node Type": "Result"})
        ([{'source': 9999, 'target': 1000, 'operation_type': 'Result', 'label': 'Result', 'label_metadata': ''}], 9999)
        """
        return {
            'label': 'Result',
            'label_metadata': '',
        }

    @DBParser.parse_default_decor
    def parse_gather(self, execution_node):
        """
        >>> p = PostgresParser(True)
        >>> p.parse_gather(1000, {"Node Type": "Gather"})
        ([{'source': 9999, 'target': 1000, 'operation_type': 'Gather', 'label': 'Gather', 'label_metadata': ''}], 9999)
        """
        return {
            'label': 'Gather',
            'label_metadata': '',
        }

    @DBParser.parse_default_decor
    def parse_hash(self, execution_node):
        """
        >>> p = PostgresParser(True)
        >>> p.parse_hash(1000, {"Node Type": "Hash", "Parent Relationship": "Inner"})
        ([{'source': 9999, 'target': 1000, 'operation_type': 'Hash', 'label': 'HASH', 'label_metadata': ''}], 9999)
        """
        return {
            'label': 'HASH',
            'label_metadata': '',
        }

    @DBParser.parse_default_decor
    def parse_join(self, execution_node):
        """
        >>> p = PostgresParser(True)

        >>> p.parse_join(1000, {"Node Type": "Nested Loop",  "Join Type": "Inner", "Join Filter": "(crew.title_id = titles.title_id)"})
        ([{'source': 9999, 'target': 1000, 'operation_type': 'Nested Loop', 'label': 'JOIN', 'label_metadata': "Join Filter ('Inner', '(crew.title_id = titles.title_id)')"}], 9999)

        >>> p.parse_join(1000, {"Node Type": "Hash Join",  "Join Type": "Inner", "Join Filter": "(crew.person_id = people.person_id)"})
        ([{'source': 9998, 'target': 1000, 'operation_type': 'Hash Join', 'label': 'JOIN', 'label_metadata': "Join Filter ('Inner', '(crew.person_id = people.person_id)')"}], 9998)
        """
        cond_key = [
            key for key in execution_node.keys(
            ) if 'Cond' in key or 'Join Filter' == key
        ]
        metadata = f"{cond_key[0]} {itemgetter('Join Type', cond_key[0])(execution_node)}" if cond_key else ''
        return {
            'label': 'JOIN',
            'label_metadata': metadata,
        }

    @DBParser.parse_filterable_node_decor
    def parse_scan(self):
        """
        >>> p = PostgresParser(True)
        >>> p.parse_scan(1000, {"Node Type": "Seq Scan", "Relation Name": "people", "Actual Rows": 3, "Filter": "people.age = 30", "Rows Removed by Filter": 3446258})
        ([{'source': 9999, 'target': 1000, 'operation_type': 'Where', 'actual_rows': 3, 'label': 'People*', 'label_metadata': 'Filter condition: people.age = 30'}, {'source': 9998, 'target': 9999, 'operation_type': 'Seq Scan', 'actual_rows': 3446261, 'label': 'People', 'label_metadata': ''}], 9998)
        """

        def parse_where(execution_node):
            return {
                'label': f'{execution_node["Relation Name"].title()}*',
                'label_metadata': f"Filter condition: {itemgetter('Filter')(execution_node)}",
                'operation_type': 'Where',
            }

        def parse_naive_scan(execution_node):
            res = {
                'label': execution_node.get('Index Name', execution_node.get('Relation Name', '')).title(),
                'label_metadata': ''
            }
            if 'Actual Rows' in execution_node:
                res['actual_rows'] = execution_node['Actual Rows'] + \
                    execution_node.get('Rows Removed by Filter', 0)
            return res

        yield parse_where
        yield parse_naive_scan

    @DBParser.parse_filterable_node_decor
    def parse_subquery(self):
        """
        >>> p = PostgresParser(True)
        >>> p.parse_subquery(1000, {"Node Type": "Subquery Scan", "Actual Rows": 3, "Filter": "people.age = 30", "Rows Removed by Filter": 3446258, "Alias": "a"})
        ([{'source': 9999, 'target': 1000, 'operation_type': 'Where', 'actual_rows': 3, 'label': 'a*', 'label_metadata': 'Filter condition: people.age = 30'}, {'source': 9998, 'target': 9999, 'operation_type': 'Subquery Scan', 'actual_rows': 3446261, 'label': 'a', 'label_metadata': ''}], 9998)
        """

        def parse_naive_sub_query(execution_node):
            res = {
                'label': execution_node['Alias'],
                'label_metadata': '',
            }
            if 'Actual Rows' in execution_node:
                res['actual_rows'] = execution_node['Actual Rows'] + \
                    execution_node.get('Rows Removed by Filter', 0)
            return res

        def parse_where_sub_query(execution_node):
            return {
                'label': f'{execution_node["Alias"]}*',
                'label_metadata': f"Filter condition: {itemgetter('Filter')(execution_node)}",
                'operation_type': 'Where',
            }

        yield parse_where_sub_query
        yield parse_naive_sub_query

    @DBParser.parse_filterable_node_decor
    def parse_aggregate(self):
        """
        >>> p = PostgresParser(True)
        >>> p.parse_aggregate(9996, {"Node Type": "Hashaggregate", "Actual Rows": 3, "Filter": "(count(1) > 5)", "Rows Removed by Filter": 34,  "Group Key": ["titles.genres"], "Output": ["titles.genres"]})
        ([{'source': 9999, 'target': 9996, 'operation_type': 'Having', 'actual_rows': 3, 'label': 'AGG*', 'label_metadata': 'Filter condition: (count(1) > 5)'}, {'source': 9998, 'target': 9999, 'operation_type': 'Hashaggregate', 'actual_rows': 37, 'label': 'AGG', 'label_metadata': "Group key: ['titles.genres']\\nOutput: ['titles.genres']"}], 9998)
        """

        # todo FIX STR repreesntation
        def parse_having(execution_node):
            return {
                'label': 'AGG*',
                'label_metadata': f"Filter condition: {itemgetter('Filter')(execution_node)}",
                'operation_type': 'Having',
            }

        def parse_naive_aggregate(execution_node):
            res = {
                'label': 'AGG',
                'label_metadata': f"Group key: {itemgetter('Group Key')(execution_node)}\n"
                                  f"Output: {itemgetter('Output')(execution_node)}"
            }
            if 'Actual Rows' in execution_node:
                res['actual_rows'] = execution_node['Actual Rows'] + \
                    execution_node.get('Rows Removed by Filter', 0)
            return res

        yield parse_having
        yield parse_naive_aggregate

    def enrich_stats(self, df):

        df['estimated_cost'] = df['total_cost']
        df['redundent_operation'] = False

        if 'actual_startup_time' in df.columns:
            df['actual_startup_duration'] = df['actual_startup_time']
            df['actual_duration'] = df['actual_total_time']

        for i, row in df.iterrows():

            relevant_ops = df.query(f"target=={row['source']}")
            if 'actual_startup_time' in df.columns:
                df.loc[i, 'actual_duration'] = row.actual_total_time if relevant_ops.empty else row.actual_total_time - \
                    max(relevant_ops.actual_total_time)
                df.loc[
                    i, 'actual_startup_duration'] = row.actual_startup_time if relevant_ops.empty else row.actual_startup_time - \
                    max(
                    relevant_ops.total_cost)

            df.loc[i, 'estimated_cost'] = row.total_cost if relevant_ops.empty else row.total_cost - \
                max(relevant_ops.total_cost)

            if any(op in row.label.split(' ') for op in self.label_replacement.keys()):
                df.loc[i, 'label'] = self.label_replacement[row.label].join(
                    relevant_ops.label,
                )
            if row.label == 'Unique' and 'actual_startup_time' in df.columns:
                df.loc[i, 'redundent_operation'] = (
                    sum(relevant_ops.actual_rows) == row.actual_rows
                )

        df['estimated_cost_pct'] = calc_precentage(
            df['estimated_cost'], df['total_cost'])
        if 'actual_startup_time' in df.columns:
            df['actual_duration_pct'] = calc_precentage(
                df['actual_duration'], df['actual_total_time'])
            df['actual_plan_rows_ratio'] = calc_ratio(
                df, 'actual_rows', 'plan_rows')

        df['label_metadata'] = df.operation_type.map(
            lambda s: f"\nDescription: {self.description_dict.get(s,'')}" if s else '') + df.label_metadata
        return df


if __name__ == '__main__':
    import doctest

    doctest.testmod()
