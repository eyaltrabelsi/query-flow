try:
    from .coloring_utils import sample_colors, color_range
    from .dataframe_sankey_vizualizer import DataFrameSankeyVizualizer
except ImportError:
    # Support running doctests not as a module
    from dataframe_sankey_vizualizer import DataFrameSankeyVizualizer  # type: ignore
    from coloring_utils import sample_colors, color_range  # type: ignore

__all__ = ['QueryVizualizer']


def listify(val):
    if type(val) in [str, int, float, dict]:
        return [val]
    return val


class QueryVizualizer(DataFrameSankeyVizualizer):
    columns_pks = frozenset([
        'source', 'target', 'label', 'query_hash', 'node_hash',
        'label_metadata', 'operation_type', 'redundent_operation',
    ])

    supported_metrics = {
        'actual_rows': ' Rows',
        'actual_startup_duration': ' Seconds',
        'actual_duration': ' Seconds',
        'actual_duration_pct': ' Percent',
        'estimated_cost': ' Units',
        'estimated_cost_pct': ' Percent',
        'plan_rows': 'Rows',
    }

    default_metrics = {'actual_rows': ' Rows', 'plan_rows': 'Rows'}
    node_colors = {
        'Aggregate': 'purple',
        'Having': 'mediumpurple',
        'Hashaggregate': 'purple',
        'Join': 'mediumseagreen',
        'Hash Join': 'mediumseagreen',
        'Append': 'olivedrab',
        'Scan': 'blue',
        'Seq Scan': 'blue',
        'Limit': 'khaki',
        'Nested Loop': 'mediumseagreen',
        'Where': 'deepskyblue',
    }
    special_cases_link_colors = {'empty': 'red', 'redundant': 'coral'}

    def __init__(self, parser, is_colored_nodes=False, node_colors=None):
        super().__init__()
        self.parser = parser
        self.is_colored_nodes = is_colored_nodes

        if node_colors:
            self.node_colors = node_colors

    def get_flow_df(self, queries, con_str, should_log=False):
        execution_plans = [self.parser.from_query(query, con_str, should_log)
                           for query in listify(queries)]
        return self.parser.parse(execution_plans)

    def _enrich_colors(self, df, metrics):
        # Apply basic coloring for queries links
        queries_base_link_colors = sample_colors(df.query_hash.nunique())
        df['color_link'] = 'silver'
        if len(queries_base_link_colors) > 1:
            df['color_link'] = df['query_hash'].astype('category').cat.codes.map(
                lambda code: queries_base_link_colors[code]
            )

        # Adjusting luminance according to the number of metrics
        for color in df['color_link'].unique():
            adjusted_colors = list(color_range(color, len(metrics)))
            df.loc[df['color_link'] == color, 'color_link'] = df['variable'].astype('category').cat.codes.map(
                lambda code: adjusted_colors[code]
            )

        # Apply special case coloring for queries link
        what_case = df.apply(lambda x: QueryVizualizer._get_case(
            x['variable'], x['value'], x['redundent_operation']), axis=1)
        df.loc[what_case != 'default', 'color_link'] = what_case.map(
            self.special_cases_link_colors)

        # Apply basic coloring for queries nodes
        if self.is_colored_nodes:
            df['color_node'] = df['operation_type'].map(self.node_colors)
        else:
            df['color_node'] = 'black'
        return df

    @staticmethod
    def _get_case(metric, value, redundent_operation):
        """
        >>> QueryVizualizer._get_case("actual_rows", 0, True)
        'redundant'

        >>> QueryVizualizer._get_case("actual_rows", 0, False)
        'empty'

        >>> QueryVizualizer._get_case("actual_rows", 2, False)
        'default'

        >>> QueryVizualizer._get_case("cost", 0, False)
        'default'
        """

        if redundent_operation:
            return 'redundant'
        elif metric == 'actual_rows' and value == 0:
            return 'empty'
        else:
            return 'default'


if __name__ == '__main__':
    import doctest
    doctest.testmod()
