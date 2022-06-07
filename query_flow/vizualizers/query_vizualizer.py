import collections

import numpy as np
import pandas as pd
from plotly.offline import iplot, plot

try:
    from query_flow.utils.coloring_utils import color_range, sample_colors
    from query_flow.utils.misc import listify
except ImportError:

    # Support running doctests not as a module
    from query_flow.utils.coloring_utils import color_range, sample_colors  # type: ignore
    from query_flow.utils.misc import listify

__all__ = ['QueryVizualizer']


class QueryVizualizer:
    columns_pks = frozenset(
        [
            'source',
            'target',
            'label',
            'query_hash',
            'node_hash',
            'label_metadata',
            'operation_type',
            'redundent_operation',
        ]
    )

    supported_metrics = {
        'actual_rows': ' Rows',
        'nodeOutputRows': ' Rows',
        'nodeOutputDataSize': ' GB',
        'nodeCpuTime': ' Seconds',
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

    def get_flow_df(self, queries, con_str):
        execution_plans = [self.parser.from_query(query, con_str) for query in listify(queries)]
        return self.parser.parse(execution_plans)

    def _enrich_colors(self, df, metrics):
        # Apply basic coloring for queries links
        queries_base_link_colors = sample_colors(df.query_hash.nunique())
        df['color_link'] = 'silver'
        if len(queries_base_link_colors) > 1:
            df['color_link'] = (
                df['query_hash'].astype('category').cat.codes.map(lambda code: queries_base_link_colors[code])
            )

        # Adjusting luminance according to the number of metrics
        for color in df['color_link'].unique():
            adjusted_colors = list(color_range(color, len(metrics)))
            df.loc[df['color_link'] == color, 'color_link'] = (
                df['variable'].astype('category').cat.codes.map(lambda code: adjusted_colors[code])
            )

        # Apply special case coloring for queries link
        what_case = df.apply(
            lambda x: QueryVizualizer._get_case(x['variable'], x['value'], x['redundent_operation']), axis=1
        )
        df.loc[what_case != 'default', 'color_link'] = what_case.map(self.special_cases_link_colors)

        # Apply basic coloring for queries nodes
        if self.is_colored_nodes:
            df['color_node'] = df['operation_type'].map(self.node_colors)
        else:
            df['color_node'] = 'black'
        return df

    def vizualize(self, dfs, metrics, title, open_=True):
        metrics = metrics or self.default_metrics.keys()
        assert all(
            metric in self.supported_metrics.keys() for metric in metrics
        ), f'The only supported metrics are {self.supported_metrics}'

        flow_df = self._prepare_dfs_for_sankey(dfs, metrics)
        self._plot_sankey(flow_df, metrics, title, open_)

    def _plot_sankey(self, flow_df, metrics, title, open_):
        data_trace = dict(
            type='sankey',
            orientation='h',
            valueformat=',',
            valuesuffix=flow_df['variable'].map(self.supported_metrics),
            node=dict(
                pad=200,
                label=flow_df.drop_duplicates(['node_hash'])['label'] if self.parser.is_compact else flow_df['label'],
                color=flow_df['color_node'],
            ),
            link=dict(
                source=flow_df['source'],
                target=flow_df['target'],
                value=flow_df['value'].map(np.int64).replace(0, 1),
                label=flow_df['label_metadata'],
                color=flow_df['color_link'],
            ),
        )
        layout = dict(
            title=f"{title}-{','.join(metrics)}",
            font=dict(size=10),
            height=2000,  # TODO fix this too look good on all type of sizes
            updatemenus=[
                dict(
                    y=0.6,
                    buttons=[
                        dict(
                            label='Vertical',
                            method='restyle',
                            args=['orientation', 'v'],
                        ),
                        dict(
                            label='Horizontal',
                            method='restyle',
                            args=['orientation', 'h'],
                        ),
                    ],
                ),
            ],
        )
        if open_:  # TODO change this to two functions
            plot(
                dict(data=[data_trace], layout=layout),
                validate=False,
                filename=f"{title}-{','.join(metrics)}.html",
                image_width=8000,
                auto_open=False,
            )
        else:
            iplot(
                dict(data=[data_trace], layout=layout),
                validate=False,
                filename=f"{title}-{','.join(metrics)}.html",
            )

    def _prepare_dfs_for_sankey(self, flow_dfs, metrics):
        if isinstance(flow_dfs, collections.Sequence):
            flow_dfs = pd.concat(flow_dfs)
        flow_dfs = flow_dfs.melt(id_vars=self.columns_pks, value_vars=metrics)
        return self._enrich_colors(flow_dfs, metrics)

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
