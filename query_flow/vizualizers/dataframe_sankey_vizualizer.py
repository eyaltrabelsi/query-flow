import collections
from abc import ABC
from abc import abstractmethod

import numpy as np
import pandas as pd
from plotly.offline import iplot
from plotly.offline import plot

__all__ = ['DataFrameSankeyVizualizer']


class DataFrameSankeyVizualizer(ABC):
    link_defaults = None
    node_defaults = None

    def __init__(self):
        pass

    @property
    @abstractmethod
    def supported_metrics(self):
        pass

    @property
    @abstractmethod
    def default_metrics(self):
        pass

    @property
    @abstractmethod
    def columns_pks(self):
        pass

    @property
    def special_cases_link_colors(self):
        pass

    def vizualize(self, dfs, metrics, title, open_=True, plot_together=True):
        metrics = metrics or self.default_metrics.keys()
        assert all(
            metric in self.supported_metrics.keys() for metric in
            metrics
        ), f'The only supported metrics are {self.supported_metrics}'

        if plot_together:
            flow_df = self._prepare_dfs_for_sankey(dfs, metrics)
            self._plot_sankey(flow_df, metrics, title, open_)
        else:
            for metric in metrics:
                flow_df = self._prepare_dfs_for_sankey(dfs, [metric])
                self._plot_sankey(flow_df, [metric], title, open_)

    def _plot_sankey(self, flow_df, metrics, title, open_):
        data_trace = dict(
            type='sankey',
            orientation='h',
            valueformat=',',
            valuesuffix=flow_df['variable'].map(self.supported_metrics),
            node=dict(
                pad=200,
                label=flow_df.drop_duplicates(['node_hash'])[
                    'label'] if self.parser.is_compact else flow_df['label'],
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
            height=750,
            updatemenus=[
                dict(
                    y=0.6,
                    buttons=[
                        dict(
                            label='Horizontal', method='restyle',
                            args=['orientation', 'h'],
                        ),
                        dict(
                            label='Vertical', method='restyle',
                            args=['orientation', 'v'],
                        ),
                    ],
                ),
            ],
        )
        if open_:
            plot(
                dict(data=[data_trace], layout=layout),
                validate=False,
                filename=f"{title}-{','.join(metrics)}.html",
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

    @abstractmethod
    def _enrich_colors(self, dfs, metrics):
        pass


if __name__ == '__main__':
    pass
