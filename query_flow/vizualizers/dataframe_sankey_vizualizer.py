import json
from abc import ABC
from abc import abstractmethod
from pathlib import Path

import numpy as np
import pandas as pd
from plotly.offline import iplot
from plotly.offline import plot

__all__ = ['DataFrameSankeyVizualizer']


class DataFrameSankeyVizualizer(ABC):
    link_defaults = None
    node_defaults = None

    def __init__(self, config_path='example_config.json'):
        self.config = self._parse_config(config_path)

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
    def single_metric_link_colors(self):
        return self.config['single_metric_link_colors']

    @property
    def multi_metric_link_colors(self):
        return self.config['multi_metric_link_colors']

    @property
    def multi_metric_node_colors(self):
        return self.config['multi_metric_node_colors']

    def _parse_config(self, config_path):
        config_path = Path(config_path)
        extra_config = json.load(
            config_path.open(),
        ) if config_path.exists() else {}

        for key, value in extra_config.items():
            if extra_config.get(key, None):
                self.default_colors[key] = value
        return self.default_colors

    def vizualize(self, dfs, metrics, title, open_=True):
        metrics = metrics or self.default_metrics.keys()
        assert all(
            metric in self.supported_metrics.keys() for metric in
            metrics
        ), f'The only supported metrics are {self.supported_metrics}'
        flow_df = self._prepare_dfs_for_sankey(dfs, metrics)

        data_trace = dict(
            type='sankey',
            orientation='h',
            valueformat=',',
            valuesuffix=flow_df['variable'].map(self.supported_metrics),
            node=dict(
                pad=200,
                label=flow_df['label'],
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
            title=title,
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
                auto_open=open_,
            )
        else:
            iplot(
                dict(data=[data_trace], layout=layout),
                validate=False,
                filename=f"{title}-{','.join(metrics)}.html",
            )

    def _prepare_dfs_for_sankey(self, flow_dfs, metrics):
        flow_df = flow_dfs if isinstance(
            flow_dfs, pd.DataFrame) else pd.concat(flow_dfs)
        flow_df = flow_df.melt(id_vars=self.columns_pks, value_vars=metrics)
        return self._enrich_colors(flow_df, metrics)

    @abstractmethod
    def _enrich_colors(self, dfs, metrics):
        pass


if __name__ == '__main__':
    pass
