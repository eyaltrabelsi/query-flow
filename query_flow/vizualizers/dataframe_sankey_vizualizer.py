from abc import ABC, abstractmethod

import numpy as np
import pandas as pd
from plotly.offline import plot, iplot

from .config_parser import ConfigParser


class DataFrameSankeyVizualizer(ABC):
    supported_metrics = None
    default_metrics = None
    link_defaults = None
    node_defaults = None
    columns_pks = None

    def __init__(self, config_path='example_config.json'):
        self.config = ConfigParser(self.default_colors, config_path)

    def prepare_dfs_for_sankey(self, df, metrics):
        df = df if isinstance(df, pd.DataFrame) else pd.concat(df)
        # TODO maybe move this
        df = df.melt(id_vars=self.columns_pks, value_vars=metrics)
        return self.enrich_colors(df, metrics)

    @abstractmethod
    def enrich_colors(self, dfs, metrics):
        pass

    def vizualize(self, dfs, metrics, title, open_=True):
        metrics = metrics or self.default_metrics.keys()
        assert all(metric in self.supported_metrics.keys() for metric in
                   metrics), f"The only supported metrics are {self.supported_metrics}"
        df = self.prepare_dfs_for_sankey(dfs, metrics)

        data_trace = dict(
            type="sankey",
            orientation="h",
            valueformat=",",
            valuesuffix=df["variable"].map(self.supported_metrics),
            node=dict(
                pad=200,
                label=df["label"],
                color=df["color_node"]
            ),
            link=dict(

                source=df["source"],
                target=df["target"],
                value=df["value"].map(np.int64).replace(0, 1),
                label=df["label_metadata"],
                color=df["color_link"]
            ),
        )
        layout = dict(
            title=title,
            font=dict(size=10),
            height=750,
            updatemenus=[
                dict(
                    y=0.6,
                    buttons=[dict(label="Horizontal", method="restyle", args=["orientation", "h"]),
                             dict(label="Vertical", method="restyle", args=["orientation", "v"]),
                             ],
                )
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
                filename=f"{title}-{','.join(metrics)}.html"
            )


if __name__ == "__main__":
    pass
