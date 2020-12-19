
try:
    from .dataframe_sankey_vizualizer import DataFrameSankeyVizualizer
except ImportError:
    # Support running doctests not as a module
    from dataframe_sankey_vizualizer import DataFrameSankeyVizualizer

__all__ = ["QueryVizualizer"]


class QueryVizualizer(DataFrameSankeyVizualizer):
    columns_pks = frozenset(['source', 'target', 'label', 'label_metadata', 'operation_type', 'redundent_operation'])

    supported_metrics = {"actual_rows": " Rows",
                         "actual_startup_duration": " Seconds",
                         "actual_duration": " Seconds",
                         "actual_duration_pct": " Percent",
                         "estimated_cost": " Units",
                         "estimated_cost_pct": " Percent",
                         "plan_rows": "Rows"}

    default_metrics = {"actual_rows": " Rows", "plan_rows": "Rows"}
    default_colors = {
        "single_metric_node_colors": {"Aggregate": "purple",
                                      "Having": "mediumpurple",
                                      "Hashaggregate": "purple",
                                      "Join": "mediumseagreen",
                                      "Hash Join": "mediumseagreen",
                                      "Append": "olivedrab",
                                      "Scan": "blue",
                                      "Seq Scan": "blue",
                                      "Limit": "khaki",
                                      "Nested Loop": "mediumseagreen",
                                      "Where": "deepskyblue"},
        "single_metric_link_colors": {"default": "silver", "empty": "red", "redundant": "coral"},
        "multi_metric_node_colors": {},
        "multi_metric_link_colors": {"default": "silver"}

    }

    def __init__(self, parser, config_path="example_config"):
        super().__init__(config_path)
        self.parser = parser

    def get_cardinality_df(self, query, con_str, should_log=False):
        execution_plan = self.parser.from_query(query, con_str, should_log)
        return self.parser.parse(execution_plan)

    def _enrich_colors(self, df, metrics):
        if len(metrics) > 1:
            # TODO generelize this
            df["color_link"] = df["variable"].astype("category").cat.codes.map(lambda code: ["gainsboro", "darkgray", "dimgray", "slategray", "darkslategray"][code])
            df["color_node"] = df["operation_type"].map(self.multi_metric_node_colors)
        else:
            df["color_link"] = df.apply(lambda x: QueryVizualizer._color_edge(x['variable'], x['value'], x['redundent_operation']), axis=1)\
                                 .map(self.single_metric_link_colors)
            df["color_node"] = df["operation_type"].map(self.single_metric_link_colors)
        return df

    @staticmethod
    def _color_edge(metric, value, redundent_operation):
        """
        >>> QueryVizualizer._color_edge("actual_rows", 0, True)
        'redundant'

        >>> QueryVizualizer._color_edge("actual_rows", 0, False)
        'empty'

        >>> QueryVizualizer._color_edge("actual_rows", 2, False)
        'default'

        >>> QueryVizualizer._color_edge("cost", 0, False)
        'default'
        """

        if redundent_operation:
            return "redundant"
        elif metric == 'actual_rows' and value == 0:
            return "empty"
        else:
            return "default"


if __name__ == "__main__":
    import doctest
    doctest.testmod()

