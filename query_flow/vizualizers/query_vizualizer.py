from functools import partial

from .dataframe_sankey_vizualizer import DataFrameSankeyVizualizer

__all__ = ["QueryVizualizer"]


class QueryVizualizer(DataFrameSankeyVizualizer):

    columns_pks = ['source', 'target', 'label', 'label_metadata', 'operation_type', 'redundent_operation']

    supported_metrics = {"actual_rows": " Rows",
                         "actual_startup_duration": " Seconds",
                         "actual_duration": " Seconds",
                         "actual_duration_pct": " Percent",
                         "estimated_cost": " Units",
                         "estimated_cost_pct": " Percent",
                         "plan_rows": "Rows"}

    default_metrics = {"actual_rows": " Rows",
                       "plan_rows": "Rows"}

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

    @staticmethod
    def color_edge(user_edge_colors_mapping, variable, value, redundent_operation):
        if redundent_operation:
            return user_edge_colors_mapping["redundant"]
        elif variable == 'actual_rows' and value == 0:
            return user_edge_colors_mapping["empty"]
        else:
            return user_edge_colors_mapping["default"]

    def enrich_colors(self, df, metrics):
        if len(metrics) > 1:
            # TODO generelize this
            df["color_link"] = df["variable"].astype("category").cat.codes.map(lambda code: ["gainsboro", "darkgray", "dimgray", "slategray", "darkslategray"][code])
            df["color_node"] = df["operation_type"].map(self.config["multi_metric_node_colors"])
        else:
            func = partial(QueryVizualizer.color_edge, self.config["single_metric_link_colors"])
            df["color_link"] = df.apply(lambda x: func(x['variable'], x['value'], x['redundent_operation']), axis=1)
            df["color_node"] = df["operation_type"].map(self.config["single_metric_link_colors"])
        return df


if __name__ == "__main__":
    pass
