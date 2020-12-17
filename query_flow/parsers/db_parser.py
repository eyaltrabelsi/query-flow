from abc import ABC, abstractmethod

import pandas as pd

from sqlalchemy import create_engine

__all__ = ["DBParser"]


class DBParser(ABC):
    label_replacement = {"UNION": " U ", "JOIN": " ⋈ ", "UNION ALL": " U "}

    def __init__(self, is_verbose=False):
        self.is_verbose = is_verbose
        self.max_id = 10000
        self.cardinality_df = pd.DataFrame({})

    @abstractmethod
    def parse(self, execution_plan):
        pass

    def get_next_id(self):
        self.max_id -= 1
        return self.max_id

    @classmethod
    def from_query(cls, query, con_str, logger=None):
        with create_engine(con_str).connect() as con:
            explain_analyze_query = f"{cls.explain_prefix} {query.replace('%', '%%')}"
            execution_plan = (
                con.execute(explain_analyze_query)
                   .fetchone()
                   .values()[0][0][cls.first_operator_indicator]
            )
            if logger:
                logger.info(execution_plan)
            return execution_plan

    def parse_node(self, execution_node, target_id):
        source_id = None

        node_type = execution_node[self.node_type_indicator]
        if self.is_verbose or node_type not in self.verbose_ops and self.strategy_dict.get(node_type):
            parsed_nodes, source_id = self.strategy_dict[node_type](target_id, execution_node)
            self.cardinality_df = self.cardinality_df.append(parsed_nodes, ignore_index=True)

        # Recursively parsing sub-expressions
        if self.next_operator_indicator in execution_node:
            target_id = source_id or target_id

            for next_execution_node in execution_node[self.next_operator_indicator]:
                self.parse_node(next_execution_node, target_id)


if __name__ == "__main__":
    pass
