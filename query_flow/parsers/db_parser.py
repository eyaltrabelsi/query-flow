import hashlib
from abc import ABC
from abc import abstractmethod
from functools import wraps

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

__all__ = ['DBParser']


def listify(val):
    if type(val) in [str, int, float, dict]:
        return [val]
    return val


class DBParser(ABC):
    label_replacement = {'UNION': ' U ', 'JOIN': ' ⋈ ', 'UNION ALL': ' U '}
    required_parsed_attr = frozenset(['label', 'label_metadata'])
    max_supported_nodes = 10000

    # TODO is_verbose all_operations, group operations
    def __init__(self, is_verbose=False):
        self.is_verbose = is_verbose
        self._cleanup_state()

    @property
    @abstractmethod
    def node_type_indicator(self):
        pass

    @property
    @abstractmethod
    def next_operator_indicator(self):
        pass

    @property
    @abstractmethod
    def first_operator_indicator(self):
        pass

    @property
    @abstractmethod
    def filter_indicator(self):
        pass

    @property
    @abstractmethod
    def supported_metrics(self):
        pass

    @abstractmethod
    def enrich_stats(self):
        pass

    def parse(self, execution_plans):
        self._cleanup_state()
        for execution_plan in listify(execution_plans):
            self.parse_node(execution_plan, target_id=self.max_id)

        cardinality_df = DBParser.align_source_target_ids(self.cardinality_df)
        cardinality_df = self.enrich_stats(cardinality_df)
        return cardinality_df

    def _cleanup_state(self):
        self.label_to_id_dict = {}
        self.cardinality_df = pd.DataFrame({})
        self.max_id = np.int64(self.max_supported_nodes)

    def parse_node(self, execution_node, target_id):
        source_id = None

        node_type = execution_node[self.node_type_indicator]
        if self.is_verbose or node_type not in self.verbose_ops and self.strategy_dict.get(node_type):
            parsed_nodes, source_id = self.strategy_dict[node_type](
                target_id, execution_node,
            )
            self.cardinality_df = self.cardinality_df.append(
                parsed_nodes, ignore_index=True,
            )

        # Recursively parsing sub-expressions
        if self.next_operator_indicator in execution_node:
            target_id = source_id or target_id

            for next_execution_node in execution_node[self.next_operator_indicator]:
                self.parse_node(next_execution_node, target_id)

    def _get_next_id(self, execution_node, target_id, specific_attrs):

        def get_hash(execution_node, target_id, specific_attrs):
            representation = f"{execution_node[self.node_type_indicator]} {target_id} {specific_attrs['label']} {specific_attrs['label_metadata']}"
            return hashlib.sha224(representation.encode()).hexdigest()

        # TODO compact
        hash_node = get_hash(execution_node, target_id, specific_attrs)

        if hash_node not in self.label_to_id_dict:
            self.max_id -= 1
            self.label_to_id_dict[hash_node] = self.max_id

        return self.label_to_id_dict[hash_node]

    def _parse_default(self, target_id, execution_node, specific_attrs):
        assert all(attr in specific_attrs for attr in self.required_parsed_attr)

        source_id = self._get_next_id(
            execution_node, target_id, specific_attrs,
        )
        parsed_node = {
            'source': source_id, 'target': target_id,
            'operation_type': execution_node[self.node_type_indicator],
        }

        for metric in self.supported_metrics:
            if metric in execution_node:
                normalized_key = metric.replace(' ', '_').lower()
                parsed_node[normalized_key] = execution_node[metric]

        return parsed_node, source_id

    @staticmethod
    def parse_default_decor(func):
        @wraps(func)
        def parse(self, target_id, execution_node):
            specific_attrs = func(self, execution_node)
            defaults_attrs, source_id = self._parse_default(
                target_id, execution_node, specific_attrs,
            )
            return [{**defaults_attrs, **specific_attrs}], source_id

        return parse

    @staticmethod
    def parse_filterable_node_decor(func):
        @wraps(func)
        def parse(self, target_id, execution_node):
            filter_func, clause_func = func(self)

            if self.filter_indicator in execution_node:
                filter_attrs = filter_func(execution_node)
                defaults_attrs, source_id = self._parse_default(
                    target_id, execution_node, filter_attrs,
                )
                filter_node = {
                    **defaults_attrs,
                    **filter_attrs,
                }
                target_id = source_id

            clause_attrs = clause_func(execution_node)
            defaults_attrs, source_id = self._parse_default(
                target_id, execution_node, clause_attrs,
            )

            non_filter_node = {
                **defaults_attrs,
                **clause_attrs,
            }
            parsed_node = [filter_node, non_filter_node] if self.filter_indicator in execution_node else [
                non_filter_node,
            ]

            return parsed_node, source_id

        return parse

    @staticmethod
    def align_source_target_ids(flow_df):
        min_ = min(set(flow_df['source']).union(set(flow_df['target'])))
        flow_df = flow_df.assign(
            target=lambda x: x.target.map(lambda y: y - min_),
            source=lambda x: x.source.map(lambda y: y - min_),
        ) \
            .sort_values(by='source') \
            .reset_index(drop=True)
        return flow_df

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


if __name__ == '__main__':
    pass
