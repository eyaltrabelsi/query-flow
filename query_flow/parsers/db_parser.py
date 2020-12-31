import hashlib
import json
from abc import ABC
from abc import abstractmethod
from functools import wraps

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

__all__ = ['DBParser']


class DBParser(ABC):
    label_replacement = {'UNION': ' U ', 'JOIN': ' ⋈ ', 'UNION ALL': ' U '}
    required_parsed_attr = frozenset(['label', 'label_metadata'])
    max_supported_nodes = 10000

    def __init__(self, is_verbose=False):
        assert set(self.strategy_dict.keys()).issubset(
            set(self.description_dict.keys()))

        self.is_verbose = is_verbose
        self._cleanup_state()

    @property
    @abstractmethod
    def verbose_ops(self):
        pass

    @property
    @abstractmethod
    def strategy_dict(self):
        pass

    @property
    @abstractmethod
    def description_dict(self):
        pass

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

    def from_query(self, query, con_str, logger=None):
        with create_engine(con_str).connect() as con:
            explain_analyze_query = f"{self.query_prefix} {query.replace('%', '%%')}"
            execution_plan = (
                con.execute(explain_analyze_query)
                    .fetchone()
                    .values()[0][0][self.first_operator_indicator]
            )
            if logger:
                logger.info(execution_plan)
            return execution_plan

    def parse(self, execution_plans):

        self._cleanup_state()
        for execution_plan in execution_plans:
            # self.max_id -= 1
            self.parse_node(execution_plan,
                            target_id=self.max_id,  # TODO I need next id
                            query_hash=DBParser._hash_execution_plan(execution_plan))

        flow_df = DBParser.align_source_target_ids(self.flow_df)
        flow_df = self.enrich_stats(flow_df)
        return flow_df

    def _cleanup_state(self):
        self.label_to_id_dict = {}
        self.flow_df = pd.DataFrame({})
        self.max_id = np.int64(self.max_supported_nodes)

    def parse_node(self, execution_node, target_id, query_hash):
        source_id = None

        node_type = execution_node[self.node_type_indicator]
        if self.is_verbose or node_type not in self.verbose_ops and self.strategy_dict.get(node_type):
            parsed_nodes, source_id = self.strategy_dict[node_type](
                target_id, execution_node,
            )
            parsed_nodes = [dict(parsed_node, **{'query_hash': query_hash})
                            for parsed_node in parsed_nodes]

            self.flow_df = self.flow_df.append(
                parsed_nodes, ignore_index=True,
            )

        # Recursively parsing sub-expressions
        if self.next_operator_indicator in execution_node:
            target_id = source_id or target_id

            for next_execution_node in execution_node[self.next_operator_indicator]:
                self.parse_node(next_execution_node, target_id, query_hash)

    def _get_next_id(self, execution_node, specific_attrs=None):

        def get_hash(execution_node, specific_attrs):
            representation = execution_node[self.node_type_indicator]
            if specific_attrs:
                representation += f"{specific_attrs['label']} {specific_attrs['label_metadata']}"
            return hashlib.sha224(representation.encode()).hexdigest()

        hash_node = get_hash(execution_node, specific_attrs)
        # '267dcad0989101d9936b5c294716fa333fd3930e9c70bab408f05cfc' 'Hash Join 10000 ' 'JOIN Hash Cond (\'Inner\', \'(titles.title_id = crew.title_id)\')'
        # 'fbf01c16aceb7295a5ca54d40feebdeb71caf3567b9e9f0f864fe97e' 'Seq Scan 9999 ' {'label': 'Titles*', 'label_metadata': "Filter condition: (titles.genres = 'Comedy'::text)", 'operation_type': 'Where'}
        # '4274d6445c6e7d69ce4525d62e3a0e7165aa483bec1815d8651da3e4' 'Seq Scan 9998 ' 'Titles '
        # '916bce4daca5aec3e439411ba7f77065fd3f63a40a7fcc6d2648d21f' 'Hash Join 9999 ' 'JOIN Hash Cond (\'Inner\', \'(crew.person_id = people.person_id)\')'
        # '2e8f8b0a672b60e9eb7994dd5655db3a28363dd23d198ceeda98d162' Crew
        # '03838707744e4f5057a5265c6e537dd1427f7713756698561a2d6d95' 'People* Filter condition: (people.name = ANY (\'{"Owen Wilson","Adam Sandler","Jason Segel"}\'::text[]))'
        # '7c9d2ece422f92d61e7691f51ee6ae37d7e0ef860149490fa76e37f4' people
        # 'b6e473bc11cda35c630a53aed584c8938933780adbab5f3f1c820273' 'Titles* Filter condition: (titles.genres = \'Comedy\'::text)'

        if hash_node not in self.label_to_id_dict:
            self.max_id -= 1
            self.label_to_id_dict[hash_node] = self.max_id

        return self.label_to_id_dict[hash_node]

    def _parse_default(self, target_id, execution_node, specific_attrs):
        assert all(attr in specific_attrs for attr in self.required_parsed_attr)

        source_id = self._get_next_id(
            execution_node, specific_attrs,
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
    def _hash_execution_plan(execution_plan):
        representation = json.dumps(execution_plan)
        return hashlib.sha224(representation.encode()).hexdigest()

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


if __name__ == '__main__':
    pass
