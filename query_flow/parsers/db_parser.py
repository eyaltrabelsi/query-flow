import hashlib
import json
import typing
from abc import ABC, abstractmethod
from dataclasses import asdict, field, make_dataclass
from functools import wraps

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

__all__ = ['DBParser']


class DBParser(ABC):
    label_replacement = {'UNION': ' U ', 'JOIN': ' ⋈ ', 'UNION ALL': ' U '}
    required_parsed_attr = frozenset(['label', 'label_metadata'])
    max_supported_nodes = 10000

    def __init__(self, is_compact=False):
        assert set(self.strategy_dict.keys()).issubset(set(self.description_dict.keys()))
        self.is_compact = is_compact
        self.parsed_node_class = self._make_parsed_node()
        self._cleanup_state()

    @property
    @abstractmethod
    def strategy_dict(self):
        pass

    @property
    @abstractmethod
    def description_dict(self):
        pass

    @abstractmethod
    def execution_plan_extractor(self):
        pass

    @abstractmethod
    def normalize_metric(self):
        pass

    @property
    @abstractmethod
    def node_type_extractor(self):
        pass

    @property
    @abstractmethod
    def next_operator_indicator(self):
        pass

    @property
    @abstractmethod
    def filter_indicator(self):
        pass

    @property
    @abstractmethod
    def add_supported_metrics(self):
        pass

    @property
    @abstractmethod
    def supported_metrics(self):
        pass

    @abstractmethod
    def enrich_stats(self):
        pass

    def from_query(self, query, con_str):
        with create_engine(con_str).connect() as con:
            # SQLALCHEMY doesn't handle % as a regular SQL client so one need to add additional %
            explain_analyze_query = f"{self.query_prefix} {query.replace('%', '%%')}"

            # Grab the execution plan string in case its returned as a single row
            execution_plan = con.execute(explain_analyze_query).fetchone().values()[0][0]

            return self.execution_plan_extractor(execution_plan)

    def parse(self, execution_plans):

        self._cleanup_state()
        for execution_plan in execution_plans:

            # todo refactor
            if 'fragments' in execution_plan:
                for fragment in reversed(execution_plan['fragments']):
                    self.parse_node(
                        ({'fragment_id': fragment['id'], **fragment['logicalPlan']['1'][0]}),
                        target_id=np.nan,
                        query_hash=DBParser._hash_execution_plan(execution_plan),
                    )
            else:
                self.parse_node(
                    execution_plan, target_id=np.nan, query_hash=DBParser._hash_execution_plan(execution_plan)
                )

        flow_df = DBParser.align_source_target_ids(self.flow_df)
        flow_df = self.enrich_stats(flow_df)
        return flow_df

    def _cleanup_state(self):
        self.label_to_id_dict = {}
        self.flow_df = pd.DataFrame({})
        self.max_id = np.int64(self.max_supported_nodes)

    def parse_node(self, execution_node, target_id, query_hash):
        # Parsing current-expression
        node_type = self.node_type_extractor(execution_node)
        parsed_nodes, source_id = self.strategy_dict.get(node_type, self.parse_base)(target_id, execution_node)
        parsed_nodes = [dict(asdict(parsed_node), **{'query_hash': query_hash}) for parsed_node in parsed_nodes]
        # Currently we use state to save already parsed records, a cleaner version will is
        # provide calculated result as parameter and add return to the  function
        self.flow_df = self.flow_df.append(parsed_nodes, ignore_index=True)

        # Recursively parsing sub-expressions
        if self.next_operator_indicator in execution_node:
            target_id = source_id or target_id

            for next_execution_node in execution_node[self.next_operator_indicator]:
                self.parse_node(next_execution_node, target_id, query_hash)

    def _get_hash(self, execution_node, specific_attrs):
        representation = self.node_type_extractor(execution_node)
        if specific_attrs:
            representation += f"{specific_attrs['label']} {specific_attrs['label_metadata']}"
        return hashlib.sha224(representation.encode()).hexdigest()

    def _get_next_id(self, hash_node):
        if hash_node not in self.label_to_id_dict or not self.is_compact:
            self.max_id -= 1
            self.label_to_id_dict[hash_node] = self.max_id

        return self.label_to_id_dict[hash_node]

    def _make_parsed_node(self):
        supported_metrics_fields = [
            (self.normalize_metric(metric), typing.Any, field(default=np.nan, repr=False))
            for metric in self.supported_metrics
        ]
        return make_dataclass(
            'ParsedNode',
            [
                ('source', np.int64),
                ('target', np.int64),
                ('operation_type', str),
                ('label', str),
                ('label_metadata', str),  # TODO think if needed in here
                ('node_hash', str, field(repr=False)),
                ('fragment_id', str, field(default='', repr=False)),
                *supported_metrics_fields,
            ],
        )

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
                target_id,
                execution_node,
                specific_attrs,
            )
            return [self.parsed_node_class(**{**defaults_attrs, **specific_attrs})], source_id

        return parse

    @staticmethod
    def parse_filterable_node_decor(func):
        @wraps(func)
        def parse(self, target_id, execution_node):
            filter_func, clause_func = func(self)

            if self.filter_indicator(execution_node):
                filter_attrs = filter_func(execution_node)
                defaults_attrs, source_id = self._parse_default(
                    target_id,
                    execution_node,
                    filter_attrs,
                )
                filter_node = self.parsed_node_class(
                    **{
                        **defaults_attrs,
                        **filter_attrs,
                    }
                )
                target_id = source_id

            clause_attrs = clause_func(execution_node)
            defaults_attrs, source_id = self._parse_default(
                target_id,
                execution_node,
                clause_attrs,
            )

            non_filter_node = self.parsed_node_class(
                **{
                    **defaults_attrs,
                    **clause_attrs,
                }
            )
            parsed_node = [filter_node, non_filter_node] if self.filter_indicator(execution_node) else [non_filter_node]

            return parsed_node, source_id

        return parse

    def _parse_default(self, target_id, execution_node, specific_attrs):
        assert all(attr in specific_attrs for attr in self.required_parsed_attr)

        current_hash = self._get_hash(execution_node, specific_attrs)
        source_id = self._get_next_id(current_hash)

        parsed_node = {
            'source': source_id,
            'target': target_id,
            'operation_type': self.node_type_extractor(execution_node),
            'node_hash': current_hash,
        }

        if 'fragment_id' in execution_node:
            self.last_fragment_id = execution_node.get('fragment_id')
            parsed_node['fragment_id'] = self.last_fragment_id

        parsed_node = self.add_supported_metrics(parsed_node, execution_node)
        return parsed_node, source_id

    @staticmethod
    def align_source_target_ids(flow_df):
        ids = set(flow_df['source'].dropna()).union(set(flow_df['target'].dropna()))

        # In case of fragments we want to be able to connects fragments to one another
        for index, row in flow_df[flow_df['operation_type'] == 'RemoteSource'].iterrows():  # TODO
            fragment_id_to_look_for = row['label'][-2]
            current_source_id = row['source']
            flow_df.loc[
                (flow_df['fragment_id'] == fragment_id_to_look_for) & (flow_df['target'].isna()), 'target'
            ] = current_source_id

        # Give last operators the biggest id so no reuse of the same label later
        max_ = max(ids) + 1
        for index, _ in flow_df[flow_df['target'].isna()].iterrows():
            flow_df.loc[index, 'target'] = max_
            max_ += 1

        # Normalize ids to start with zero
        min_ = min(ids)
        flow_df = (
            flow_df.assign(
                target=lambda x: x.target.map(lambda y: y - min_).astype(int),
                source=lambda x: x.source.map(lambda y: y - min_).astype(int),
            )
            .sort_values(by='source')
            .reset_index(drop=True)
        )

        return flow_df


if __name__ == '__main__':
    pass
