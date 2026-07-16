# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.query_builders.base_query_builder import BaseQueryBuilder
from dfa.adw.tables.policy import PolicyStateTable, PolicyTimeSeriesTable


class PolicyStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = PolicyStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class PolicyStateCreateQueryBuilder(PolicyStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.execute_delegated_query_builder(PolicyStateUpdateQueryBuilder(self.events))

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class PolicyStateUpdateQueryBuilder(PolicyStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.executemany_state_merge_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class PolicyStateDeleteQueryBuilder(PolicyStateQueryBuilder):
    def execute_sql_for_events(self):
        self.logger.info("Row delete for policy delete request")
        return self.executemany_delete_for_events(["id", "service_instance_id", "tenancy_id"])


class PolicyTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = PolicyTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class PolicyTimeSeriesCreateQueryBuilder(PolicyTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class PolicyTimeSeriesUpdateQueryBuilder(PolicyTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class PolicyTimeSeriesDeleteQueryBuilder(PolicyTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
