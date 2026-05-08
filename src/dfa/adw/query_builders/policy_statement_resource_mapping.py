# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.query_builders.base_query_builder import BaseQueryBuilder
from dfa.adw.tables.policy_statement_resource_mapping import (
    PolicyStatementResourceMappingStateTable,
    PolicyStatementResourceMappingTimeSeriesTable,
)


class PolicyStatementResourceMappingStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = PolicyStatementResourceMappingStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class PolicyStatementResourceMappingStateCreateQueryBuilder(PolicyStatementResourceMappingStateQueryBuilder):
    def executemany_sql_for_events(self):
        return PolicyStatementResourceMappingStateUpdateQueryBuilder(self.events).executemany_sql_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class PolicyStatementResourceMappingStateUpdateQueryBuilder(PolicyStatementResourceMappingStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.executemany_state_merge_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class PolicyStatementResourceMappingStateDeleteQueryBuilder(PolicyStatementResourceMappingStateQueryBuilder):
    def execute_sql_for_events(self):
        self.logger.info("Bulk delete for policy statement resource mapping delete request")
        return self.executemany_delete_for_events(
            ["policy_statement_id", "resource_id", "service_instance_id", "tenancy_id"]
        )


class PolicyStatementResourceMappingTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = PolicyStatementResourceMappingTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class PolicyStatementResourceMappingTimeSeriesCreateQueryBuilder(PolicyStatementResourceMappingTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class PolicyStatementResourceMappingTimeSeriesUpdateQueryBuilder(PolicyStatementResourceMappingTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class PolicyStatementResourceMappingTimeSeriesDeleteQueryBuilder(PolicyStatementResourceMappingTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
