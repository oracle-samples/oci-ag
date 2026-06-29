# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.query_builders.base_query_builder import BaseQueryBuilder
from dfa.adw.tables.role import RoleStateTable, RoleTimeSeriesTable


class RoleStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = RoleStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class RoleStateCreateQueryBuilder(RoleStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.execute_delegated_query_builder(RoleStateUpdateQueryBuilder(self.events))

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class RoleStateUpdateQueryBuilder(RoleStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.executemany_state_merge_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class RoleStateDeleteQueryBuilder(RoleStateQueryBuilder):
    def execute_sql_for_events(self):
        self.logger.info("Row delete for role delete request")
        return self.executemany_delete_for_events(["id", "service_instance_id", "tenancy_id"])


class RoleTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = RoleTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class RoleTimeSeriesCreateQueryBuilder(RoleTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class RoleTimeSeriesUpdateQueryBuilder(RoleTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class RoleTimeSeriesDeleteQueryBuilder(RoleTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
