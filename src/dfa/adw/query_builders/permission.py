# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.query_builders.base_query_builder import BaseQueryBuilder
from dfa.adw.tables.permission import PermissionStateTable, PermissionTimeSeriesTable


class PermissionStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = PermissionStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class PermissionStateCreateQueryBuilder(PermissionStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.execute_delegated_query_builder(PermissionStateUpdateQueryBuilder(self.events))

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class PermissionStateUpdateQueryBuilder(PermissionStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.executemany_state_merge_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class PermissionStateDeleteQueryBuilder(PermissionStateQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_delete_for_events(["id", "service_instance_id", "tenancy_id"])


class PermissionTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = PermissionTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class PermissionTimeSeriesCreateQueryBuilder(PermissionTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class PermissionTimeSeriesUpdateQueryBuilder(PermissionTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class PermissionTimeSeriesDeleteQueryBuilder(PermissionTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
