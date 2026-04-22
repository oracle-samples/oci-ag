# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.connection import AdwConnection
from dfa.adw.query_builders.base_query_builder import (
    BaseQueryBuilder,
    DeleteQueryBuilder,
)
from dfa.adw.tables.resource import ResourceStateTable, ResourceTimeSeriesTable


class ResourceStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = ResourceStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class ResourceStateCreateQueryBuilder(ResourceStateQueryBuilder):
    def executemany_sql_for_events(self):
        return ResourceStateUpdateQueryBuilder(self.events).executemany_sql_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class ResourceStateUpdateQueryBuilder(ResourceStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.executemany_state_merge_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class ResourceStateDeleteQueryBuilder(ResourceStateQueryBuilder):
    def execute_sql_for_events(self):
        for event in self.events:
            delete_sql = DeleteQueryBuilder().get_operation_sql(
                self, event, ["id", "service_instance_id", "tenancy_id"]
            )
            AdwConnection.get_cursor().execute(delete_sql)
            self.logger.info("Row delete for group delete request")

        AdwConnection.commit()


class ResourceTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = ResourceTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class ResourceTimeSeriesCreateQueryBuilder(ResourceTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class ResourceTimeSeriesUpdateQueryBuilder(ResourceTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class ResourceTimeSeriesDeleteQueryBuilder(ResourceTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
