# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.query_builders.base_query_builder import BaseQueryBuilder
from dfa.adw.tables.access_bundle import AccessBundleStateTable, AccessBundleTimeSeriesTable


class AccessBundleStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = AccessBundleStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class AccessBundleStateCreateQueryBuilder(AccessBundleStateQueryBuilder):
    def executemany_sql_for_events(self):
        return AccessBundleStateUpdateQueryBuilder(self.events).executemany_sql_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class AccessBundleStateUpdateQueryBuilder(AccessBundleStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.executemany_state_merge_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class AccessBundleStateDeleteQueryBuilder(AccessBundleStateQueryBuilder):
    def execute_sql_for_events(self):
        self.logger.info("Bulk delete for access bundle delete request")
        return self.executemany_delete_for_events(["id", "service_instance_id", "tenancy_id"])


class AccessBundleTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = AccessBundleTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class AccessBundleTimeSeriesCreateQueryBuilder(AccessBundleTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class AccessBundleTimeSeriesUpdateQueryBuilder(AccessBundleTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class AccessBundleTimeSeriesDeleteQueryBuilder(AccessBundleTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
