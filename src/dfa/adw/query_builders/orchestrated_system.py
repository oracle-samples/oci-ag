# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.query_builders.base_query_builder import BaseQueryBuilder
from dfa.adw.tables.orchestrated_system import (
    OrchestratedSystemStateTable,
    OrchestratedSystemTimeSeriesTable,
)


class OrchestratedSystemStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = OrchestratedSystemStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class OrchestratedSystemStateCreateQueryBuilder(OrchestratedSystemStateQueryBuilder):
    def executemany_sql_for_events(self):
        return OrchestratedSystemStateUpdateQueryBuilder(self.events).executemany_sql_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class OrchestratedSystemStateUpdateQueryBuilder(OrchestratedSystemStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.executemany_state_merge_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class OrchestratedSystemStateDeleteQueryBuilder(OrchestratedSystemStateQueryBuilder):
    def execute_sql_for_events(self):
        self.logger.info("Bulk delete for orchestrated system delete request")
        return self.executemany_delete_for_events(["id", "service_instance_id", "tenancy_id"])


class OrchestratedSystemTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = OrchestratedSystemTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class OrchestratedSystemTimeSeriesCreateQueryBuilder(OrchestratedSystemTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class OrchestratedSystemTimeSeriesUpdateQueryBuilder(OrchestratedSystemTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class OrchestratedSystemTimeSeriesDeleteQueryBuilder(OrchestratedSystemTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
