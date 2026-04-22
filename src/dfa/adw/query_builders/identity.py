# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.connection import AdwConnection
from dfa.adw.query_builders.base_query_builder import (
    BaseQueryBuilder,
    DeleteQueryBuilder,
)
from dfa.adw.tables.identity import IdentityStateTable, IdentityTimeSeriesTable


class IdentityTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = IdentityTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class IdentityTimeSeriesCreateQueryBuilder(IdentityTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class IdentityTimeSeriesUpdateQueryBuilder(IdentityTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class IdentityTimeSeriesDeleteQueryBuilder(IdentityTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class IdentityStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = IdentityStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class IdentityStateCreateQueryBuilder(IdentityStateQueryBuilder):
    def executemany_sql_for_events(self):
        return IdentityStateUpdateQueryBuilder(self.events).executemany_sql_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class IdentityStateUpdateQueryBuilder(IdentityStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.executemany_state_merge_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class IdentityStateDeleteQueryBuilder(IdentityStateQueryBuilder):
    def execute_sql_for_events(self):
        for event in self.events:
            # if delete target identity, id (global id) must be empty
            if ((event.get("id") is None) or (event.get("id") == "")) and (
                event.get("ti_id") is not None
            ):
                delete_sql = DeleteQueryBuilder().get_operation_sql(
                    self, event, ["ti_id", "service_instance_id", "tenancy_id"]
                )
            elif (event.get("id") is not None) and (event.get("id") != ""):
                delete_sql = DeleteQueryBuilder().get_operation_sql(
                    self, event, ["id", "service_instance_id", "tenancy_id"]
                )
            else:
                continue

            ## delete initial identity record if one exists
            AdwConnection.get_cursor().execute(delete_sql)
            self.logger.info("Row delete for identity delete request")

        AdwConnection.commit()
