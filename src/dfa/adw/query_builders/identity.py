# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.query_builders.base_query_builder import BaseQueryBuilder
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
        return self.execute_delegated_query_builder(IdentityStateUpdateQueryBuilder(self.events))

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class IdentityStateUpdateQueryBuilder(IdentityStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.executemany_state_merge_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class IdentityStateDeleteQueryBuilder(IdentityStateQueryBuilder):
    def execute_sql_for_events(self):
        global_identity_deletes = []
        target_identity_deletes = []

        for event in self.events:
            # if delete target identity, id (global id) must be empty
            if ((event.get("id") is None) or (event.get("id") == "")) and (event.get("ti_id") is not None):
                target_identity_deletes.append(event)
            elif (event.get("id") is not None) and (event.get("id") != ""):
                global_identity_deletes.append(event)

        if target_identity_deletes:
            self.logger.info("Bulk delete for target identity delete request")
            self.executemany_delete_for_events(
                ["ti_id", "service_instance_id", "tenancy_id"],
                events=target_identity_deletes,
            )

        if global_identity_deletes:
            self.logger.info("Bulk delete for identity delete request")
            self.executemany_delete_for_events(
                ["id", "service_instance_id", "tenancy_id"],
                events=global_identity_deletes,
            )
