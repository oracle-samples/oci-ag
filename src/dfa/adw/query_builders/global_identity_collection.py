# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.query_builders.base_query_builder import BaseQueryBuilder
from dfa.adw.tables.global_identity_collection import (
    GlobalIdentityCollectionStateTable,
    GlobalIdentityCollectionTimeSeriesTable,
)


class GlobalIdentityCollectionStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = GlobalIdentityCollectionStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class GlobalIdentityCollectionStateCreateQueryBuilder(GlobalIdentityCollectionStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.execute_delegated_query_builder(GlobalIdentityCollectionStateUpdateQueryBuilder(self.events))

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class GlobalIdentityCollectionStateUpdateQueryBuilder(GlobalIdentityCollectionStateQueryBuilder):
    def executemany_sql_for_events(self):
        self.logger.info(
            "Using merge operations for %d global identity collection events",
            len(self.events),
        )

        gic_adds = []
        gic_removes = []

        for gic_event in self.events:
            if gic_event["member_operation_type"] == "remove":
                gic_removes.append(gic_event)
            else:
                gic_adds.append(gic_event)

        if len(gic_removes) > 0:
            GlobalIdentityCollectionStateDeleteQueryBuilder(gic_removes).execute_sql_for_events()
        else:
            self.logger.info(
                "No global identity collection member removes found, moving onto global identity collection member adds"
            )

        if len(gic_adds) == 0:
            self.logger.info("No events to process by global identity collection query builder")
            return

        self.executemany_state_merge_for_events(gic_adds)

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class GlobalIdentityCollectionStateDeleteQueryBuilder(GlobalIdentityCollectionStateQueryBuilder):
    def execute_sql_for_events(self):
        self.logger.info("Row delete for global identity collection delete request")
        events_with_member = []
        events_without_member = []

        for event in self.events:
            if event["member_global_id"] != "":
                events_with_member.append(event)
            else:
                events_without_member.append(event)

        if events_with_member:
            self.executemany_delete_for_events(
                ["id", "member_global_id", "service_instance_id", "tenancy_id"],
                events=events_with_member,
            )
        if events_without_member:
            self.executemany_delete_for_events(
                ["id", "service_instance_id", "tenancy_id"],
                events=events_without_member,
            )


class GlobalIdentityCollectionTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = GlobalIdentityCollectionTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class GlobalIdentityCollectionTimeSeriesCreateQueryBuilder(GlobalIdentityCollectionTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class GlobalIdentityCollectionTimeSeriesUpdateQueryBuilder(GlobalIdentityCollectionTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class GlobalIdentityCollectionTimeSeriesDeleteQueryBuilder(GlobalIdentityCollectionTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
