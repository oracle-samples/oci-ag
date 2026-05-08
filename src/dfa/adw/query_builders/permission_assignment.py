# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.query_builders.base_query_builder import BaseQueryBuilder
from dfa.adw.tables.permission_assignment import PermissionAssignmentStateTable, PermissionAssignmentTimeSeriesTable


class PermissionAssignmentStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = PermissionAssignmentStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class PermissionAssignmentStateCreateQueryBuilder(PermissionAssignmentStateQueryBuilder):
    def executemany_sql_for_events(self):
        return PermissionAssignmentStateUpdateQueryBuilder(self.events).executemany_sql_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class PermissionAssignmentStateUpdateQueryBuilder(PermissionAssignmentStateQueryBuilder):
    def executemany_sql_for_events(self):
        permission_assignment_adds = []
        permission_assignment_removes = []

        for pa_event in self.events:
            if pa_event["identity_operation_type"] == "remove":
                permission_assignment_removes.append(pa_event)
            else:
                permission_assignment_adds.append(pa_event)

        if len(permission_assignment_removes) > 0:
            PermissionAssignmentStateDeleteQueryBuilder(permission_assignment_removes).execute_sql_for_events()
        else:
            self.logger.info("No permission assignment removes found... moving onto permission assignment adds")

        if len(permission_assignment_adds) == 0:
            self.logger.info("No events to process by permission assignment query builder")
            return

        self.executemany_state_merge_for_events(permission_assignment_adds)

    def execute_sql_for_events(self):
        self.executemany_sql_for_events()


class PermissionAssignmentStateDeleteQueryBuilder(PermissionAssignmentStateQueryBuilder):
    def execute_sql_for_events(self):
        self.logger.info("Row delete for permission assignment delete request")
        events_with_permission = []
        events_without_permission = []

        for event in self.events:
            if event["permission_id"] != "":
                events_with_permission.append(event)
            else:
                events_without_permission.append(event)

        if events_with_permission:
            self.executemany_delete_for_events(
                [
                    "target_identity_id",
                    "permission_id",
                    "service_instance_id",
                    "tenancy_id",
                ],
                events=events_with_permission,
            )
        if events_without_permission:
            self.executemany_delete_for_events(
                [
                    "target_identity_id",
                    "service_instance_id",
                    "tenancy_id",
                ],
                events=events_without_permission,
            )


class PermissionAssignmentTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = PermissionAssignmentTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class PermissionAssignmentTimeSeriesCreateQueryBuilder(PermissionAssignmentTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class PermissionAssignmentTimeSeriesUpdateQueryBuilder(PermissionAssignmentTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class PermissionAssignmentTimeSeriesDeleteQueryBuilder(PermissionAssignmentTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
