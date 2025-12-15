# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.connection import AdwConnection
from dfa.adw.query_builders.base_query_builder import (
    BaseQueryBuilder,
    DeleteQueryBuilder,
    InsertManyQueryBuilder,
    UpdateManyQueryBuilder,
)
from dfa.adw.tables.permission_assignment import (
    PermissionAssignmentStateTable,
    PermissionAssignmentTimeSeriesTable,
)


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
            PermissionAssignmentStateDeleteQueryBuilder(
                permission_assignment_removes
            ).execute_sql_for_events()
        else:
            self.logger.info(
                "No permission assignment removes found... moving onto permission assignment adds"
            )

        ## Bulk insert and updates
        self.logger.info(
            "Using bulk insert / update operations for %d permission assignment events",
            len(permission_assignment_adds),
        )

        if len(permission_assignment_adds) == 0:
            self.logger.info("No events to process by permission assignment query builder")
            return

        insert_statement = InsertManyQueryBuilder().get_operation_sql(
            self, permission_assignment_adds, []
        )
        input_sizes = InsertManyQueryBuilder().get_input_sizes(permission_assignment_adds)
        AdwConnection.get_cursor().setinputsizes(**input_sizes)
        AdwConnection.get_cursor().executemany(
            insert_statement, permission_assignment_adds, batcherrors=True
        )

        constraint_violating_rows = []
        for batch_error in AdwConnection.get_cursor().getbatcherrors():
            if batch_error.full_code == "ORA-00001":
                constraint_violating_rows.append(permission_assignment_adds[batch_error.offset])
            else:
                self.logger.info("permission assignment create failed - %s", batch_error.message)

        if len(constraint_violating_rows) > 0:
            self.logger.info(
                "%d permission assignment creates failed for unique constraint violation - \
                performing bulk permission assignment updates",
                len(constraint_violating_rows),
            )
            update_sql = UpdateManyQueryBuilder().get_operation_sql(
                self,
                constraint_violating_rows,
                [],
                self.table_manager.get_unique_contraint_definition_details()["columns"],
            )

            AdwConnection.get_cursor().executemany(
                update_sql, constraint_violating_rows, batcherrors=True
            )

            for batch_error in AdwConnection.get_cursor().getbatcherrors():
                self.logger.info("permission assignment update failed - %s", batch_error.message)

        AdwConnection.commit()

    def execute_sql_for_events(self):
        self.executemany_sql_for_events()


class PermissionAssignmentStateDeleteQueryBuilder(PermissionAssignmentStateQueryBuilder):
    def execute_sql_for_events(self):
        for event in self.events:
            if event['permission_id'] != "":
                delete_sql = DeleteQueryBuilder().get_operation_sql(
                    self,
                    event,
                    [
                        "target_identity_id",
                        "permission_id",
                        "service_instance_id",
                        "tenancy_id",
                    ],
                )
            else:
                delete_sql = DeleteQueryBuilder().get_operation_sql(
                self,
                event,
                [
                    "target_identity_id",
                    "service_instance_id",
                    "tenancy_id",
                ],
                )
            AdwConnection.get_cursor().execute(delete_sql)
            self.logger.info("Row delete for permission assignment delete request")

        self.logger.info("Committing work for now")
        AdwConnection.commit()


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
