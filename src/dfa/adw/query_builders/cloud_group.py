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
from dfa.adw.tables.cloud_group import CloudGroupStateTable, CloudGroupTimeSeriesTable


class CloudGroupStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = CloudGroupStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class CloudGroupStateCreateQueryBuilder(CloudGroupStateQueryBuilder):
    def executemany_sql_for_events(self):
        return CloudGroupStateUpdateQueryBuilder(self.events).executemany_sql_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class CloudGroupStateUpdateQueryBuilder(CloudGroupStateQueryBuilder):
    def executemany_sql_for_events(self):
        group_membership_adds = []
        group_membership_removes = []

        for group_event in self.events:
            if group_event["identity_operation_type"] == "remove":
                group_membership_removes.append(group_event)
            else:
                group_membership_adds.append(group_event)

        if len(group_membership_removes) > 0:
            CloudGroupStateDeleteQueryBuilder(group_membership_removes).execute_sql_for_events()
        else:
            self.logger.info(
                "No group membership removes found... moving onto group membership adds"
            )

        ## Bulk insert and updates
        self.logger.info(
            "Using bulk insert / update operations for %d cloud group membership events",
            len(group_membership_adds),
        )

        if len(group_membership_adds) == 0:
            self.logger.info("No events to process by group membership query builder")
            return

        insert_statement = InsertManyQueryBuilder().get_operation_sql(
            self, group_membership_adds, []
        )
        input_sizes = InsertManyQueryBuilder().get_input_sizes(
            CloudGroupStateTable().get_column_list_definition_for_table_ddl()
        )
        AdwConnection.get_cursor().setinputsizes(**input_sizes)
        AdwConnection.get_cursor().executemany(
            insert_statement, group_membership_adds, batcherrors=True
        )

        constraint_violating_rows = []
        for batch_error in AdwConnection.get_cursor().getbatcherrors():
            if batch_error.full_code == "ORA-00001":
                constraint_violating_rows.append(group_membership_adds[batch_error.offset])
            else:
                self.logger.info("group membership create failed - %s", batch_error.message)

        if len(constraint_violating_rows) > 0:
            self.logger.info(
                "%d group membership creates failed for unique constraint violation - \
                performing bulk group membership updates",
                len(constraint_violating_rows),
            )
            update_sql = UpdateManyQueryBuilder().get_operation_sql(
                self,
                constraint_violating_rows,
                [],
                self.table_manager.get_unique_contraint_definition_details()["columns"],
            )

            AdwConnection.get_cursor().setinputsizes(**input_sizes)
            AdwConnection.get_cursor().executemany(
                update_sql, constraint_violating_rows, batcherrors=True
            )

            for batch_error in AdwConnection.get_cursor().getbatcherrors():
                self.logger.info("identity update failed - %s", batch_error.message)

        AdwConnection.commit()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class CloudGroupStateDeleteQueryBuilder(CloudGroupStateQueryBuilder):
    def execute_sql_for_events(self):
        for event in self.events:
            if event["identity_global_id"] != "":
                delete_sql = DeleteQueryBuilder().get_operation_sql(
                    self, event, ["id", "identity_global_id", "service_instance_id", "tenancy_id"]
                )
            else:
                delete_sql = DeleteQueryBuilder().get_operation_sql(
                    self, event, ["id", "service_instance_id", "tenancy_id"]
                )
            AdwConnection.get_cursor().execute(delete_sql)

        self.logger.info("Committing work for now")
        AdwConnection.commit()


class CloudGroupTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = CloudGroupTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class CloudGroupTimeSeriesCreateQueryBuilder(CloudGroupTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class CloudGroupTimeSeriesUpdateQueryBuilder(CloudGroupTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class CloudGroupTimeSeriesDeleteQueryBuilder(CloudGroupTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
