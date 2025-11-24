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
        self.logger.info(
            "Using bulk insert / update operations for %d resource events", len(self.events)
        )

        if len(self.events) == 0:
            self.logger.info("No events to process by resource query builder")
            return

        insert_statement = InsertManyQueryBuilder().get_operation_sql(self, self.events, [])
        input_sizes = InsertManyQueryBuilder().get_input_sizes(self.events)
        AdwConnection.get_cursor().setinputsizes(**input_sizes)
        AdwConnection.get_cursor().executemany(insert_statement, self.events, batcherrors=True)

        constraint_violating_rows = []
        for batch_error in AdwConnection.get_cursor().getbatcherrors():
            if batch_error.full_code == "ORA-00001":
                constraint_violating_rows.append(self.events[batch_error.offset])
            else:
                self.logger.info("identity create failed - %s", batch_error.message)

        if len(constraint_violating_rows) > 0:
            self.logger.info(
                "%d resource creates failed for unique constraint violation - \
                performing bulk resource updates",
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
                self.logger.info("resource update failed - %s", batch_error.message)

        AdwConnection.commit()

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

        self.logger.info("Committing work for now")
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
