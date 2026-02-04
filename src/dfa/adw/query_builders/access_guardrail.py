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
from dfa.adw.tables.access_guardrail import (
    AccessGuardrailStateTable,
    AccessGuardrailTimeSeriesTable,
)


class AccessGuardrailStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = AccessGuardrailStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class AccessGuardrailStateCreateQueryBuilder(AccessGuardrailStateQueryBuilder):
    def executemany_sql_for_events(self):
        return AccessGuardrailStateUpdateQueryBuilder(self.events).executemany_sql_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class AccessGuardrailStateUpdateQueryBuilder(AccessGuardrailStateQueryBuilder):
    def executemany_sql_for_events(self):
        self.logger.info(
            "Using bulk insert / update operations for %d access guardrail events", len(self.events)
        )

        if len(self.events) == 0:
            self.logger.info("No events to process by access guardrail query builder")
            return

        insert_statement = InsertManyQueryBuilder().get_operation_sql(self, self.events, [])
        input_sizes = InsertManyQueryBuilder().get_input_sizes(
            AccessGuardrailStateTable().get_column_list_definition_for_table_ddl()
        )
        AdwConnection.get_cursor().setinputsizes(**input_sizes)
        AdwConnection.get_cursor().executemany(insert_statement, self.events, batcherrors=True)

        constraint_violating_rows = []
        for batch_error in AdwConnection.get_cursor().getbatcherrors():
            if batch_error.full_code == "ORA-00001":
                constraint_violating_rows.append(self.events[batch_error.offset])
            else:
                self.logger.info("access guardrail create failed - %s", batch_error.message)

        if len(constraint_violating_rows) > 0:
            self.logger.info(
                "%d access guardrail creates failed for unique constraint violation - \
                performing bulk access guardrail updates",
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
                self.logger.info("access guardrail update failed - %s", batch_error.message)

        AdwConnection.commit()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class AccessGuardrailStateDeleteQueryBuilder(AccessGuardrailStateQueryBuilder):
    def execute_sql_for_events(self):
        for event in self.events:
            delete_sql = DeleteQueryBuilder().get_operation_sql(
                self, event, ["id", "service_instance_id", "tenancy_id"]
            )
            AdwConnection.get_cursor().execute(delete_sql)
            self.logger.info("Row delete for access guardrail delete request")

        self.logger.info("Committing work for now")
        AdwConnection.commit()


class AccessGuardrailTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = AccessGuardrailTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class AccessGuardrailTimeSeriesCreateQueryBuilder(AccessGuardrailTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class AccessGuardrailTimeSeriesUpdateQueryBuilder(AccessGuardrailTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class AccessGuardrailTimeSeriesDeleteQueryBuilder(AccessGuardrailTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
