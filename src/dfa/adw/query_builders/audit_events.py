# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.connection import AdwConnection
from dfa.adw.query_builders.base_query_builder import BaseQueryBuilder, InsertManyQueryBuilder
from dfa.adw.tables.audit_events import AuditEventsTable


class AuditEventsStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = AuditEventsTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class AuditEventsStateCreateQueryBuilder(AuditEventsStateQueryBuilder):
    def executemany_sql_for_events(self):
        self.logger.info("Using bulk insert operations for %d audit events", len(self.events))

        if len(self.events) == 0:
            self.logger.info("No events to process by audit query builder")
            return

        insert_statement = InsertManyQueryBuilder().get_operation_sql(self, self.events, [])
        input_sizes = InsertManyQueryBuilder().get_input_sizes(
            AuditEventsTable().get_column_list_definition_for_table_ddl()
        )
        AdwConnection.get_cursor().setinputsizes(**input_sizes)
        AdwConnection.get_cursor().executemany(insert_statement, self.events, batcherrors=True)

        if len(AdwConnection.get_cursor().getbatcherrors()) > 0:
            self.logger.info(
                "Audit insert failed %s times due to duplicate rows",
                len(AdwConnection.get_cursor().getbatcherrors()),
            )

        AdwConnection.commit()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
