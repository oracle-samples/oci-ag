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
from dfa.adw.tables.global_identity_collection import (
    GlobalIdentityCollectionStateTable,
    GlobalIdentityCollectionTimeSeriesTable,
)


class GlobalIdentityCollectionStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = GlobalIdentityCollectionStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class GlobalIdentityCollectionStateCreateQueryBuilder(GlobalIdentityCollectionStateQueryBuilder):
    def executemany_sql_for_events(self):
        return GlobalIdentityCollectionStateUpdateQueryBuilder(
            self.events
        ).executemany_sql_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class GlobalIdentityCollectionStateUpdateQueryBuilder(GlobalIdentityCollectionStateQueryBuilder):
    def executemany_sql_for_events(self):
        self.logger.info(
            "Using bulk insert / update operations for %d global identity collection events",
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

        insert_statement = InsertManyQueryBuilder().get_operation_sql(self, gic_adds, [])
        input_sizes = InsertManyQueryBuilder().get_input_sizes(gic_adds)
        AdwConnection.get_cursor().setinputsizes(**input_sizes)
        AdwConnection.get_cursor().executemany(insert_statement, gic_adds, batcherrors=True)

        constraint_violating_rows = []
        for batch_error in AdwConnection.get_cursor().getbatcherrors():
            if batch_error.full_code == "ORA-00001":
                constraint_violating_rows.append(gic_adds[batch_error.offset])
            else:
                self.logger.info(
                    "global identity collection create failed - %s", batch_error.message
                )

        if len(constraint_violating_rows) > 0:
            self.logger.info(
                "%d global identity collection creates failed for unique constraint violation - \
                performing bulk global identity collection updates",
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
                self.logger.info(
                    "global identity collection update failed - %s", batch_error.message
                )

        AdwConnection.commit()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class GlobalIdentityCollectionStateDeleteQueryBuilder(GlobalIdentityCollectionStateQueryBuilder):
    def execute_sql_for_events(self):
        for event in self.events:
            delete_sql = DeleteQueryBuilder().get_operation_sql(
                self, event, ["id", "service_instance_id", "tenancy_id"]
            )
            AdwConnection.get_cursor().execute(delete_sql)
            self.logger.info("Row delete for global identity collection delete request")

        self.logger.info("Committing work for now")
        AdwConnection.commit()


class GlobalIdentityCollectionTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = GlobalIdentityCollectionTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class GlobalIdentityCollectionTimeSeriesCreateQueryBuilder(
    GlobalIdentityCollectionTimeSeriesQueryBuilder
):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class GlobalIdentityCollectionTimeSeriesUpdateQueryBuilder(
    GlobalIdentityCollectionTimeSeriesQueryBuilder
):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class GlobalIdentityCollectionTimeSeriesDeleteQueryBuilder(
    GlobalIdentityCollectionTimeSeriesQueryBuilder
):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
