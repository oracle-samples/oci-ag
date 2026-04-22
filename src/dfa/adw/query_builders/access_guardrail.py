# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.connection import AdwConnection
from dfa.adw.query_builders.base_query_builder import (
    BaseQueryBuilder,
    DeleteQueryBuilder,
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
        return self.executemany_state_merge_for_events()

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
