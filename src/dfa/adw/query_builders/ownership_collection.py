# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.query_builders.base_query_builder import BaseQueryBuilder
from dfa.adw.tables.ownership_collection import (
    OwnershipCollectionStateTable,
    OwnershipCollectionTimeSeriesTable,
)


class OwnershipCollectionStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = OwnershipCollectionStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class OwnershipCollectionStateCreateQueryBuilder(OwnershipCollectionStateQueryBuilder):
    def executemany_sql_for_events(self):
        return OwnershipCollectionStateUpdateQueryBuilder(self.events).executemany_sql_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class OwnershipCollectionStateUpdateQueryBuilder(OwnershipCollectionStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.executemany_state_merge_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class OwnershipCollectionStateDeleteQueryBuilder(OwnershipCollectionStateQueryBuilder):
    def execute_sql_for_events(self):
        self.logger.info("Row delete for ownership collection delete request")
        return self.executemany_delete_for_events(["id", "service_instance_id", "tenancy_id"])


class OwnershipCollectionTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = OwnershipCollectionTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class OwnershipCollectionTimeSeriesCreateQueryBuilder(OwnershipCollectionTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class OwnershipCollectionTimeSeriesUpdateQueryBuilder(OwnershipCollectionTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class OwnershipCollectionTimeSeriesDeleteQueryBuilder(OwnershipCollectionTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
