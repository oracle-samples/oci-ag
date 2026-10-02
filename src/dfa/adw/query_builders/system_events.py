# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from pypika import Table

from dfa.adw.query_builders.base_query_builder import BaseQueryBuilder
from dfa.adw.tables.system_events import SystemEventsTimeSeriesTable


class SystemEventsTimeSeriesCreateQueryBuilder(Table, BaseQueryBuilder):
    table_manager = SystemEventsTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name())
        self.events = events

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
