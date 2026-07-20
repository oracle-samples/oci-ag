# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.query_builders.base_query_builder import BaseQueryBuilder
from dfa.adw.tables.approval_workflow import ApprovalWorkflowStateTable, ApprovalWorkflowTimeSeriesTable


class ApprovalWorkflowStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = ApprovalWorkflowStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class ApprovalWorkflowStateCreateQueryBuilder(ApprovalWorkflowStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.execute_delegated_query_builder(ApprovalWorkflowStateUpdateQueryBuilder(self.events))

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class ApprovalWorkflowStateUpdateQueryBuilder(ApprovalWorkflowStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.executemany_state_merge_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class ApprovalWorkflowStateDeleteQueryBuilder(ApprovalWorkflowStateQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_delete_for_events(["id", "service_instance_id", "tenancy_id"])


class ApprovalWorkflowTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = ApprovalWorkflowTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class ApprovalWorkflowTimeSeriesCreateQueryBuilder(ApprovalWorkflowTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class ApprovalWorkflowTimeSeriesUpdateQueryBuilder(ApprovalWorkflowTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class ApprovalWorkflowTimeSeriesDeleteQueryBuilder(ApprovalWorkflowTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
