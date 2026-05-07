# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.query_builders.base_query_builder import BaseQueryBuilder
from dfa.adw.tables.cloud_policy import CloudPolicyStateTable, CloudPolicyTimeSeriesTable


class CloudPolicyStateQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = CloudPolicyStateTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class CloudPolicyStateCreateQueryBuilder(CloudPolicyStateQueryBuilder):
    def executemany_sql_for_events(self):
        return CloudPolicyStateUpdateQueryBuilder(self.events).executemany_sql_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class CloudPolicyStateUpdateQueryBuilder(CloudPolicyStateQueryBuilder):
    def executemany_sql_for_events(self):
        return self.executemany_state_merge_for_events()

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class CloudPolicyStateDeleteQueryBuilder(CloudPolicyStateQueryBuilder):
    def execute_sql_for_events(self):
        self.logger.info("Bulk delete for tgt access pol stmt delete request")
        return self.executemany_delete_for_events(["policy_statement_id", "service_instance_id", "tenancy_id"])


class CloudPolicyTimeSeriesQueryBuilder(Table, ABC, BaseQueryBuilder):
    table_manager = CloudPolicyTimeSeriesTable()

    def __init__(self, events: list):
        super().__init__(self.table_manager.get_table_name().upper())
        self.events = events

    @abstractmethod
    def execute_sql_for_events(self):
        pass


class CloudPolicyTimeSeriesCreateQueryBuilder(CloudPolicyTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class CloudPolicyTimeSeriesUpdateQueryBuilder(CloudPolicyTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class CloudPolicyTimeSeriesDeleteQueryBuilder(CloudPolicyTimeSeriesQueryBuilder):
    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()
