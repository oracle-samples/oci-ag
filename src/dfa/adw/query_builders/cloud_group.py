# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod

from pypika import Table

from dfa.adw.query_builders.base_query_builder import BaseQueryBuilder
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

        if len(group_membership_adds) == 0:
            self.logger.info("No events to process by group membership query builder")
            return

        self.executemany_state_merge_for_events(group_membership_adds)

    def execute_sql_for_events(self):
        return self.executemany_sql_for_events()


class CloudGroupStateDeleteQueryBuilder(CloudGroupStateQueryBuilder):
    def execute_sql_for_events(self):
        self.logger.info("Row delete for cloud group delete request")
        events_with_identity = []
        events_without_identity = []

        for event in self.events:
            if event["identity_global_id"] != "":
                events_with_identity.append(event)
            else:
                events_without_identity.append(event)

        if events_with_identity:
            self.executemany_delete_for_events(
                ["id", "identity_global_id", "service_instance_id", "tenancy_id"],
                events=events_with_identity,
            )
        if events_without_identity:
            self.executemany_delete_for_events(
                ["id", "service_instance_id", "tenancy_id"],
                events=events_without_identity,
            )


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
