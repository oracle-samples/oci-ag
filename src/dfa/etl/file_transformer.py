# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json
import os

import pandas as pd

from common.ocihelpers.storage import BaseObjectStorage
from dfa.adw.connection import AdwConnection
from dfa.adw.query_builders.base_query_builder import get_query_builder
from dfa.etl.abstract_transformer import AbstractTransformer


class FileTransformer(AbstractTransformer):
    transformer_name = "dfa_file_transformer"
    is_timeseries = False
    query_builder = None

    _namespace = None
    _bucket_name = None
    _object_name = None
    _object_storage_client = None

    def __init__(self, namespace, bucket_name, object_name, is_timeseries=False):
        self.logger.info(
            "Initializing FileTransformer with namespace %s, bucket_name %s, object_name %s, is_timeseries %s",
            namespace,
            bucket_name,
            object_name,
            is_timeseries,
        )
        self.is_timeseries = is_timeseries
        self.transformer_name += "_timeseries" if is_timeseries else ""
        self._namespace = namespace
        self._bucket_name = bucket_name
        self._object_name = object_name
        self._object_storage_client = BaseObjectStorage()

    def _set_raw_event_data(self, event_data):
        content = event_data.data.content.decode("utf-8")

        if self._object_name.endswith(".jsonl"):
            for line in content.splitlines():
                raw_event = json.loads(line)
                if "headers" in raw_event:
                    if "messageType" in raw_event["headers"]:
                        self._event_object_type = raw_event["headers"]["messageType"]
                    if "operation" in raw_event["headers"]:
                        self._operation_type = raw_event["headers"]["operation"]
                    if "eventTime" in raw_event["headers"]:
                        self._event_timestamp = raw_event["headers"]["eventTime"]
                    if "tenancyId" in raw_event["headers"]:
                        self._tenancy_id = raw_event["headers"]["tenancyId"]
                    if "serviceInstanceId" in raw_event["headers"]:
                        self._service_instance_id = raw_event["headers"]["serviceInstanceId"]
                    continue
                self._raw_events.append(raw_event)
        else:
            outer = json.loads(content)
            try:
                raw_field = outer.get("data")
                raw_data = json.loads(raw_field) if isinstance(raw_field, str) else raw_field
            except Exception as e:
                self.logger.error("Failed to parse payload 'data' field: %s", e)
                raw_data = None
            if isinstance(raw_data, dict):
                self.logger.debug("we have a dict")
                self._raw_events = [raw_data]
            elif isinstance(raw_data, list):
                self._raw_events = raw_data
            else:
                self.logger.warning("Cannot process event content - unknown format was found.")
            headers = outer.get("headers", {})
            if headers:
                if "messageType" in headers:
                    self._event_object_type = headers["messageType"]
                if "operation" in headers:
                    self._operation_type = headers["operation"]
                if "eventTime" in headers:
                    self._event_timestamp = headers["eventTime"]
                if "tenancyId" in headers:
                    self._tenancy_id = headers["tenancyId"]
                if "serviceInstanceId" in headers:
                    self._service_instance_id = headers["serviceInstanceId"]

        if not self.is_valid_object_type(self.get_event_object_type()):
            self.logger.info(
                "Skipping processing for event of type %s", self.get_event_object_type()
            )

    def extract_data(self):
        self.logger.info("Extracting data from object storage.")
        event_data = self._object_storage_client.download(
            self._namespace, self._bucket_name, self._object_name
        )
        self._raw_events = []
        self._prepared_events = []
        self._prepared_events_df = None
        self._set_raw_event_data(event_data)

    def transform_data(self):
        if self.is_valid_object_type(self.get_event_object_type()):
            self.logger.info("Transforming data...")

            transformer = self.transformer_factory()

            self._prepared_events = []
            for raw_event in self._get_raw_events():
                transformer.set_tenancy_id(self._tenancy_id)
                transformer.set_service_instance_id(self._service_instance_id)
                transformer.set_event_timestamp_for_message(self._event_timestamp)
                self._append_prepared_event(transformer.transform_raw_event(raw_event))

            self._prepared_events_df = pd.DataFrame(self._get_prepared_events())

    def clean_data(self):
        self.logger.info("Cleaning up data...")
        transformer = self.transformer_factory()
        self._prepared_events_df = transformer.clean_prepared_events(self._get_prepared_events_df())

    def chunk_prepared_events(self, chunk_size=None):
        if chunk_size is None:
            try:
                chunk_size = int(os.getenv("DFA_BATCH_SIZE", "10000"))
            except ValueError:
                chunk_size = 10000
        self.logger.info(
            "Splitting %d events into chunks of %d",
            len(self._prepared_events),
            chunk_size,
        )
        chunks = []
        for i in range(0, len(self._prepared_events), chunk_size):
            chunks.append(self._prepared_events[i : i + chunk_size])
        self._prepared_events = chunks

    def load_data(self):
        self.logger.info("Loading transformed data to data store...")
        if len(self._prepared_events) > 0:
            self.chunk_prepared_events()
            for batched_events in self._prepared_events:
                self.logger.info(
                    "Building queries for %d %s %s",
                    len(batched_events),
                    self.get_event_object_type(),
                    self.get_operation_type(),
                )
                self.query_builder = get_query_builder(
                    self.get_event_object_type(),
                    self.get_operation_type(),
                    batched_events,
                    self.is_timeseries,
                )
                self.query_builder.execute_sql_for_events()
            self.logger.info("We executed all of the queries")
        else:
            self.logger.info(
                "No data to load for %s events for %s operation",
                self.get_event_object_type(),
                self.get_operation_type(),
            )
        AdwConnection.close()
