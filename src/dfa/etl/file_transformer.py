# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json
import os
from datetime import datetime, timezone

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
    _num_of_batches = None
    _snapshot_id = None
    _snapshot_status = None

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
        self._num_of_batches = None
        self._snapshot_status = None

    @staticmethod
    def _parse_int_header_value(value):
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                return int(value.strip())
            except ValueError:
                return None
        return None

    def _apply_headers(self, headers):
        if "messageType" in headers:
            self._event_object_type = headers["messageType"]
        if "operation" in headers:
            self._operation_type = headers["operation"]
        if "eventTime" in headers:
            self._event_timestamp = headers["eventTime"]
        if "correlationId" in headers:
            self._snapshot_id = headers["correlationId"]
        if "status" in headers:
            self._snapshot_status = headers["status"]
        if "tenancyId" in headers:
            self._tenancy_id = headers["tenancyId"]
        if "serviceInstanceId" in headers:
            self._service_instance_id = headers["serviceInstanceId"]

        parsed_num_of_batches = None
        if "numOfBatches" in headers:
            parsed_num_of_batches = self._parse_int_header_value(headers["numOfBatches"])
        if parsed_num_of_batches is not None:
            self._num_of_batches = parsed_num_of_batches

    def _set_raw_event_data(self, event_data):
        content = event_data.data.content.decode("utf-8")

        if self._object_name.endswith(".jsonl"):
            for line in content.splitlines():
                raw_event = json.loads(line)
                if "headers" in raw_event:
                    self._apply_headers(raw_event["headers"])
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
                self._raw_events = [raw_data]
            elif isinstance(raw_data, list):
                self._raw_events = raw_data
            else:
                self.logger.warning("Cannot process event content - unknown format was found.")
            headers = outer.get("headers", {})
            if headers:
                self._apply_headers(headers)

        if not self.is_valid_object_type(self.get_event_object_type()):
            self.logger.info("Skipping processing for event of type %s", self.get_event_object_type())

    def extract_data(self):
        self.logger.info("Extracting data from object storage.")
        event_data = self._object_storage_client.download(self._namespace, self._bucket_name, self._object_name)
        self._raw_events = []
        self._prepared_events = []
        self._snapshot_id = None
        self._num_of_batches = None
        self._snapshot_status = None
        self._set_raw_event_data(event_data)

    def _get_snapshot_id_for_batch(self):
        return self._snapshot_id or self._object_name

    def _get_batch_id_for_batch(self):
        object_file_name = self._object_name.rsplit("/", 1)[-1]
        if object_file_name.endswith(".jsonl"):
            return object_file_name[: -len(".jsonl")]
        return object_file_name

    def _is_snapshot_completion_marker(self):
        return (
            len(self._raw_events) == 0
            and self._num_of_batches is not None
            and isinstance(self._snapshot_status, str)
            and self._snapshot_status.strip().upper() == "COMPLETED"
        )

    def _get_utc_current_event_timestamp(self):
        return datetime.fromisoformat(self._event_timestamp).astimezone(timezone.utc).strftime("%d-%b-%y %H:%M:%S.%f")

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

    def chunk_prepared_events(self, chunk_size=None):
        if chunk_size is None:
            try:
                chunk_size = int(os.getenv("DFA_BATCH_SIZE", "10000"))
            except ValueError:
                chunk_size = 10000

        chunks = []
        for i in range(0, len(self._prepared_events), chunk_size):
            chunks.append(self._prepared_events[i : i + chunk_size])
        self._prepared_events = chunks

    def load_data(self):
        self.logger.info("Loading %d transformed data to data store...", len(self._prepared_events))
        try:
            current_query_builder = None
            snapshot_query_builder = None
            should_track_snapshot = (
                not self.is_timeseries
                and self.get_operation_type() == "CREATE"
                and self.is_valid_object_type(self.get_event_object_type())
            )
            is_snapshot_completion_marker = self._is_snapshot_completion_marker()
            should_track_snapshot_batch = should_track_snapshot and not is_snapshot_completion_marker
            should_finalize_snapshot = should_track_snapshot and is_snapshot_completion_marker
            if should_track_snapshot_batch or should_finalize_snapshot:
                snapshot_query_builder = get_query_builder(
                    self.get_event_object_type(),
                    self.get_operation_type(),
                    [],
                    self.is_timeseries,
                )
                self.query_builder = snapshot_query_builder

            if len(self._prepared_events) > 0:
                self.chunk_prepared_events()
                for batched_events in self._prepared_events:
                    self.logger.info(
                        "Building queries for %d %s %s",
                        len(batched_events),
                        self.get_event_object_type(),
                        self.get_operation_type(),
                    )
                    current_query_builder = get_query_builder(
                        self.get_event_object_type(),
                        self.get_operation_type(),
                        batched_events,
                        self.is_timeseries,
                        retry_merge_conflicts=not self.is_timeseries,
                    )
                    self.query_builder = current_query_builder
                    self.query_builder.execute_sql_for_events()

            if should_track_snapshot_batch and snapshot_query_builder is not None:
                snapshot_query_builder.register_snapshot_batch_completed(
                    snapshot_id=self._get_snapshot_id_for_batch(),
                    batch_id=self._get_batch_id_for_batch(),
                    event_timestamp=self._get_utc_current_event_timestamp(),
                    tenancy_id=self._tenancy_id,
                    service_instance_id=self._service_instance_id,
                )
            if should_finalize_snapshot and snapshot_query_builder is not None:
                if self._num_of_batches is not None:
                    snapshot_query_builder.finalize_snapshot_cleanup_if_ready(
                        snapshot_id=self._get_snapshot_id_for_batch(),
                        num_of_batches=self._num_of_batches,
                        tenancy_id=self._tenancy_id,
                        service_instance_id=self._service_instance_id,
                    )
        except Exception:
            AdwConnection.rollback_and_close()
            raise

        AdwConnection.close()
