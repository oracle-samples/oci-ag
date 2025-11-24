# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import time
from datetime import datetime

from common.ocihelpers.stream import DataEnablementStream
from dfa.adw.connection import AdwConnection
from dfa.adw.query_builders.base_query_builder import get_query_builder
from dfa.etl.abstract_transformer import AbstractTransformer


class StreamTransformer(AbstractTransformer):
    transformer_name = "dfa_stream_transformer"
    is_timeseries = False

    _stream_manager = None
    _processing_start_time = None
    _processing_duration = None

    def __init__(self, is_timeseries=False):
        self.logger.info("Initializing StreamTransformer with is_timeseries %s", is_timeseries)
        self.is_timeseries = is_timeseries
        self.transformer_name += "_timeseries" if is_timeseries else ""
        self._stream_manager = DataEnablementStream(self.transformer_name)

    def _start_processing_timer(self):
        self._processing_start_time = time.perf_counter()
        self.logger.info(
            "start processing time at: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        )

    def _end_processing_timer(self):
        self._processing_duration = time.perf_counter() - self._processing_start_time
        self.logger.info("Took %f seconds to write to data store", self._processing_duration)

    def _set_raw_event_data(self, event_data):
        self.logger.info("Received %d events to process", len(self._get_raw_events()))
        self._raw_events = event_data

    def _append_prepared_event(self, event):
        self.logger.info("Appending prepared event...")
        if event is None or len(event) == 0:
            self.logger.info("Event to append is empty or None")
            return

        if not isinstance(event, list):
            event = [event]

        prepared_events = {
            "event_object_type": event[0]["event_object_type"],
            "operation_type": event[0]["operation_type"],
            "data": event,
        }

        if len(self._prepared_events) == 0:
            self._prepared_events.append(prepared_events)
        else:
            event_operation_found = False
            for prepared_event_operation in self._prepared_events:
                if (
                    prepared_event_operation["event_object_type"]
                    == prepared_events["event_object_type"]
                    and prepared_event_operation["operation_type"]
                    == prepared_events["operation_type"]
                ):

                    event_operation_found = True
                    prepared_event_operation["data"].extend(prepared_events["data"])
                    break

            if not event_operation_found:
                self._prepared_events.append(prepared_events)

    def transform_messages(self, messages):
        self._start_processing_timer()
        self._set_raw_event_data(messages)
        self.transform_data()

    def extract_data(self):
        pass

    def transform_data(self):
        self.logger.info("Transforming stream message data...")
        self._prepared_events = []

        for event_type in self._get_raw_events():
            self._event_object_type = None
            self._operation_type = None

            if not self.is_valid_object_type(event_type):
                self.logger.info("Skipping processing for event of type %s", event_type)
                continue

            operations = self._get_raw_events()[event_type].keys()
            for operation in operations:
                self._event_object_type = event_type
                self._operation_type = operation
                transformer = self.transformer_factory()
                event_type_ops_messages = self._get_raw_events()[event_type][operation]

                for message in event_type_ops_messages:
                    self._append_prepared_event(transformer.transform_stream_message(message))

                self.logger.info("Processed all events for %s %s", event_type, operation)

        self.logger.info("Transformed %d events", len(self._prepared_events))

    def clean_data(self):
        pass

    def load_data(self):
        self.logger.info("Loading transformed data to data store...")
        for prepared_events in self._prepared_events:
            self.logger.info(
                "Building queries for %s events for %s operation",
                prepared_events["event_object_type"],
                prepared_events["operation_type"],
            )
            query_builder = get_query_builder(
                prepared_events["event_object_type"],
                prepared_events["operation_type"],
                prepared_events["data"],
                self.is_timeseries,
            )
            query_builder.execute_sql_for_events()
        self.logger.info("We executed all of the queries")
        self._end_processing_timer()

        AdwConnection.close()
