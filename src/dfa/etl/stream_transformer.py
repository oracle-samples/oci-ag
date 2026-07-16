# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

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
        self.is_timeseries = is_timeseries
        self.transformer_name += (
            "_timeseries" if is_timeseries and self.transformer_name != "dfa_audit_transformer" else ""
        )
        self._stream_manager = DataEnablementStream(self.transformer_name)

    def _set_raw_event_data(self, event_data):
        self._raw_events = event_data

    def transform_messages(self, messages):
        self._set_raw_event_data(messages)
        self.transform_data()

    def extract_data(self):
        pass

    def transform_data(self):
        self._prepared_events = []

        for event_type in self._get_raw_events():
            self._event_object_type = None
            self._operation_type = None

            if not self.is_valid_object_type(event_type):
                continue

            operations = self._get_raw_events()[event_type].keys()
            for operation in operations:
                self._event_object_type = event_type
                self._operation_type = operation
                transformer = self.transformer_factory()
                event_type_ops_messages = self._get_raw_events()[event_type][operation]

                for message in event_type_ops_messages:
                    self._append_prepared_event(transformer.transform_stream_message(message))

        self.logger.info(
            "%s transformed %d %s %s events",
            self.transformer_name,
            len(self._prepared_events),
            self._event_object_type,
            self._operation_type,
        )

    def load_data(self):
        try:
            prepared_events_by_operation = {}
            for event in self._prepared_events:
                key = (event["event_object_type"], event["operation_type"])
                event_data = {
                    field: value
                    for field, value in event.items()
                    if field not in {"event_object_type", "operation_type"}
                }
                prepared_events_by_operation.setdefault(key, []).append(event_data)

            for (event_object_type, operation_type), events in prepared_events_by_operation.items():
                self.logger.info(
                    "%s building queries for %d %s %s",
                    self.transformer_name,
                    len(events),
                    event_object_type,
                    operation_type,
                )
                query_builder = get_query_builder(
                    event_object_type,
                    operation_type,
                    events,
                    self.is_timeseries,
                )
                query_builder.execute_sql_for_events()
        except Exception:
            AdwConnection.rollback_and_close()
            raise

        AdwConnection.close()
