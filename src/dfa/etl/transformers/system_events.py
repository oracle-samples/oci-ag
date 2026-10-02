# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.adw.tables.system_events import SystemEventsTimeSeriesTable
from dfa.etl.transformers.base_event_transformer import BaseEventTransformer


class SystemEventsCreateEventTransformer(BaseEventTransformer):
    def transform_raw_event(self, raw_event):
        event = SystemEventsTimeSeriesTable().get_default_row()
        event.update(
            tenancy_id=self._get_tenancy_id(),
            service_instance_id=self._get_service_instance_id() or raw_event.get("serviceInstanceId"),
            event_timestamp=self._get_event_timestamp(),
            event_object_type=self.get_event_object_type(),
            operation_type=self.get_operation_type(),
        )
        # Preserve the source timestamp verbatim, including nanoseconds. The
        # standard EVENT_TIMESTAMP column records the data-enablement envelope time.
        for source, column in (
            ("eventTimestamp", "source_event_timestamp"),
            ("sourceEventType", "source_event_type"),
            ("sourceEventId", "source_event_id"),
            ("sourceEventTypeVersion", "source_event_type_version"),
            ("sourceOrigin", "source_origin"),
            ("sourceAction", "source_action"),
            ("sourceRequestId", "source_request_id"),
        ):
            if source in raw_event:
                event[column] = raw_event[source]
        if "data" in raw_event:
            event["data"] = json.dumps(raw_event["data"])
        return [event]

    def transform_stream_message(self, message):
        data = self._access_message_value_data(message)
        events = data if isinstance(data, list) else [data]
        return [row for event in events for row in self.transform_raw_event(event)]
