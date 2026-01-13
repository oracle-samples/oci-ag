# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.etl.transformers.base_event_transformer import BaseEventTransformer
from dfa.adw.tables.audit_events import AuditEventsTable

class AuditEventsEventTransformer(BaseEventTransformer):
    def transform_raw_event(self, raw_event):
        base_audit_event = AuditEventsTable().get_default_row()
        audit_events_list = []

        try:
            if self._get_tenancy_id():
                base_audit_event["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                base_audit_event["service_instance_id"] = self._get_service_instance_id()

            if self._get_event_timestamp():
                base_audit_event["event_timestamp"] = self._get_event_timestamp()

            if "source" in raw_event:
                base_audit_event["source"] = raw_event["source"]

            if "eventType" in raw_event:
                base_audit_event["audit_event_type"] = raw_event["eventType"]

            if "eventTypeVersion" in raw_event:
                base_audit_event["audit_event_type_version"] = raw_event["eventTypeVersion"]

            if "contentType" in raw_event:
                base_audit_event["content_type"] = raw_event["contentType"]

            if "region" in raw_event:
                base_audit_event["region"] = raw_event["region"]

            if "availabilityDomain" in raw_event:
                base_audit_event["availability_domain"] = raw_event["availabilityDomain"]

            if "identity" in raw_event:
                if "host" in raw_event["identity"]:
                    base_audit_event["identity_host"] = raw_event["identity"]["host"]
                if "userAgent" in raw_event["identity"]:
                    base_audit_event["identity_user_agent"] = raw_event["identity"]["userAgent"]
                if "principalId" in raw_event["identity"]:
                    base_audit_event["identity_principal_id"] = raw_event["identity"]["principalId"]

            if "request" in raw_event:
                if "requestTime" in raw_event["request"]:
                    base_audit_event["request_time"] = raw_event["request"]["requestTime"]
                if "id" in raw_event["request"]:
                    base_audit_event["request_id"] = raw_event["request"]["id"]
                if "path" in raw_event["request"]:
                    base_audit_event["request_path"] = raw_event["request"]["path"]
                if "action" in raw_event["request"]:
                    base_audit_event["request_action"] = raw_event["request"]["action"]
                if "parameters" in raw_event["request"]:
                    base_audit_event["request_parameters"] = json.dumps(
                        raw_event["request"]["parameters"]
                    )
                if "headers" in raw_event["request"]:
                    base_audit_event["request_headers"] = json.dumps(
                        raw_event["request"]["headers"]
                    )
                if "payload" in raw_event["request"]:
                    base_audit_event["request_payload"] = json.dumps(
                        raw_event["request"]["payload"]
                    )

            if "response" in raw_event:
                if "responseTime" in raw_event["response"]:
                    base_audit_event["response_time"] = raw_event["response"]["responseTime"]
                if "status" in raw_event["response"]:
                    base_audit_event["response_status"] = raw_event["response"]["status"]
                if "headers" in raw_event["response"]:
                    base_audit_event["response_headers"] = json.dumps(
                        raw_event["response"]["headers"]
                    )
                if "payload" in raw_event["response"]:
                    base_audit_event["response_payload"] = json.dumps(
                        raw_event["response"]["payload"]
                    )

            if "stateChange" in raw_event:
                base_audit_event["state_change"] = json.dumps(raw_event["stateChange"])

            if "customAttributes" in raw_event:
                base_audit_event["attributes"] = json.dumps(raw_event["customAttributes"])

            base_audit_event["event_object_type"] = self.get_event_object_type()
            base_audit_event["operation_type"] = self.get_operation_type()

            audit_events_list.append(base_audit_event)

        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        return audit_events_list


class AuditEventsCreateEventTransformer(AuditEventsEventTransformer):
    def transform_stream_message(self, message):
        transformed_audit_events = []
        # if isinstance(message.value['data'], list):

        if isinstance(self._access_message_value_data(message), list):
            # for event in message.value['data']:
            for event in self._access_message_value_data(message):
                transformed_audit_events.extend(self.transform_raw_event(event))
        else:
            transformed_audit_events.extend(super().transform_stream_message(message))

        return transformed_audit_events
