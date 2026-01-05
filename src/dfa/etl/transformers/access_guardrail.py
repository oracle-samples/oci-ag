# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.etl.transformers.base_event_transformer import BaseEventTransformer


class AccessGuardrailEventTransformer(BaseEventTransformer):
    def transform_raw_event(self, raw_event):
        base_access_guardrail = {
            "id": "",
            "tenancy_id": "",
            "service_instance_id": "",
            "external_id": "",
            "name": "",
            "description": "",
            "action_on_failure_action_type": "",
            "action_on_failure_revoke_after_number_of_days": None,
            "action_on_failure_risk": "",
            "action_on_failure_should_user_manager_be_notified": "",
            "created_by_display_name": "",
            "created_by_resource_type": "",
            "created_by_value": "",
            "created_on": None,
            "etag": "",
            "is_detective_violation_check_enabled": "",
            "lifecycle_state": "",
            "owner_display_name": "",
            "owner_value": "",
            "ownership_collection_id": "",
            "rules": "[]",
            "tags": "",
            "updated_by_display_name": "",
            "updated_by_resource_type": "",
            "updated_by_value": "",
            "updated_on": None,
            "event_object_type": "",
            "operation_type": "",
            "event_timestamp": "",
            "attributes": "{}",
        }

        access_guardrail_list = []

        try:
            if self._get_tenancy_id():
                base_access_guardrail["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                base_access_guardrail["service_instance_id"] = self._get_service_instance_id()

            if self._get_event_timestamp():
                base_access_guardrail["event_timestamp"] = self._get_event_timestamp()

            if "id" in raw_event:
                base_access_guardrail["id"] = raw_event["id"]

            if "externalId" in raw_event:
                base_access_guardrail["external_id"] = raw_event["externalId"]

            if "name" in raw_event:
                base_access_guardrail["name"] = raw_event["name"]

            if "description" in raw_event:
                base_access_guardrail["description"] = raw_event["description"]

            if "actionOnFailure" in raw_event:
                if "actionType" in raw_event["actionOnFailure"]:
                    base_access_guardrail["action_on_failure_action_type"] = raw_event[
                        "actionOnFailure"
                    ]["actionType"]

                if "revokeLaterAfterNumberOfDays" in raw_event["actionOnFailure"]:
                    base_access_guardrail["action_on_failure_revoke_after_number_of_days"] = (
                        raw_event["actionOnFailure"]["revokeLaterAfterNumberOfDays"]
                    )

                if "risk" in raw_event["actionOnFailure"]:
                    base_access_guardrail["action_on_failure_risk"] = raw_event["actionOnFailure"][
                        "risk"
                    ]

                if "shouldUserManagerBeNotified" in raw_event["actionOnFailure"]:
                    base_access_guardrail["action_on_failure_should_user_manager_be_notified"] = (
                        str(raw_event["actionOnFailure"]["shouldUserManagerBeNotified"])
                    )

            if "createdByRef" in raw_event:
                if "displayName" in raw_event["createdByRef"]:
                    base_access_guardrail["created_by_display_name"] = raw_event["createdByRef"][
                        "displayName"
                    ]
                if "value" in raw_event["createdByRef"]:
                    base_access_guardrail["created_by_value"] = raw_event["createdByRef"]["value"]
                if "resourceType" in raw_event["createdByRef"]:
                    base_access_guardrail["created_by_resource_type"] = raw_event["createdByRef"][
                        "resourceType"
                    ]

            if "createdOn" in raw_event:
                base_access_guardrail["created_on"] = raw_event["createdOn"]

            if "etag" in raw_event:
                base_access_guardrail["etag"] = raw_event["etag"]

            if "isDetectiveViolationCheckEnabled" in raw_event:
                base_access_guardrail["is_detective_violation_check_enabled"] = str(
                    raw_event["isDetectiveViolationCheckEnabled"]
                )

            if "lifecycleState" in raw_event:
                base_access_guardrail["lifecycle_state"] = raw_event["lifecycleState"]

            if "owner" in raw_event:
                if "displayName" in raw_event["owner"]:
                    base_access_guardrail["owner_display_name"] = raw_event["owner"]["displayName"]

                if "value" in raw_event["owner"]:
                    base_access_guardrail["owner_value"] = raw_event["owner"]["value"]

            if "ownerShipCollectionId" in raw_event:
                base_access_guardrail["ownership_collection_id"] = str(
                    raw_event["ownerShipCollectionId"]
                )

            if "updatedByRef" in raw_event:
                if "displayName" in raw_event["updatedByRef"]:
                    base_access_guardrail["updated_by_display_name"] = raw_event["updatedByRef"][
                        "displayName"
                    ]
                if "value" in raw_event["updatedByRef"]:
                    base_access_guardrail["updated_by_value"] = raw_event["updatedByRef"]["value"]
                if "resourceType" in raw_event["updatedByRef"]:
                    base_access_guardrail["updated_by_resource_type"] = raw_event["updatedByRef"][
                        "resourceType"
                    ]

            if "updatedOn" in raw_event:
                base_access_guardrail["updated_on"] = raw_event["updatedOn"]

            if "tags" in raw_event:
                base_access_guardrail["tags"] = raw_event["tags"]

            if "customAttributes" in raw_event:
                base_access_guardrail["attributes"] = json.dumps(raw_event["customAttributes"])

            base_access_guardrail["event_object_type"] = self.get_event_object_type()
            base_access_guardrail["operation_type"] = self.get_operation_type()

            if "rules" in raw_event:
                base_access_guardrail["rules"] = json.dumps(raw_event["rules"])

            access_guardrail_list.append(base_access_guardrail)

        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        return access_guardrail_list

    def transform_stream_message(self, message):
        transformed_access_guardrail = []
        if isinstance(self._access_message_value_data(message), list):
            for event in self._access_message_value_data(message):
                transformed_access_guardrail.extend(self.transform_raw_event(event))
        else:
            transformed_access_guardrail.extend(super().transform_stream_message(message))

        return transformed_access_guardrail


class AccessGuardrailCreateEventTransformer(AccessGuardrailEventTransformer):
    pass


class AccessGuardrailUpdateEventTransformer(AccessGuardrailEventTransformer):
    pass


class AccessGuardrailDeleteEventTransformer(AccessGuardrailEventTransformer):
    pass
