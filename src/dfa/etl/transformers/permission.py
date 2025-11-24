# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.etl.transformers.base_event_transformer import BaseEventTransformer


class PermissionEventTransformer(BaseEventTransformer):
    def transform_raw_event(self, raw_event):
        base_permission = {
            "id": "",
            "tenancy_id": "",
            "service_instance_id": "",
            "external_id": "",
            "name": "",
            "description": "",
            "display_name": "",
            "permission_type_id": "",
            "resource_id": "",
            "resource_name": "",
            "risk_level": "",
            "status": "",
            "target_id": "",
            "user_defined_tags": "",
            "owner_display_name": "",
            "owner_value": "",
            "event_object_type": "",
            "operation_type": "",
            "event_timestamp": "",
            "attributes": "{}",
        }

        permission_list = []

        try:
            if self._get_tenancy_id():
                base_permission["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                base_permission["service_instance_id"] = self._get_service_instance_id()

            if self._get_event_timestamp():
                base_permission["event_timestamp"] = self._get_event_timestamp()

            if "id" in raw_event:
                base_permission["id"] = raw_event["id"]

            if "externalId" in raw_event:
                base_permission["external_id"] = raw_event["externalId"]

            if "name" in raw_event:
                base_permission["name"] = raw_event["name"]

            if "description" in raw_event:
                base_permission["description"] = raw_event["description"]

            if "displayName" in raw_event:
                base_permission["display_name"] = raw_event["displayName"]

            if "permissionTypeId" in raw_event:
                base_permission["permission_type_id"] = raw_event["permissionTypeId"]

            if "resourceId" in raw_event:
                base_permission["resource_id"] = raw_event["resourceId"]

            if "resourceName" in raw_event:
                base_permission["resource_name"] = raw_event["resourceName"]

            if "riskLevel" in raw_event:
                base_permission["risk_level"] = raw_event["riskLevel"]

            if "status" in raw_event:
                base_permission["status"] = raw_event["status"]

            if "targetId" in raw_event:
                base_permission["target_id"] = raw_event["targetId"]

            if "userDefinedTags" in raw_event:
                base_permission["user_defined_tags"] = json.dumps(raw_event["userDefinedTags"])

            if "owner" in raw_event:
                if "displayName" in raw_event["owner"]:
                    base_permission["owner_display_name"] = raw_event["owner"]["displayName"]
                if "value" in raw_event["owner"]:
                    base_permission["owner_value"] = raw_event["owner"]["value"]

            if "customAttributes" in raw_event:
                base_permission["attributes"] = json.dumps(raw_event["customAttributes"])

            base_permission["event_object_type"] = self.get_event_object_type()
            base_permission["operation_type"] = self.get_operation_type()
            permission_list.append(base_permission)

        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        return permission_list

    def transform_stream_message(self, message):
        transformed_permission = []
        # if isinstance(message.value['data'], list):
        if isinstance(self._access_message_value_data(message), list):
            # for event in message.value['data']:
            for event in self._access_message_value_data(message):
                transformed_permission.extend(self.transform_raw_event(event))
        else:
            transformed_permission.extend(super().transform_stream_message(message))

        return transformed_permission


class PermissionCreateEventTransformer(PermissionEventTransformer):
    pass


class PermissionUpdateEventTransformer(PermissionEventTransformer):
    pass


class PermissionDeleteEventTransformer(PermissionEventTransformer):
    pass
