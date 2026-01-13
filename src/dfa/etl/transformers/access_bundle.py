# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.etl.transformers.base_event_transformer import BaseEventTransformer
from dfa.adw.tables.access_bundle import AccessBundleStateTable

class AccessBundleEventTransformer(BaseEventTransformer):
    def transform_raw_event(self, raw_event):
        base_access_bundle = AccessBundleStateTable().get_default_row()
        access_bundle_list = []

        try:
            if self._get_tenancy_id():
                base_access_bundle["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                base_access_bundle["service_instance_id"] = self._get_service_instance_id()

            if self._get_event_timestamp():
                base_access_bundle["event_timestamp"] = self._get_event_timestamp()

            if "id" in raw_event:
                base_access_bundle["id"] = raw_event["id"]

            if "externalId" in raw_event:
                base_access_bundle["external_id"] = raw_event["externalId"]

            if "name" in raw_event:
                base_access_bundle["name"] = raw_event["name"]

            if "description" in raw_event:
                base_access_bundle["description"] = raw_event["description"]

            if "displayName" in raw_event:
                base_access_bundle["display_name"] = raw_event["displayName"]

            if "requestableBy" in raw_event:
                base_access_bundle["requestable_by"] = raw_event["requestableBy"]

            if "status" in raw_event:
                base_access_bundle["status"] = raw_event["status"]

            if "approvalWorkflow" in raw_event:
                if "id" in raw_event["approvalWorkflow"]:
                    base_access_bundle["approval_workflow_id"] = raw_event["approvalWorkflow"]["id"]
                if "name" in raw_event["approvalWorkflow"]:
                    base_access_bundle["approval_workflow_name"] = raw_event["approvalWorkflow"][
                        "name"
                    ]
                if "description" in raw_event["approvalWorkflow"]:
                    base_access_bundle["approval_workflow_description"] = raw_event[
                        "approvalWorkflow"
                    ]["description"]

            if "targetId" in raw_event:
                base_access_bundle["target_id"] = raw_event["targetId"]

            if "tags" in raw_event:
                base_access_bundle["tags"] = raw_event["tags"]

            if "accessBundleType" in raw_event:
                base_access_bundle["access_bundle_type"] = raw_event["accessBundleType"]

            if "createdBy" in raw_event:
                base_access_bundle["created_by"] = raw_event["createdBy"]

            if "createdByRef" in raw_event:
                if "displayName" in raw_event["createdByRef"]:
                    base_access_bundle["created_by_display_name"] = raw_event["createdByRef"][
                        "displayName"
                    ]
                if "value" in raw_event["createdByRef"]:
                    base_access_bundle["created_by_value"] = raw_event["createdByRef"]["value"]
                if "resourceType" in raw_event["createdByRef"]:
                    base_access_bundle["created_by_resource_type"] = raw_event["createdByRef"][
                        "resourceType"
                    ]

            if "createdOn" in raw_event:
                base_access_bundle["created_on"] = raw_event["createdOn"]

            if "updatedBy" in raw_event:
                base_access_bundle["updated_by"] = raw_event["updatedBy"]

            if "updatedByRef" in raw_event:
                if "displayName" in raw_event["updatedByRef"]:
                    base_access_bundle["updated_by_display_name"] = raw_event["updatedByRef"][
                        "displayName"
                    ]
                if "value" in raw_event["updatedByRef"]:
                    base_access_bundle["updated_by_value"] = raw_event["updatedByRef"]["value"]
                if "resourceType" in raw_event["updatedByRef"]:
                    base_access_bundle["updated_by_resource_type"] = raw_event["updatedByRef"][
                        "resourceType"
                    ]

            if "updatedOn" in raw_event:
                base_access_bundle["updated_on"] = raw_event["updatedOn"]

            if "agManaged" in raw_event:
                base_access_bundle["ag_managed"] = raw_event["agManaged"]

            if "owner" in raw_event:
                if "displayName" in raw_event["owner"]:
                    base_access_bundle["owner_display_name"] = raw_event["owner"]["displayName"]
                if "value" in raw_event["owner"]:
                    base_access_bundle["owner_value"] = raw_event["owner"]["value"]

            if "ownerShipCollectionId" in raw_event:
                base_access_bundle["ownership_collection_id"] = raw_event["ownerShipCollectionId"]

            if "managedByIds" in raw_event:
                base_access_bundle["managed_by_ids"] = json.dumps(raw_event["managedByIds"])

            if "ownerUIDs" in raw_event:
                base_access_bundle["owner_uids"] = json.dumps(raw_event["ownerUIDs"])

            if "isAccountProfileExists" in raw_event:
                base_access_bundle["account_profile_exists"] = raw_event["isAccountProfileExists"]

            if "accountProfileId" in raw_event:
                base_access_bundle["account_profile_id"] = raw_event["accountProfileId"]

            if "accountProfileName" in raw_event:
                base_access_bundle["account_profile_name"] = raw_event["accountProfileName"]

            if "autoApproveIfNoViolation" in raw_event:
                base_access_bundle["auto_approval_if_no_violation"] = raw_event[
                    "autoApproveIfNoViolation"
                ]

            if "accessLimitType" in raw_event:
                base_access_bundle["access_limit_type"] = raw_event["accessLimitType"]

            if "expirationTime" in raw_event:
                base_access_bundle["expiration_time"] = raw_event["expirationTime"]

            if "notificationTime" in raw_event:
                base_access_bundle["notification_time"] = raw_event["notificationTime"]

            if "extensionTime" in raw_event:
                base_access_bundle["extension_time"] = raw_event["extensionTime"]

            if "extensionApprovalWorkflow" in raw_event:
                if "id" in raw_event["extensionApprovalWorkflow"]:
                    base_access_bundle["extension_approval_workflow_id"] = raw_event[
                        "extensionApprovalWorkflow"
                    ]["id"]
                if "name" in raw_event["extensionApprovalWorkflow"]:
                    base_access_bundle["extension_approval_workflow_name"] = raw_event[
                        "extensionApprovalWorkflow"
                    ]["name"]
                if "description" in raw_event["extensionApprovalWorkflow"]:
                    base_access_bundle["extension_approval_workflow_description"] = raw_event[
                        "extensionApprovalWorkflow"
                    ]["description"]

            if "accessGuardrailIds" in raw_event:
                base_access_bundle["access_guardrail_ids"] = json.dumps(
                    raw_event["accessGuardrailIds"]
                )

            if "customAttributes" in raw_event:
                base_access_bundle["attributes"] = json.dumps(raw_event["customAttributes"])

            base_access_bundle["event_object_type"] = self.get_event_object_type()
            base_access_bundle["operation_type"] = self.get_operation_type()

            if "permissionIds" in raw_event and isinstance(raw_event.get("permissionIds"), list):
                permission_ids_list = raw_event["permissionIds"]
                for permission_id in permission_ids_list:
                    access_bundle_copy = base_access_bundle.copy()
                    access_bundle_copy["permission_id"] = permission_id
                    access_bundle_list.append(access_bundle_copy)
            else:
                access_bundle_list.append(base_access_bundle)

        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        return access_bundle_list

    def transform_stream_message(self, message):
        transformed_access_bundle = []
        if isinstance(self._access_message_value_data(message), list):
            for event in self._access_message_value_data(message):
                transformed_access_bundle.extend(self.transform_raw_event(event))
        else:
            transformed_access_bundle.extend(super().transform_stream_message(message))

        return transformed_access_bundle


class AccessBundleCreateEventTransformer(AccessBundleEventTransformer):
    pass


class AccessBundleUpdateEventTransformer(AccessBundleEventTransformer):
    pass


class AccessBundleDeleteEventTransformer(AccessBundleEventTransformer):
    pass
