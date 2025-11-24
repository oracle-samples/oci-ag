# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.etl.transformers.base_event_transformer import BaseEventTransformer


class GlobalIdentityCollectionEventTransformer(BaseEventTransformer):
    def transform_raw_event(self, raw_event):
        base_gic = {
            "id": "",
            "tenancy_id": "",
            "service_instance_id": "",
            "name": "",
            "member_operation_type": "",
            "member_global_id": "",
            "display_name": "",
            "external_id": "",
            "target_id": "",
            "identity_collection_description": "",
            "risk": 0,
            "identity_collection_type": "",
            "is_managed_at_target": "",
            "status": "",
            "created_by": "",
            "created_by_display_name": "",
            "created_by_value": "",
            "created_by_resource_type": "",
            "created_on": 0,
            "updated_by": "",
            "updated_by_display_name": "",
            "updated_by_value": "",
            "updated_by_resource_type": "",
            "updated_on": 0,
            "ag_managed": "",
            "owner_display_name": "",
            "owner_value": "",
            "ownership_collection_id": "",
            "tags": "",
            "managed_by_ids": "[]",
            "owner_uids": "[]",
            "access_guardrail_ids": "[]",
            "event_object_type": "",
            "operation_type": "",
            "event_timestamp": "",
            "attributes": "{}",
        }
        gic_list = []

        try:
            if self._get_tenancy_id():
                base_gic["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                base_gic["service_instance_id"] = self._get_service_instance_id()

            if self._get_event_timestamp():
                base_gic["event_timestamp"] = self._get_event_timestamp()

            if "id" in raw_event:
                base_gic["id"] = raw_event["id"]

            if "name" in raw_event:
                base_gic["name"] = raw_event["name"]

            if "displayName" in raw_event:
                base_gic["display_name"] = raw_event["displayName"]

            if "externalId" in raw_event:
                base_gic["external_id"] = raw_event["externalId"]

            if "targetId" in raw_event:
                base_gic["target_id"] = raw_event["targetId"]

            if "agRisk" in raw_event:
                if "value" in raw_event["agRisk"]:
                    base_gic["risk"] = raw_event["agRisk"]["value"]

            if "identityCollectionDescription" in raw_event:
                base_gic["identity_collection_description"] = raw_event[
                    "identityCollectionDescription"
                ]

            if "identityCollectionType" in raw_event:
                base_gic["identity_collection_type"] = raw_event["identityCollectionType"]

            if "isManagedAtTarget" in raw_event:
                base_gic["is_managed_at_target"] = raw_event["isManagedAtTarget"]

            if "status" in raw_event:
                base_gic["status"] = raw_event["status"]

            if "createdBy" in raw_event:
                base_gic["created_by"] = raw_event["createdBy"]

            if "createdByRef" in raw_event:
                if "displayName" in raw_event["createdByRef"]:
                    base_gic["created_by_display_name"] = raw_event["createdByRef"]["displayName"]
                if "value" in raw_event["createdByRef"]:
                    base_gic["created_by_value"] = raw_event["createdByRef"]["value"]
                if "resourceType" in raw_event["createdByRef"]:
                    base_gic["created_by_resource_type"] = raw_event["createdByRef"]["resourceType"]

            if "createdOn" in raw_event:
                base_gic["created_on"] = raw_event["createdOn"]

            if "updatedBy" in raw_event:
                base_gic["updated_by"] = raw_event["updatedBy"]

            if "updatedByRef" in raw_event:
                if "displayName" in raw_event["updatedByRef"]:
                    base_gic["updated_by_display_name"] = raw_event["updatedByRef"]["displayName"]
                if "value" in raw_event["updatedByRef"]:
                    base_gic["updated_by_value"] = raw_event["updatedByRef"]["value"]
                if "resourceType" in raw_event["updatedByRef"]:
                    base_gic["updated_by_resource_type"] = raw_event["updatedByRef"]["resourceType"]

            if "updatedOn" in raw_event:
                base_gic["updated_on"] = raw_event["updatedOn"]

            if "agManaged" in raw_event:
                base_gic["ag_managed"] = raw_event["agManaged"]

            if "owner" in raw_event:
                if "displayName" in raw_event["owner"]:
                    base_gic["owner_display_name"] = raw_event["owner"]["displayName"]
                if "value" in raw_event["owner"]:
                    base_gic["owner_value"] = raw_event["owner"]["value"]

            if "ownerShipCollectionId" in raw_event:
                base_gic["ownership_collection_id"] = raw_event["ownerShipCollectionId"]

            if "tags" in raw_event:
                if (
                    raw_event["tags"] is not None
                    and isinstance(raw_event["tags"], list)
                    and len(raw_event["tags"]) > 0
                ):
                    base_gic["tags"] = ",".join(raw_event["tags"])
                else:
                    base_gic["tags"] = raw_event["tags"]

            if "managedByIds" in raw_event:
                base_gic["managed_by_ids"] = json.dumps(raw_event["managedByIds"])

            if "ownerUids" in raw_event:
                base_gic["owner_uids"] = json.dumps(raw_event["ownerUids"])

            if "accessGuardrailIds" in raw_event:
                base_gic["access_guardrail_ids"] = json.dumps(raw_event["accessGuardrailIds"])

            if "customAttributes" in raw_event:
                base_gic["attributes"] = json.dumps(raw_event["customAttributes"])

            if "tags" in raw_event:
                base_gic["tags"] = json.dumps(raw_event["tags"])

            if "managedByIds" in raw_event:
                base_gic["managed_by_ids"] = json.dumps(raw_event["managedByIds"])

            if "ownerUIDs" in raw_event:
                base_gic["owner_uids"] = json.dumps(raw_event["ownerUIDs"])

            base_gic["event_object_type"] = self.get_event_object_type()
            base_gic["operation_type"] = self.get_operation_type()

            gic_list.append(base_gic)

            add_members_list = []
            remove_members_list = []

            new_gic = {
                key: value
                for key, value in base_gic.items()
                if key in ["id", "name", "tenancy_id", "service_instance_id"]
            }
            if "add" in raw_event and "members" in raw_event["add"]:
                add_members_list = raw_event["add"]["members"]
                for member_id in add_members_list:
                    if "globalIdentityId" in member_id:
                        gic_copy = new_gic.copy()
                        gic_copy["member_operation_type"] = "add"
                        gic_copy["member_global_id"] = member_id["globalIdentityId"]
                        gic_list.append(gic_copy)

            if "remove" in raw_event and "members" in raw_event["remove"]:
                remove_members_list = raw_event["remove"]["members"]
                for member_id in remove_members_list:
                    if "globalIdentityId" in member_id:
                        gic_copy = new_gic.copy()
                        gic_copy["member_operation_type"] = "remove"
                        gic_copy["member_global_id"] = member_id["globalIdentityId"]
                        gic_list.append(gic_copy)

        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        return gic_list

    def transform_stream_message(self, message):
        transformed_gic = []
        if isinstance(self._access_message_value_data(message), list):
            for event in self._access_message_value_data(message):
                transformed_gic.extend(self.transform_raw_event(event))
        else:
            transformed_gic.extend(super().transform_stream_message(message))

        return transformed_gic


class GlobalIdentityCollectionCreateEventTransformer(GlobalIdentityCollectionEventTransformer):
    pass


class GlobalIdentityCollectionUpdateEventTransformer(GlobalIdentityCollectionEventTransformer):
    pass


class GlobalIdentityCollectionDeleteEventTransformer(GlobalIdentityCollectionEventTransformer):
    pass
