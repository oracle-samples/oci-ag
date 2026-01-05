# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.etl.transformers.base_event_transformer import BaseEventTransformer


class RoleEventTransformer(BaseEventTransformer):
    def transform_raw_event(self, raw_event):
        role = {
            "id": "",
            "tenancy_id": "",
            "service_instance_id": "",
            "external_id": "",
            "name": "",
            "description": "",
            "requestable_by": "",
            "status": "",
            "approval_workflow_id": "",
            "approval_workflow_name": "",
            "approval_workflow_description": "",
            "access_bundle_id": "",
            "created_by": "",
            "created_on": None,
            "updated_by": "",
            "updated_on": None,
            "ag_managed": "",
            "owner_display_name": "",
            "owner_value": "",
            "ownership_collection_id": "",
            "managed_by_ids": "[]",
            "owner_uids": "[]",
            "event_object_type": "",
            "operation_type": "",
            "event_timestamp": "",
            "attributes": "{}",
        }

        role_list = []

        try:
            if self._get_tenancy_id():
                role["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                role["service_instance_id"] = self._get_service_instance_id()

            if self._get_event_timestamp():
                role["event_timestamp"] = self._get_event_timestamp()

            if "id" in raw_event:
                role["id"] = raw_event["id"]

            if "externalId" in raw_event:
                role["external_id"] = raw_event["externalId"]

            if "name" in raw_event:
                role["name"] = raw_event["name"]

            if "description" in raw_event:
                role["description"] = raw_event["description"]

            if "requestableBy" in raw_event:
                role["requestable_by"] = raw_event["requestableBy"]

            if "status" in raw_event:
                role["status"] = raw_event["status"]

            if "approvalWorkflow" in raw_event:
                if "id" in raw_event["approvalWorkflow"]:
                    role["approval_workflow_id"] = raw_event["approvalWorkflow"]["id"]

                if "name" in raw_event["approvalWorkflow"]:
                    role["approval_workflow_name"] = raw_event["approvalWorkflow"]["name"]

                if "description" in raw_event["approvalWorkflow"]:
                    role["approval_workflow_description"] = raw_event["approvalWorkflow"][
                        "description"
                    ]

            if "createdBy" in raw_event:
                role["created_by"] = raw_event["createdBy"]

            if "createdOn" in raw_event:
                role["created_on"] = raw_event["createdOn"]

            if "updatedBy" in raw_event:
                role["updated_by"] = raw_event["updatedBy"]

            if "updatedOn" in raw_event:
                role["updated_on"] = raw_event["updatedOn"]

            if "agManaged" in raw_event:
                role["ag_managed"] = str(raw_event["agManaged"])

            if "owner" in raw_event:
                if "displayName" in raw_event["owner"]:
                    role["owner_display_name"] = raw_event["owner"]["displayName"]

                if "value" in raw_event["owner"]:
                    role["owner_value"] = raw_event["owner"]["value"]

            if "ownerShipCollectionId" in raw_event:
                role["ownership_collection_id"] = str(raw_event["ownerShipCollectionId"])

            if "managedByIds" in raw_event:
                role["managed_by_ids"] = json.dumps(raw_event["managedByIds"])

            if "ownerUIDs" in raw_event:
                role["owner_uids"] = json.dumps(raw_event["ownerUIDs"])

            if "customAttributes" in raw_event:
                role["attributes"] = json.dumps(raw_event["customAttributes"])

            role["event_object_type"] = self.get_event_object_type()
            role["operation_type"] = self.get_operation_type()

            if "accessBundleIds" in raw_event:
                access_bundle_ids = raw_event["accessBundleIds"]
                for ab_id in access_bundle_ids:
                    role_copy = role.copy()
                    role_copy["access_bundle_id"] = ab_id
                    role_list.append(role_copy)
            else:
                role_list.append(role)

        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        return role_list

    def transform_stream_message(self, message):
        transformed_role = []
        if isinstance(self._access_message_value_data(message), list):
            for event in self._access_message_value_data(message):
                transformed_role.extend(self.transform_raw_event(event))
        else:
            transformed_role.extend(super().transform_stream_message(message))

        return transformed_role


class RoleCreateEventTransformer(RoleEventTransformer):
    pass


class RoleUpdateEventTransformer(RoleEventTransformer):
    pass


class RoleDeleteEventTransformer(RoleEventTransformer):
    pass
