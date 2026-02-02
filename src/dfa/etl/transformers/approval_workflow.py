# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.adw.tables.approval_workflow import ApprovalWorkflowStateTable
from dfa.etl.transformers.base_event_transformer import BaseEventTransformer


class ApprovalWorkflowEventTransformer(BaseEventTransformer):
    def transform_raw_event(self, raw_event):
        approval_workflow = ApprovalWorkflowStateTable().get_default_row()
        aw_list = []

        try:
            if self._get_tenancy_id():
                approval_workflow["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                approval_workflow["service_instance_id"] = self._get_service_instance_id()

            if self._get_event_timestamp():
                approval_workflow["event_timestamp"] = self._get_event_timestamp()

            if "id" in raw_event:
                approval_workflow["id"] = raw_event["id"]

            if "name" in raw_event:
                approval_workflow["name"] = raw_event["name"]

            if "description" in raw_event:
                approval_workflow["description"] = raw_event["description"]

            if "status" in raw_event:
                approval_workflow["status"] = raw_event["status"]

            if "createdBy" in raw_event:
                approval_workflow["created_by"] = raw_event["createdBy"]

            if "createdOn" in raw_event:
                approval_workflow["created_on"] = raw_event["createdOn"]

            if "updatedBy" in raw_event:
                approval_workflow["updated_by"] = raw_event["updatedBy"]

            if "updatedOn" in raw_event:
                approval_workflow["updated_on"] = raw_event["updatedOn"]

            if "version" in raw_event:
                approval_workflow["version"] = str(raw_event["version"])

            if "etagVersion" in raw_event:
                approval_workflow["etag_version"] = str(raw_event["etagVersion"])

            if "tags" in raw_event:
                approval_workflow["tags"] = str(raw_event["tags"])

            if "summary" in raw_event:
                approval_workflow["summary"] = str(raw_event["summary"])

            if "ownershipCollectionId" in raw_event:
                approval_workflow["ownership_collection_id"] = raw_event["ownershipCollectionId"]

            if "customAttributes" in raw_event:
                approval_workflow["attributes"] = json.dumps(raw_event["customAttributes"])

            approval_workflow["event_object_type"] = self.get_event_object_type()
            approval_workflow["operation_type"] = self.get_operation_type()

        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        aw_list.append(approval_workflow)
        return aw_list

    def transform_stream_message(self, message):
        transformed_aw = []
        if isinstance(self._access_message_value_data(message), list):
            for event in self._access_message_value_data(message):
                transformed_aw.extend(self.transform_raw_event(event))
        else:
            transformed_aw.extend(super().transform_stream_message(message))

        return transformed_aw


class ApprovalWorkflowCreateEventTransformer(ApprovalWorkflowEventTransformer):
    pass


class ApprovalWorkflowUpdateEventTransformer(ApprovalWorkflowEventTransformer):
    pass


class ApprovalWorkflowDeleteEventTransformer(ApprovalWorkflowEventTransformer):
    pass
