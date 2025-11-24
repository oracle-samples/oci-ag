# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.etl.transformers.base_event_transformer import BaseEventTransformer


class PolicyEventTransformer(BaseEventTransformer):
    def transform_raw_event(self, raw_event):
        base_policy = {
            "id": "",
            "tenancy_id": "",
            "service_instance_id": "",
            "name": "",
            "external_id": "",
            "description": "",
            "display_name": "",
            "status": "",
            "is_transformed_policy": "",
            "constraints": "",
            "tags": "",
            "policy_type": "",
            "policy_version": "",
            "target_id": "",
            "target_policy_id": "",
            "policy_rule_id": "",
            "policy_rule_assignment_id": "",
            "policy_rule_identity_group_id": "",
            "policy_rule_parsed_on": 0,
            "policy_rule_version": "",
            "policy_rule_action": "",
            "policy_rule_statement": "",
            "policy_rule_status": "",
            "policy_rule_type": "",
            "policy_rule_created_by": "",
            "policy_rule_created_on": 0,
            "policy_rule_updated_by": "",
            "policy_rule_updated_on": 0,
            "created_by": "",
            "created_by_display_name": "",
            "created_by_resource_type": "",
            "created_by_value": "",
            "created_on": 0,
            "updated_by": "",
            "updated_by_display_name": "",
            "updated_by_resource_type": "",
            "updated_by_value": "",
            "updated_on": 0,
            "ag_risk": 0,
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

        policy_list = []

        try:
            if self._get_tenancy_id():
                base_policy["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                base_policy["service_instance_id"] = self._get_service_instance_id()

            if self._get_event_timestamp():
                base_policy["event_timestamp"] = self._get_event_timestamp()

            if "id" in raw_event:
                base_policy["id"] = raw_event["id"]

            if "name" in raw_event:
                base_policy["name"] = raw_event["name"]

            if "externalId" in raw_event:
                base_policy["external_id"] = raw_event["externalId"]

            if "description" in raw_event:
                base_policy["description"] = raw_event["description"]

            if "displayName" in raw_event:
                base_policy["display_name"] = raw_event["displayName"]

            if "status" in raw_event:
                base_policy["status"] = raw_event["status"]

            if "isTransformedPolicy" in raw_event:
                base_policy["is_transformed_policy"] = str(raw_event["isTransformedPolicy"])

            if "constraints" in raw_event:
                base_policy["constraints"] = raw_event["constraints"]

            if "tags" in raw_event:
                base_policy["tags"] = raw_event["tags"]

            if "policyType" in raw_event:
                base_policy["policy_type"] = raw_event["policyType"]

            if "policyVersion" in raw_event:
                base_policy["policy_version"] = str(raw_event["policyVersion"])

            if "targetId" in raw_event:
                base_policy["target_id"] = raw_event["targetId"]

            if "targetPolicyId" in raw_event:
                base_policy["target_policy_id"] = raw_event["targetPolicyId"]

            if "createdBy" in raw_event:
                base_policy["created_by"] = raw_event["createdBy"]

            if "createdByRef" in raw_event:
                if "displayName" in raw_event["createdByRef"]:
                    base_policy["created_by_display_name"] = raw_event["createdByRef"][
                        "displayName"
                    ]
                if "value" in raw_event["createdByRef"]:
                    base_policy["created_by_value"] = raw_event["createdByRef"]["value"]
                if "resourceType" in raw_event["createdByRef"]:
                    base_policy["created_by_resource_type"] = raw_event["createdByRef"][
                        "resourceType"
                    ]

            if "createdOn" in raw_event:
                base_policy["created_on"] = raw_event["createdOn"]

            if "updatedBy" in raw_event:
                base_policy["updated_by"] = str(raw_event["updatedBy"])

            if "updatedByRef" in raw_event:
                if "displayName" in raw_event["updatedByRef"]:
                    base_policy["updated_by_display_name"] = raw_event["updatedByRef"][
                        "displayName"
                    ]
                if "value" in raw_event["updatedByRef"]:
                    base_policy["updated_by_value"] = raw_event["updatedByRef"]["value"]
                if "resourceType" in raw_event["updatedByRef"]:
                    base_policy["updated_by_resource_type"] = raw_event["updatedByRef"][
                        "resourceType"
                    ]

            if "updatedOn" in raw_event:
                base_policy["updated_on"] = raw_event["updatedOn"]

            if "agManaged" in raw_event:
                base_policy["ag_managed"] = str(raw_event["agManaged"])

            if "owner" in raw_event:
                if "displayName" in raw_event["owner"]:
                    base_policy["owner_display_name"] = raw_event["owner"]["displayName"]
                if "value" in raw_event["owner"]:
                    base_policy["owner_value"] = raw_event["owner"]["value"]

            if "ownerShipCollectionId" in raw_event:
                base_policy["ownership_collection_id"] = raw_event["ownerShipCollectionId"]

            if "agRisk" in raw_event:
                if "value" in raw_event["agRisk"]:
                    base_policy["ag_risk"] = raw_event["agRisk"]["value"]

            if "managedByIds" in raw_event:
                base_policy["managed_by_ids"] = json.dumps(raw_event["managedByIds"])

            if "ownerUIDs" in raw_event:
                base_policy["owner_uids"] = json.dumps(raw_event["ownerUIDs"])

            if "customAttributes" in raw_event:
                base_policy["attributes"] = json.dumps(raw_event["customAttributes"])

            base_policy["event_object_type"] = self.get_event_object_type()
            base_policy["operation_type"] = self.get_operation_type()

            if "policyRules" in raw_event:
                policy_rules_list = raw_event["policyRules"]
                for pr in policy_rules_list:
                    policy_copy = base_policy.copy()
                    if "id" in pr:
                        policy_copy["policy_rule_id"] = pr["id"]
                    if "assignmentId" in pr:
                        policy_copy["policy_rule_assignment_id"] = pr["assignmentId"]
                    if "identityGroupId" in pr:
                        policy_copy["policy_rule_identity_group_id"] = pr["identityGroupId"]
                    if "parsedOn" in pr:
                        policy_copy["policy_rule_parsed_on"] = pr["parsedOn"]
                    if "policyRuleVersion" in pr:
                        policy_copy["policy_rule_version"] = pr["policyRuleVersion"]
                    if "ruleAction" in pr:
                        policy_copy["policy_rule_action"] = pr["ruleAction"]
                    if "ruleStatement" in pr:
                        policy_copy["policy_rule_statement"] = pr["ruleStatement"]
                    if "ruleStatus" in pr:
                        policy_copy["policy_rule_status"] = pr["ruleStatus"]
                    if "ruleType" in pr:
                        policy_copy["policy_rule_type"] = pr["ruleType"]
                    if "createdBy" in pr:
                        policy_copy["policy_rule_created_by"] = pr["createdBy"]
                    if "createdOn" in pr:
                        policy_copy["policy_rule_created_on"] = pr["createdOn"]
                    if "updatedBy" in pr:
                        policy_copy["policy_rule_updated_by"] = pr["updatedBy"]
                    if "updatedOn" in pr:
                        policy_copy["policy_rule_updated_on"] = pr["updatedOn"]

                    policy_list.append(policy_copy)
            else:
                policy_list.append(base_policy)

        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        return policy_list

    def transform_stream_message(self, message):
        transformed_policy = []
        if isinstance(self._access_message_value_data(message), list):
            for event in self._access_message_value_data(message):
                transformed_policy.extend(self.transform_raw_event(event))
        else:
            transformed_policy.extend(super().transform_stream_message(message))

        return transformed_policy


class PolicyCreateEventTransformer(PolicyEventTransformer):
    pass


class PolicyUpdateEventTransformer(PolicyEventTransformer):
    pass


class PolicyDeleteEventTransformer(PolicyEventTransformer):
    pass
