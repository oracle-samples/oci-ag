# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.adw.tables.cloud_policy import CloudPolicyStateTable
from dfa.etl.transformers.base_event_transformer import BaseEventTransformer


class CloudPolicyEventTransformer(BaseEventTransformer):
    def transform_raw_event(self, raw_event):
        base_policy_statement = CloudPolicyStateTable().get_default_row()
        policies = []

        try:
            if self._get_tenancy_id():
                base_policy_statement["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                base_policy_statement["service_instance_id"] = self._get_service_instance_id()

            if self._get_event_timestamp():
                base_policy_statement["event_timestamp"] = self._get_event_timestamp()

            if "id" in raw_event:
                if self.get_operation_type() == "DELETE":
                    base_policy_statement["policy_statement_id"] = raw_event["id"]
                else:
                    base_policy_statement["id"] = raw_event["id"]

            if "compartmentId" in raw_event:
                base_policy_statement["compartment_id"] = raw_event["compartmentId"]

            if "description" in raw_event:
                base_policy_statement["description"] = raw_event["description"]

            if "externalId" in raw_event:
                base_policy_statement["external_id"] = raw_event["externalId"]

            if "location" in raw_event:
                if "compartment" in raw_event["location"]:
                    base_policy_statement["location"] = raw_event["location"]["compartment"]

            if "name" in raw_event:
                base_policy_statement["name"] = raw_event["name"]

            if "policyStatementId" in raw_event:
                if self.get_operation_type() != "DELETE":
                    base_policy_statement["policy_statement_id"] = raw_event["policyStatementId"]

            if "statement" in raw_event:
                base_policy_statement["statement"] = raw_event["statement"]

            if "targetId" in raw_event:
                base_policy_statement["target_id"] = raw_event["targetId"]

            if "verb" in raw_event:
                base_policy_statement["verb"] = raw_event["verb"]

            if "customAttributes" in raw_event:
                base_policy_statement["attributes"] = json.dumps(raw_event["customAttributes"])

            base_policy_statement["event_object_type"] = self.get_event_object_type()
            base_policy_statement["operation_type"] = self.get_operation_type()

            if "resourceTypes" in raw_event and isinstance(raw_event.get("resourceTypes"), list):
                resource_set = raw_event["resourceTypes"]
            else:
                resource_set = set()

            if "subjects" in raw_event and isinstance(raw_event.get("subjects"), list):
                subject_set = raw_event["subjects"]
            else:
                subject_set = set()

            if resource_set and subject_set:
                for resource in resource_set:
                    for subject in subject_set:
                        policy = base_policy_statement.copy()
                        policy["resource_type"] = resource
                        policy["subject_id"] = subject.get("id", "")
                        policy["subject_name"] = subject.get("name", "")
                        policy["subject_type"] = subject.get("type", "")
                        policies.append(policy)
            elif resource_set:
                for resource in resource_set:
                    policy = base_policy_statement.copy()
                    policy["resource_type"] = resource
                    policies.append(policy)
            elif subject_set:
                for subject in subject_set:
                    policy = base_policy_statement.copy()
                    policy["subject_id"] = subject.get("id", "")
                    policy["subject_name"] = subject.get("name", "")
                    policy["subject_type"] = subject.get("type", "")
                    policies.append(policy)
            else:
                policies.append(base_policy_statement)
        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        return policies

    def transform_stream_message(self, message):
        transformed_policies = []
        # if isinstance(message.value['data'], list):
        if isinstance(self._access_message_value_data(message), list):
            # for event in message.value['data']:
            for event in self._access_message_value_data(message):
                transformed_policies.extend(self.transform_raw_event(event))
        else:
            transformed_policies.extend(super().transform_stream_message(message))

        return transformed_policies


class CloudPolicyCreateEventTransformer(CloudPolicyEventTransformer):
    pass


class CloudPolicyUpdateEventTransformer(CloudPolicyEventTransformer):
    pass


class CloudPolicyDeleteEventTransformer(CloudPolicyEventTransformer):
    pass
