# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
# pylint: disable=import-outside-toplevel

import json

from dfa.adw.tables.cloud_policy import CloudPolicyStateTable
from dfa.etl.transformers.base_event_transformer import BaseEventTransformer
from dfa.etl.transformers.policy_utils import parse_policy_statement


class CloudPolicyEventTransformer(BaseEventTransformer):

    # pylint: disable=too-many-locals
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
                elif isinstance(raw_event["location"], str):
                    base_policy_statement["location"] = raw_event["location"]

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

            if "resourceTypes" in raw_event:
                base_policy_statement["resource_type"] = raw_event["resourceTypes"]

            if "subjects" in raw_event and isinstance(raw_event.get("subjects"), list):
                subject_set = raw_event["subjects"]
            else:
                subject_set = set()

            # Compute permissiveness for the base statement once
            # It may be duplicated per resource/subject pair but same score
            temp_attrs = base_policy_statement.get("attributes")
            temp_statement = base_policy_statement.get("statement", "")
            score = 0
            results = parse_policy_statement(temp_statement)
            if results:
                score = results.get("risk_score", 0)

            # Embed score into attributes JSON
            try:
                attrs_obj = json.loads(temp_attrs) if temp_attrs else {}
                if not isinstance(attrs_obj, dict):
                    attrs_obj = {}
            except Exception:
                attrs_obj = {}
            attrs_obj["permissive_score"] = int(score)
            base_policy_statement["attributes"] = json.dumps(attrs_obj)

            if subject_set:
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
