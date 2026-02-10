# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
# pylint: disable=import-outside-toplevel

import json
import re

from dfa.adw.tables.cloud_policy import CloudPolicyStateTable
from dfa.etl.transformers.base_event_transformer import BaseEventTransformer


class CloudPolicyEventTransformer(BaseEventTransformer):

    def _compute_permissive_score(
        self, statement: str, verb: str, resource_types, attributes_json: str
    ):
        # pylint: disable=too-many-locals
        # Action weight
        v = (verb or "").lower()
        if v == "manage":
            action = 3
        elif v == "use":
            action = 2
        elif v == "read":
            action = 1
        else:  # inspect or unknown
            action = 0

        # Resource breadth weight
        breadth = 0
        # Determine from explicit resource types list
        rt_list = resource_types if isinstance(resource_types, list) else []
        if any(str(rt).lower() == "all-resources" for rt in rt_list):
            breadth = 2
        elif any(str(rt).lower().endswith("-family") for rt in rt_list):
            breadth = 1
        else:
            # fallback: parse statement string
            s = (statement or "").lower()
            if "all-resources" in s:
                breadth = 2
            elif "-family" in s:
                breadth = 1

        # Scope weight
        scope = 0
        st = (statement or "").lower()
        if " in tenancy" in st:
            scope = 2
        elif " in compartment" in st:
            scope = 1

        # Condition modifiers
        modifiers = 0
        try:
            _ = json.loads(attributes_json) if attributes_json else {}
        except Exception:
            _ = {}
        s = (statement or "").lower()
        # Advanced policy features heuristics
        has_all = " where all {" in s
        if has_all:
            modifiers += 1  # 'all' is stricter than 'any'
        # operation-level filter reduces breadth
        if "request.operation" in s:
            modifiers += 1
            # Heuristic: smaller operation sets are stricter
            m = re.search(r"request\.operation\s*in\s*\{([^}]*)\}", s)
            if m:
                ops = [op.strip().strip("'\"") for op in m.group(1).split(",") if op.strip()]
                if 0 < len(ops) <= 3:
                    modifiers += 1
        # tag-based conditions reduce scope/breadth
        if any(
            tok in s
            for tok in [
                "target.tag.",
                "target.resource.tag.",
                "request.user.tag.",
                "request.principal.tag.",
            ]
        ):
            modifiers += 1
        # network source restriction reduces exposure
        if "request.networksource" in s:
            modifiers += 1
        # direct target id hints at resource-level restriction
        if any(tok in s for tok in ["target.id", "target.resource.id", "target.ocid"]):
            # Slightly higher weight for explicit target id use
            modifiers += 2

        raw_score = action + breadth + scope - modifiers
        # Clamp to 1..5 if any access; if everything zero, return 1 for minimal access, else min 5
        if raw_score <= 0:
            score = 1
        else:
            score = min(5, raw_score)

        return score

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
            temp_verb = base_policy_statement.get("verb", "")
            temp_resource_types = (
                raw_event.get("resourceTypes")
                if isinstance(raw_event.get("resourceTypes"), list)
                else []
            )
            score = self._compute_permissive_score(
                temp_statement, temp_verb, temp_resource_types, temp_attrs
            )
            # Dynamic-group guardrail: require instance principal + compartment constraint in where clause
            has_dynamic_group = False
            if "subjects" in raw_event and isinstance(raw_event.get("subjects"), list):
                for subj in raw_event["subjects"]:
                    try:
                        if str(subj.get("type", "")).lower() == "dynamic-group":
                            has_dynamic_group = True
                            break
                    except Exception:
                        continue
            s_lower = (temp_statement or "").lower()
            has_inst_type_clause = "request.principal.type" in s_lower and "instance" in s_lower
            has_compartment_clause = "request.principal.compartment.id" in s_lower
            if has_dynamic_group and not (has_inst_type_clause and has_compartment_clause):
                score = 5
            # If dynamic-group has the required guardrail but our generic condition parser
            # didn't recognize it (no 'where all/any'), adjust overly-permissive score down by 1
            if (
                has_dynamic_group
                and (has_inst_type_clause and has_compartment_clause)
                and score == 5
            ):
                score = 4
            # No environment-based OCID matching; guardrail relies on presence of both clauses only
            # Embed score into attributes JSON
            try:
                attrs_obj = json.loads(temp_attrs) if temp_attrs else {}
                if not isinstance(attrs_obj, dict):
                    attrs_obj = {}
            except Exception:
                attrs_obj = {}
            attrs_obj["permissive_score"] = score
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
