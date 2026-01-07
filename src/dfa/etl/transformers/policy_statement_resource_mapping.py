# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.etl.transformers.base_event_transformer import BaseEventTransformer
from dfa.adw.tables.policy_statement_resource_mapping import PolicyStatementResourceMappingStateTable

class PolicyStatementResourceMappingEventTransformer(BaseEventTransformer):
    def transform_raw_event(self, raw_event):
        base_policy_statement_resource_mapping = PolicyStatementResourceMappingStateTable().get_default_row()
        psrm_list = []

        try:
            if self._get_tenancy_id():
                base_policy_statement_resource_mapping["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                base_policy_statement_resource_mapping["service_instance_id"] = (
                    self._get_service_instance_id()
                )

            if self._get_event_timestamp():
                base_policy_statement_resource_mapping["event_timestamp"] = (
                    self._get_event_timestamp()
                )

            if "id" in raw_event:
                base_policy_statement_resource_mapping["id"] = raw_event["id"]

            if "compartmentId" in raw_event:
                base_policy_statement_resource_mapping["compartment_id"] = raw_event[
                    "compartmentId"
                ]

            if "externalId" in raw_event:
                base_policy_statement_resource_mapping["policy_external_id"] = raw_event[
                    "externalId"
                ]

            if "policyStatementId" in raw_event:
                base_policy_statement_resource_mapping["policy_statement_id"] = raw_event[
                    "policyStatementId"
                ]

            if "targetId" in raw_event:
                base_policy_statement_resource_mapping["target_id"] = raw_event["targetId"]

            if "customAttributes" in raw_event:
                base_policy_statement_resource_mapping["attributes"] = json.dumps(
                    raw_event["customAttributes"]
                )

            base_policy_statement_resource_mapping["event_object_type"] = (
                self.get_event_object_type()
            )
            base_policy_statement_resource_mapping["operation_type"] = self.get_operation_type()

            resources_list = []

            if "resources" in raw_event:
                resources_list = raw_event["resources"]
                for r in resources_list:
                    policy_statement_resource_mapping = (
                        base_policy_statement_resource_mapping.copy()
                    )

                    if "id" in r:
                        policy_statement_resource_mapping["resource_id"] = r["id"]
                    if "externalId" in r:
                        policy_statement_resource_mapping["resource_external_id"] = r["externalId"]

                    psrm_list.append(policy_statement_resource_mapping)

            if not resources_list:
                psrm_list.append(base_policy_statement_resource_mapping)

        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        return psrm_list

    def transform_stream_message(self, message):
        transformed_psrm_events = []
        # if isinstance(message.value['data'], list):
        if isinstance(self._access_message_value_data(message), list):
            # for event in message.value['data']:
            for event in self._access_message_value_data(message):
                transformed_psrm_events.extend(self.transform_raw_event(event))
        else:
            transformed_psrm_events.extend(super().transform_stream_message(message))

        return transformed_psrm_events


class PolicyStatementResourceMappingCreateEventTransformer(
    PolicyStatementResourceMappingEventTransformer
):
    pass


class PolicyStatementResourceMappingUpdateEventTransformer(
    PolicyStatementResourceMappingEventTransformer
):
    pass


class PolicyStatementResourceMappingDeleteEventTransformer(
    PolicyStatementResourceMappingEventTransformer
):
    pass
