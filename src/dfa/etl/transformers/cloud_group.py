# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.adw.tables.cloud_group import CloudGroupStateTable
from dfa.etl.transformers.base_event_transformer import BaseEventTransformer


class CloudGroupEventTransformer(BaseEventTransformer):
    def transform_raw_event(self, raw_event):
        base_group = CloudGroupStateTable().get_default_row()
        group_list = []

        try:
            if self._get_tenancy_id():
                base_group["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                base_group["service_instance_id"] = self._get_service_instance_id()

            if self._get_event_timestamp():
                base_group["event_timestamp"] = self._get_event_timestamp()

            if "id" in raw_event:
                base_group["id"] = raw_event["id"]

            if "externalId" in raw_event:
                base_group["external_id"] = raw_event["externalId"]

            if "targetId" in raw_event:
                base_group["target_id"] = raw_event["targetId"]

            if "compartmentId" in raw_event:
                base_group["compartment_id"] = raw_event["compartmentId"]

            if "name" in raw_event:
                base_group["name"] = raw_event["name"]

            if "domainId" in raw_event:
                base_group["domain_id"] = raw_event["domainId"]

            if "id" in raw_event:
                base_group["group_membership_type"] = raw_event["id"].split(".")[1]

            if "customAttributes" in raw_event:
                base_group["attributes"] = json.dumps(raw_event["customAttributes"])

            base_group["event_object_type"] = self.get_event_object_type()
            base_group["operation_type"] = self.get_operation_type()

            group_list.append(base_group)

            add_identities = []
            remove_identities = []

            if "add" in raw_event and "identities" in raw_event["add"]:
                add_identities = raw_event["add"]["identities"]
                for i in add_identities:
                    group = base_group.copy()
                    group["identity_operation_type"] = "add"

                    if "id" in i:
                        group["identity_global_id"] = i["id"]
                    if "targetIdentityId" in i:
                        group["identity_target_identity_id"] = i["targetIdentityId"]
                    if "externalId" in i:
                        group["identity_external_id"] = i["externalId"]

                    group_list.append(group)

            if "remove" in raw_event and "identities" in raw_event["remove"]:
                remove_identities = raw_event["remove"]["identities"]
                for i in remove_identities:
                    group = base_group.copy()
                    group["identity_operation_type"] = "remove"

                    if "id" in i:
                        group["identity_global_id"] = i["id"]
                    if "targetIdentityId" in i:
                        group["identity_target_identity_id"] = i["targetIdentityId"]
                    if "externalId" in i:
                        group["identity_external_id"] = i["externalId"]

                    group_list.append(group)

            if not add_identities and not remove_identities:
                group_list.append(base_group)

        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        return group_list

    def transform_stream_message(self, message):
        transformed_groups = []
        # if isinstance(message.value['data'], list):
        if isinstance(self._access_message_value_data(message), list):
            # for event in message.value['data']:
            for event in self._access_message_value_data(message):
                transformed_groups.extend(self.transform_raw_event(event))
        else:
            transformed_groups.extend(super().transform_stream_message(message))

        return transformed_groups


class CloudGroupCreateEventTransformer(CloudGroupEventTransformer):
    pass


class CloudGroupUpdateEventTransformer(CloudGroupEventTransformer):
    pass


class CloudGroupDeleteEventTransformer(CloudGroupEventTransformer):
    pass
