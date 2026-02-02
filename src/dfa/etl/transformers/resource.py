# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.adw.tables.resource import ResourceStateTable
from dfa.etl.transformers.base_event_transformer import BaseEventTransformer


class ResourceEventTransformer(BaseEventTransformer):
    def transform_raw_event(self, raw_event):
        resource = ResourceStateTable().get_default_row()

        resource_list = []
        try:
            if self._get_tenancy_id():
                resource["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                resource["service_instance_id"] = self._get_service_instance_id()

            if self._get_event_timestamp():
                resource["event_timestamp"] = self._get_event_timestamp()

            if "id" in raw_event:
                resource["id"] = raw_event["id"]

            if "description" in raw_event:
                resource["description"] = raw_event["description"]

            if "externalId" in raw_event:
                resource["external_id"] = raw_event["externalId"]

            if "resourceName" in raw_event:
                resource["resource_name"] = raw_event["resourceName"]

            if "resourceType" in raw_event:
                resource["resource_type"] = raw_event["resourceType"]

            if "targetId" in raw_event:
                resource["target_id"] = raw_event["targetId"]

            if "tenancyId" in raw_event:
                resource["tenancy_id"] = raw_event["tenancyId"]

            if "customAttributes" in raw_event:
                resource["attributes"] = json.dumps(raw_event["customAttributes"])

            resource["event_object_type"] = self.get_event_object_type()
            resource["operation_type"] = self.get_operation_type()

            resource_list.append(resource)

        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        return resource_list

    def transform_stream_message(self, message):
        transformed_resources = []
        # if isinstance(message.value['data'], list):
        if isinstance(self._access_message_value_data(message), list):
            # for event in message.value['data']:
            for event in self._access_message_value_data(message):
                transformed_resources.extend(self.transform_raw_event(event))
        else:
            transformed_resources.extend(super().transform_stream_message(message))

        return transformed_resources


class ResourceCreateEventTransformer(ResourceEventTransformer):
    pass


class ResourceUpdateEventTransformer(ResourceEventTransformer):
    pass


class ResourceDeleteEventTransformer(ResourceEventTransformer):
    pass
