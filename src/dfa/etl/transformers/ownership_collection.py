# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.etl.transformers.base_event_transformer import BaseEventTransformer
from dfa.adw.tables.ownership_collection import OwnershipCollectionStateTable

class OwnershipCollectionEventTransformer(BaseEventTransformer):
    def transform_raw_event(self, raw_event):
        ownership_collection = OwnershipCollectionStateTable().get_default_row()
        ownership_collection_list = []

        try:
            if self._get_tenancy_id():
                ownership_collection["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                ownership_collection["service_instance_id"] = self._get_service_instance_id()

            if self._get_event_timestamp():
                ownership_collection["event_timestamp"] = self._get_event_timestamp()

            if self.get_operation_type() == "DELETE":
                if "id" in raw_event:
                    ownership_collection["id"] = raw_event["id"]
            else:
                if "ownershipCollectionId" in raw_event:
                    ownership_collection["id"] = raw_event["ownershipCollectionId"]

            if "entityId" in raw_event:
                ownership_collection["entity_id"] = raw_event["entityId"]

            if "entityName" in raw_event:
                ownership_collection["entity_name"] = raw_event["entityName"]

            if "isPrimary" in raw_event:
                ownership_collection["is_primary"] = raw_event["isPrimary"]

            if "externalId" in raw_event:
                ownership_collection["external_id"] = raw_event["externalId"]

            if "usageName" in raw_event:
                ownership_collection["resource_name"] = raw_event["usageName"]

            if "timeCreated" in raw_event:
                ownership_collection["created_on"] = raw_event["timeCreated"]

            if "lastModified" in raw_event:
                ownership_collection["updated_on"] = raw_event["lastModified"]

            if "customAttributes" in raw_event:
                ownership_collection["attributes"] = json.dumps(raw_event["customAttributes"])

            ownership_collection["event_object_type"] = self.get_event_object_type()
            ownership_collection["operation_type"] = self.get_operation_type()

            ownership_collection_list.append(ownership_collection)

        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        return ownership_collection_list

    def transform_stream_message(self, message):
        transformed_ownership_collections = []
        # if isinstance(message.value['data'], list):
        if isinstance(self._access_message_value_data(message), list):
            # for event in message.value['data']:
            for event in self._access_message_value_data(message):
                transformed_ownership_collections.extend(self.transform_raw_event(event))
        else:
            transformed_ownership_collections.extend(super().transform_stream_message(message))

        return transformed_ownership_collections


class OwnershipCollectionCreateEventTransformer(OwnershipCollectionEventTransformer):
    pass


class OwnershipCollectionUpdateEventTransformer(OwnershipCollectionEventTransformer):
    pass


class OwnershipCollectionDeleteEventTransformer(OwnershipCollectionEventTransformer):
    pass
