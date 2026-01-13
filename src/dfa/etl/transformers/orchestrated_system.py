# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.etl.transformers.base_event_transformer import BaseEventTransformer
from dfa.adw.tables.orchestrated_system import OrchestratedSystemStateTable

class OrchestratedSystemEventTransformer(BaseEventTransformer):
    def transform_raw_event(self, raw_event):
        orchestrated_system = OrchestratedSystemStateTable().get_default_row()
        orchestrated_system_list = []

        try:
            if self._get_tenancy_id():
                orchestrated_system["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                orchestrated_system["service_instance_id"] = self._get_service_instance_id()

            if self._get_event_timestamp():
                orchestrated_system["event_timestamp"] = self._get_event_timestamp()

            if "id" in raw_event:
                orchestrated_system["id"] = raw_event["id"]

            if "name" in raw_event:
                orchestrated_system["name"] = raw_event["name"]

            if "type" in raw_event:
                orchestrated_system["type"] = raw_event["type"]

            if "state" in raw_event:
                orchestrated_system["state"] = raw_event["state"]

            if "createdBy" in raw_event:
                orchestrated_system["created_by"] = raw_event["createdBy"]

            if "targetMode" in raw_event:
                orchestrated_system["target_mode"] = raw_event["targetMode"]

            if "timeCreated" in raw_event:
                orchestrated_system["created_on"] = raw_event["timeCreated"]

            if "timeUpdated" in raw_event:
                orchestrated_system["updated_on"] = raw_event["timeUpdated"]

            if "ownershipCollectionId" in raw_event:
                orchestrated_system["ownership_collection_id"] = raw_event["ownershipCollectionId"]

            if "primaryOwner" in raw_event:
                orchestrated_system["primary_owner"] = raw_event["primaryOwner"]

            if "customAttributes" in raw_event:
                orchestrated_system["attributes"] = json.dumps(raw_event["customAttributes"])

            orchestrated_system["event_object_type"] = self.get_event_object_type()
            orchestrated_system["operation_type"] = self.get_operation_type()

            orchestrated_system_list.append(orchestrated_system)

        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        return orchestrated_system_list

    def transform_stream_message(self, message):
        transformed_orchestrated_systems = []
        # if isinstance(message.value['data'], list):
        if isinstance(self._access_message_value_data(message), list):
            # for event in message.value['data']:
            for event in self._access_message_value_data(message):
                transformed_orchestrated_systems.extend(self.transform_raw_event(event))
        else:
            transformed_orchestrated_systems.extend(super().transform_stream_message(message))

        return transformed_orchestrated_systems


class OrchestratedSystemCreateEventTransformer(OrchestratedSystemEventTransformer):
    pass


class OrchestratedSystemUpdateEventTransformer(OrchestratedSystemEventTransformer):
    pass


class OrchestratedSystemDeleteEventTransformer(OrchestratedSystemEventTransformer):
    pass
