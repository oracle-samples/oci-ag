# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.etl.transformers.base_event_transformer import BaseEventTransformer


class IdentityEventTransformer(BaseEventTransformer):

    def transform_raw_event(self, raw_event):
        identity = {
            "id": "",
            "tenancy_id": "",
            "service_instance_id": "",
            "ag_status": "",
            "ag_sub_type": "",
            "display_name": "",
            "location": "",
            "risk": None,
            "ag_risk_attributes": "{}",
            "status": "",
            "username": "",
            "last_name": "",
            "first_name": "",
            "identity_attributes": "{}",
            "ti_external_id": "",
            "ti_id": "",
            "ti_target_id": "",
            "ti_domain_id": "",
            "ti_identity_name": "",
            "ti_identity_status": "",
            "ti_operation_type": "",
            "ti_event_timestamp": "",
            "ti_attributes": "{}",
            "identity_type": "",
            "event_object_type": "",
            "operation_type": "",
            "event_timestamp": "",
        }
        identities_list = []

        try:

            if self._get_tenancy_id():
                identity["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                identity["service_instance_id"] = self._get_service_instance_id()

            if self._get_event_timestamp():
                identity["event_timestamp"] = self._get_event_timestamp()

            identity["event_object_type"] = self.get_event_object_type()
            identity["operation_type"] = self.get_operation_type()

            if "id" in raw_event["globalIdentity"]:
                identity["id"] = raw_event["globalIdentity"]["id"]
                identity["identity_type"] = raw_event["globalIdentity"]["id"].split(".")[1]

            if "identity" in raw_event["globalIdentity"]:

                if "agStatus" in raw_event["globalIdentity"]["identity"]:
                    identity["ag_status"] = raw_event["globalIdentity"]["identity"]["agStatus"]

                if "agSubType" in raw_event["globalIdentity"]["identity"]:
                    identity["ag_sub_type"] = raw_event["globalIdentity"]["identity"]["agSubType"]

                if "displayName" in raw_event["globalIdentity"]["identity"]:
                    identity["display_name"] = raw_event["globalIdentity"]["identity"][
                        "displayName"
                    ]

                if "location" in raw_event["globalIdentity"]["identity"]:
                    identity["location"] = raw_event["globalIdentity"]["identity"]["location"]

                if "agRisk" in raw_event["globalIdentity"]["identity"]:
                    identity["risk"] = raw_event["globalIdentity"]["identity"]["agRisk"]["value"]
                    if "customAttributes" in raw_event["globalIdentity"]["identity"]["agRisk"]:
                        identity["ag_risk_attributes"] = json.dumps(
                            raw_event["globalIdentity"]["identity"]["agRisk"]["customAttributes"]
                        )

                if "status" in raw_event["globalIdentity"]["identity"]:
                    identity["status"] = raw_event["globalIdentity"]["identity"]["status"]

                if "userName" in raw_event["globalIdentity"]["identity"]:
                    identity["username"] = raw_event["globalIdentity"]["identity"]["userName"]

                if "name" in raw_event["globalIdentity"]["identity"]:
                    if "familyName" in raw_event["globalIdentity"]["identity"]["name"]:
                        identity["last_name"] = raw_event["globalIdentity"]["identity"]["name"][
                            "familyName"
                        ]
                    if "givenName" in raw_event["globalIdentity"]["identity"]["name"]:
                        identity["first_name"] = raw_event["globalIdentity"]["identity"]["name"][
                            "givenName"
                        ]

                identity["identity_attributes"] = json.dumps(
                    raw_event["globalIdentity"]["identity"]
                )

                if "targetIdentities" in raw_event["globalIdentity"]:
                    identity["ti_operation_type"] = self.get_operation_type()
                    target_identity_list = raw_event["globalIdentity"]["targetIdentities"]
                    if len(target_identity_list) > 0:
                        for ti in target_identity_list:
                            target_identity = identity.copy()
                            if self._get_event_timestamp():
                                target_identity["ti_event_timestamp"] = self._get_event_timestamp()
                            if "externalId" in ti:
                                target_identity["ti_external_id"] = ti["externalId"]
                            if "id" in ti:
                                target_identity["ti_id"] = ti["id"]
                            if "targetId" in ti:
                                target_identity["ti_target_id"] = ti["targetId"]
                            if "identity" in ti:
                                target_identity["ti_attributes"] = json.dumps(ti["identity"])
                                if "status" in ti["identity"]:
                                    target_identity["ti_identity_status"] = ti["identity"]["status"]
                                if "name" in ti["identity"]:
                                    target_identity["ti_identity_name"] = json.dumps(
                                        ti["identity"]["name"]
                                    )

                            identities_list.append(target_identity)
                    else:
                        identities_list.append(identity)

                else:
                    identities_list.append(identity)

            else:
                if self.get_operation_type() == "DELETE" and identity["id"] != "":
                    identities_list.append(identity)
                    return identities_list

                self.logger.info("Skipping event - orphaned target identities")

        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        return identities_list

    def clean_prepared_events(self, prepared_events_df):
        if "id" in prepared_events_df:
            filter_condition = prepared_events_df["id"] != ""
            prepared_events_df = prepared_events_df.loc[filter_condition]
        return prepared_events_df

    def transform_stream_message(self, message):
        transformed_identities = []
        if isinstance(self._access_message_value_data(message), list):
            for event in self._access_message_value_data(message):
                transformed_identities.extend(self.transform_raw_event(event))
        else:
            transformed_identities.extend(super().transform_stream_message(message))

        return transformed_identities


class IdentityCreateEventTransformer(IdentityEventTransformer):
    pass


class IdentityUpdateEventTransformer(IdentityEventTransformer):
    pass


class IdentityDeleteEventTransformer(IdentityEventTransformer):
    pass
