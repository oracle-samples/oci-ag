# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.etl.transformers.base_event_transformer import BaseEventTransformer
from dfa.adw.tables.permission_assignment import PermissionAssignmentStateTable

class PermissionAssignmentEventTransformer(BaseEventTransformer):
    def __process_permission_assignments(self, identities_list, operation_type, base_pa, pa_list):
        for i in identities_list:
            pa_copy = base_pa.copy()
            pa_copy["identity_operation_type"] = operation_type
            if "id" in i:
                pa_copy["assignment_id"] = i["id"]
            if "externalId" in i:
                pa_copy["external_id"] = i["externalId"]
            if "targetId" in i:
                pa_copy["target_id"] = i["targetId"]
            if "targetType" in i:
                pa_copy["target_type"] = i["targetType"]
            if "granttype" in i:
                pa_copy["grant_type"] = i["granttype"]
            if "permissionType" in i:
                pa_copy["permission_type"] = i["permissionType"]
            if "permissionId" in i:
                pa_copy["permission_id"] = i["permissionId"]
            if "permissionName" in i:
                pa_copy["permission_name"] = i["permissionName"]
            if "accessBundleId" in i:
                pa_copy["access_bundle_id"] = i["accessBundleId"]
            if "accessBundleName" in i:
                pa_copy["access_bundle_name"] = i["accessBundleName"]
            if "roleId" in i:
                pa_copy["role_id"] = i["roleId"]
            if "roleName" in i:
                pa_copy["role_name"] = i["roleName"]
            if "identityGroupId" in i:
                pa_copy["identity_group_id"] = i["identityGroupId"]
            if "identityGroupName" in i:
                pa_copy["identity_group_name"] = i["identityGroupName"]
            if "resourceId" in i:
                pa_copy["resource_id"] = i["resourceId"]
            if "resourceDisplayName" in i:
                pa_copy["resource_display_name"] = i["resourceDisplayName"]
            if "policyId" in i:
                pa_copy["policy_id"] = i["policyId"]
            if "policyName" in i:
                pa_copy["policy_name"] = i["policyName"]
            if "policyRuleId" in i:
                pa_copy["policy_rule_id"] = i["policyRuleId"]
            if "userLogin" in i:
                pa_copy["user_login"] = i["userLogin"]
            if "validFrom" in i:
                pa_copy["valid_from"] = i["validFrom"]
            if "validTo" in i:
                pa_copy["valid_to"] = i["validTo"]
            if "customAttributes" in i:
                pa_copy["assignment_attributes"] = json.dumps(i["customAttributes"])

            pa_list.append(pa_copy)

    def transform_raw_event(self, raw_event):
        base_pa = PermissionAssignmentStateTable().get_default_row()
        pa_list = []

        try:
            if self._get_tenancy_id():
                base_pa["tenancy_id"] = self._get_tenancy_id()

            if self._get_service_instance_id():
                base_pa["service_instance_id"] = self._get_service_instance_id()

            if self._get_event_timestamp():
                base_pa["event_timestamp"] = self._get_event_timestamp()

            if "targetIdentityId" in raw_event:
                base_pa["target_identity_id"] = raw_event["targetIdentityId"]

            if "globalIdentityId" in raw_event:
                base_pa["global_identity_id"] = raw_event["globalIdentityId"]

            if "additionalProperties" in raw_event:
                base_pa["attributes"] = json.dumps(raw_event["additionalProperties"])

            base_pa["operation_type"] = self.get_operation_type()
            base_pa["event_object_type"] = self.get_event_object_type()

            if self.get_operation_type() == "DELETE":
                ids_list = []
                if "ids" in raw_event:
                    ids_list = raw_event["ids"]
                    for i in ids_list:
                        pa_copy = base_pa.copy()
                        pa_copy["permission_id"] = i
                        pa_list.append(pa_copy)
                if not ids_list:
                    pa_list.append(base_pa)

            else:
                add_identities = []
                remove_identities = []

                if "add" in raw_event:
                    add_identities = raw_event["add"]
                    self.__process_permission_assignments(add_identities, "add", base_pa, pa_list)

                if "remove" in raw_event:
                    remove_identities = raw_event["remove"]
                    self.__process_permission_assignments(
                        remove_identities, "remove", base_pa, pa_list
                    )

                if not add_identities and not remove_identities:
                    pa_list.append(base_pa)

        except KeyError as e:
            self.logger.error(
                "Cannot process event due to KeyError - %s is missing from event data", e
            )

        return pa_list

    def transform_stream_message(self, message):
        transformed_pa = []
        if isinstance(self._access_message_value_data(message), list):
            for event in self._access_message_value_data(message):
                transformed_pa.extend(self.transform_raw_event(event))
        else:
            transformed_pa.extend(super().transform_stream_message(message))

        return transformed_pa


class PermissionAssignmentCreateEventTransformer(PermissionAssignmentEventTransformer):
    pass


class PermissionAssignmentUpdateEventTransformer(PermissionAssignmentEventTransformer):
    pass


class PermissionAssignmentDeleteEventTransformer(PermissionAssignmentEventTransformer):
    pass
