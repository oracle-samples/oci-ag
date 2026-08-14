# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json

from dfa.adw.tables.permission_assignment import PermissionAssignmentStateTable
from dfa.etl.transformers.base_event_transformer import BaseEventTransformer


class PermissionAssignmentEventTransformer(BaseEventTransformer):

    def __process_permission_assignments(
        self,
        assignment_list,
        base_pa,
        pa_list,
        identity_operation_type=None,
        created_field=None,
        updated_field=None,
    ):
        for i in assignment_list:
            if not isinstance(i, dict):
                self.logger.warning("Skipping invalid permission assignment row")
                continue
            if not self.is_timeseries() and i.get("id") in (None, ""):
                self.logger.warning("Skipping permission assignment state row without an id")
                continue
            pa_copy = base_pa.copy()
            if identity_operation_type:
                pa_copy["identity_operation_type"] = identity_operation_type
            if "id" in i:
                pa_copy["assignment_id"] = i["id"]
            if i.get("targetIdentityId") not in (None, ""):
                pa_copy["target_identity_id"] = i["targetIdentityId"]
            if i.get("globalIdentityId") not in (None, ""):
                pa_copy["global_identity_id"] = i["globalIdentityId"]
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
            if "validFrom" in i and i["validFrom"] not in (0, "0"):
                pa_copy["valid_from"] = i["validFrom"]
            if "validTo" in i and i["validTo"] not in (0, "0"):
                pa_copy["valid_to"] = i["validTo"]
            if created_field and created_field in i and i[created_field] not in (0, "0"):
                pa_copy["created_on"] = i[created_field]
            if updated_field and updated_field in i and i[updated_field] not in (0, "0"):
                pa_copy["updated_on"] = i[updated_field]
            if "status" in i:
                pa_copy["status"] = i["status"]
            if "accountStatus" in i:
                pa_copy["account_status"] = i["accountStatus"]
            if "customAttributes" in i:
                pa_copy["assignment_attributes"] = json.dumps(i["customAttributes"])
            if "additionalProperties" in i:
                pa_copy["assignment_attributes"] = json.dumps(i["additionalProperties"])

            pa_list.append(pa_copy)

    def _get_base_permission_assignment(self, raw_event=None):
        base_pa = PermissionAssignmentStateTable().get_default_row()
        if self._get_tenancy_id():
            base_pa["tenancy_id"] = self._get_tenancy_id()

        if self._get_service_instance_id():
            base_pa["service_instance_id"] = self._get_service_instance_id()

        if self._get_event_timestamp():
            base_pa["event_timestamp"] = self._get_event_timestamp()

        if isinstance(raw_event, dict):
            if "targetIdentityId" in raw_event:
                base_pa["target_identity_id"] = raw_event["targetIdentityId"]
            if "globalIdentityId" in raw_event:
                base_pa["global_identity_id"] = raw_event["globalIdentityId"]
            if "additionalProperties" in raw_event:
                base_pa["assignment_attributes"] = json.dumps(raw_event["additionalProperties"])

        base_pa["operation_type"] = self.get_operation_type()
        base_pa["event_object_type"] = self.get_event_object_type()

        return base_pa

    def _transform_v1_event(self, raw_event):
        base_pa = self._get_base_permission_assignment(raw_event)
        pa_list = []

        if self.get_operation_type() == "DELETE":
            permission_ids = raw_event.get("ids", [])
            for permission_id in permission_ids:
                pa_copy = base_pa.copy()
                pa_copy["permission_id"] = permission_id
                pa_list.append(pa_copy)
            if not permission_ids:
                pa_list.append(base_pa)
            return pa_list

        add_assignments = raw_event.get("add", [])
        remove_assignments = raw_event.get("remove", [])
        self.__process_permission_assignments(
            add_assignments,
            base_pa,
            pa_list,
            identity_operation_type="add",
        )
        self.__process_permission_assignments(
            remove_assignments,
            base_pa,
            pa_list,
            identity_operation_type="remove",
        )
        return pa_list

    def _transform_v2_assignment(self, assignment):
        base_pa = self._get_base_permission_assignment()
        pa_list = []
        identity_operation_type = "remove" if self.get_operation_type() == "DELETE" else "add"
        self.__process_permission_assignments(
            [assignment],
            base_pa,
            pa_list,
            identity_operation_type=identity_operation_type,
            created_field="created",
            updated_field="lastModified",
        )
        return pa_list

    def _get_v2_assignments(self, raw_event):
        if isinstance(raw_event, list):
            return raw_event
        if isinstance(raw_event, dict):
            return [raw_event]
        if isinstance(raw_event, str):
            try:
                parsed_event = json.loads(raw_event)
                if isinstance(parsed_event, list):
                    return parsed_event
                if isinstance(parsed_event, dict):
                    return [parsed_event]
            except json.JSONDecodeError:
                self.logger.error("Failed to parse raw event as JSON: %s", raw_event)
        return []

    def transform_raw_event(self, raw_event):
        if self.get_event_type_version() == "1.0":
            if isinstance(raw_event, list):
                transformed_pa = []
                for event in raw_event:
                    transformed_pa.extend(self._transform_v1_event(event))
                return transformed_pa
            return self._transform_v1_event(raw_event)

        transformed_pa = []
        for assignment in self._get_v2_assignments(raw_event):
            transformed_pa.extend(self._transform_v2_assignment(assignment))
        return transformed_pa

    def transform_stream_message(self, message):
        transformed_pa = []
        message_data = self._access_message_value_data(message)
        if isinstance(message_data, list):
            for event in message_data:
                transformed_pa.extend(self.transform_raw_event(event))
        else:
            transformed_pa.extend(self.transform_raw_event(message_data))

        return transformed_pa


class PermissionAssignmentCreateEventTransformer(PermissionAssignmentEventTransformer):
    pass


class PermissionAssignmentUpdateEventTransformer(PermissionAssignmentEventTransformer):
    pass


class PermissionAssignmentDeleteEventTransformer(PermissionAssignmentEventTransformer):
    pass
