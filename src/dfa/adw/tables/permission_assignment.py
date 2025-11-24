# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from dfa.adw.tables.base_table import BaseStateTable, BaseTable


class PermissionAssignmentTimeSeriesTable(BaseTable):
    _table_name = "permission_assignment_ts"
    _schema = None

    def _column_definitions(self):
        json = """
[
        {"field_name":"TARGET_IDENTITY_ID","column_name":"TARGET_IDENTITY_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"GLOBAL_IDENTITY_ID","column_name":"GLOBAL_IDENTITY_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"IDENTITY_OPERATION_TYPE","column_name":"IDENTITY_OPERATION_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"ASSIGNMENT_ID","column_name":"ASSIGNMENT_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"EXTERNAL_ID","column_name":"EXTERNAL_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"TARGET_ID","column_name":"TARGET_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"TARGET_TYPE","column_name":"TARGET_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"GRANT_TYPE","column_name":"GRANT_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"PERMISSION_TYPE","column_name":"PERMISSION_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"PERMISSION_ID","column_name":"PERMISSION_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"PERMISSION_NAME","column_name":"PERMISSION_NAME","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"ACCESS_BUNDLE_ID","column_name":"ACCESS_BUNDLE_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"ACCESS_BUNDLE_NAME","column_name":"ACCESS_BUNDLE_NAME","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"ROLE_ID","column_name":"ROLE_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"ROLE_NAME","column_name":"ROLE_NAME","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"IDENTITY_GROUP_ID","column_name":"IDENTITY_GROUP_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"IDENTITY_GROUP_NAME","column_name":"IDENTITY_GROUP_NAME","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"RESOURCE_ID","column_name":"RESOURCE_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"RESOURCE_DISPLAY_NAME","column_name":"RESOURCE_DISPLAY_NAME","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"POLICY_ID","column_name":"POLICY_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"POLICY_NAME","column_name":"POLICY_NAME","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"POLICY_RULE_ID","column_name":"POLICY_RULE_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"USER_LOGIN","column_name":"USER_LOGIN","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"VALID_FROM","column_name":"VALID_FROM","column_expression":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
        {"field_name":"VALID_TO","column_name":"VALID_TO","column_expression":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
        {"field_name":"ASSIGNMENT_ATTRIBUTES","column_name":"ASSIGNMENT_ATTRIBUTES","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
        {"field_name":"EVENT_OBJECT_TYPE","column_name":"EVENT_OBJECT_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"OPERATION_TYPE","column_name":"OPERATION_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"EVENT_TIMESTAMP","column_name":"EVENT_TIMESTAMP","column_expression":"SYSTIMESTAMP","skip_column":false,"data_type":"TIMESTAMP WITH TIME ZONE","data_length":null,"data_format":"YYYY-MM-DD\\"T\\"HH24:MI:SS.FFTZH:TZM"},
        {"field_name":"ATTRIBUTES","column_name":"ATTRIBUTES","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
        {"field_name":"TENANCY_ID","column_name":"TENANCY_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"SERVICE_INSTANCE_ID","column_name":"SERVICE_INSTANCE_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null}
]
"""

        return json


class PermissionAssignmentStateTable(BaseStateTable, PermissionAssignmentTimeSeriesTable):
    _table_name = "permission_assignment_state"

    def get_unique_contraint_definition_details(self):
        return {
            "name": "DFA_UNQ_PA_ST_CONST",
            "columns": [
                "TARGET_IDENTITY_ID",
                "PERMISSION_ID",
                "ACCESS_BUNDLE_ID",
                "SERVICE_INSTANCE_ID",
                "TENANCY_ID",
            ],
        }
