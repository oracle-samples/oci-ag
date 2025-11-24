# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from dfa.adw.tables.base_table import BaseStateTable, BaseTable


class PolicyTimeSeriesTable(BaseTable):
    _table_name = "policy_ts"
    _schema = None

    def _column_definitions(self):
        json = """
[
    {"field_name":"ID","column_name":"ID","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"NAME","column_name":"NAME","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"EXTERNAL_ID","column_name":"EXTERNAL_ID","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"DESCRIPTION","column_name":"DESCRIPTION","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"DISPLAY_NAME","column_name":"DISPLAY_NAME","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"STATUS","column_name":"STATUS","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"IS_TRANSFORMED_POLICY","column_name":"IS_TRANSFORMED_POLICY","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"CONSTRAINTS","column_name":"CONSTRAINTS","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"TAGS","column_name":"TAGS","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_TYPE","column_name":"POLICY_TYPE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_VERSION","column_name":"POLICY_VERSION","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"TARGET_ID","column_name":"TARGET_ID","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"TARGET_POLICY_ID","column_name":"TARGET_POLICY_ID","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_RULE_ID","column_name":"POLICY_RULE_ID","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_RULE_ASSIGNMENT_ID","column_name":"POLICY_RULE_ASSIGNMENT_ID","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_RULE_IDENTITY_GROUP_ID","column_name":"POLICY_RULE_IDENTITY_GROUP_ID","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_RULE_PARSED_ON","column_name":"POLICY_RULE_PARSED_ON","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
    {"field_name":"POLICY_RULE_VERSION","column_name":"POLICY_RULE_VERSION","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_RULE_ACTION","column_name":"POLICY_RULE_ACTION","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_RULE_STATEMENT","column_name":"POLICY_RULE_STATEMENT","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_RULE_STATUS","column_name":"POLICY_RULE_STATUS","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_RULE_TYPE","column_name":"POLICY_RULE_TYPE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_RULE_CREATED_BY","column_name":"POLICY_RULE_CREATED_BY","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_RULE_CREATED_ON","column_name":"POLICY_RULE_CREATED_ON","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
    {"field_name":"POLICY_RULE_UPDATED_BY","column_name":"POLICY_RULE_UPDATED_BY","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_RULE_UPDATED_ON","column_name":"POLICY_RULE_UPDATED_ON","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
    {"field_name":"CREATED_BY","column_name":"CREATED_BY","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"CREATED_BY_DISPLAY_NAME","column_name":"CREATED_BY_DISPLAY_NAME","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"CREATED_BY_RESOURCE_TYPE","column_name":"CREATED_BY_RESOURCE_TYPE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"CREATED_BY_VALUE","column_name":"CREATED_BY_VALUE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"CREATED_ON","column_name":"CREATED_ON","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
    {"field_name":"UPDATED_BY","column_name":"UPDATED_BY","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"UPDATED_BY_DISPLAY_NAME","column_name":"UPDATED_BY_DISPLAY_NAME","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"UPDATED_BY_RESOURCE_TYPE","column_name":"UPDATED_BY_RESOURCE_TYPE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"UPDATED_BY_VALUE","column_name":"UPDATED_BY_VALUE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"UPDATED_ON","column_name":"UPDATED_ON","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
    {"field_name":"AG_RISK","column_name":"AG_RISK","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
    {"field_name":"AG_MANAGED","column_name":"AG_MANAGED","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"OWNER_DISPLAY_NAME","column_name":"OWNER_DISPLAY_NAME","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"OWNER_VALUE","column_name":"OWNER_VALUE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"OWNERSHIP_COLLECTION_ID","column_name":"OWNERSHIP_COLLECTION_ID","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"MANAGED_BY_IDS","column_name":"MANAGED_BY_IDS","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"OWNER_UIDS","column_name":"OWNER_UIDS","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"EVENT_OBJECT_TYPE","column_name":"EVENT_OBJECT_TYPE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"OPERATION_TYPE","column_name":"OPERATION_TYPE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"EVENT_TIMESTAMP","column_name":"EVENT_TIMESTAMP","column_expression":"SYSTIMESTAMP","skip_column":false,"data_type":"TIMESTAMP WITH TIME ZONE","data_length":null,"data_format":"YYYY-MM-DD\\"T\\"HH24:MI:SS.FFTZH:TZM"},
    {"field_name":"ATTRIBUTES","column_name":"ATTRIBUTES","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"TENANCY_ID","column_name":"TENANCY_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"SERVICE_INSTANCE_ID","column_name":"SERVICE_INSTANCE_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null}
]
"""

        return json


class PolicyStateTable(BaseStateTable, PolicyTimeSeriesTable):
    _table_name = "policy_state"

    def get_unique_contraint_definition_details(self):
        return {
            "name": "DFA_UNQ_POL_ST_CONST",
            "columns": ["ID", "POLICY_RULE_ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"],
        }
