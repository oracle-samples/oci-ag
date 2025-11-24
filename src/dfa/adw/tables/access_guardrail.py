# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from dfa.adw.tables.base_table import BaseStateTable, BaseTable


class AccessGuardrailTimeSeriesTable(BaseTable):
    _table_name = "access_guardrail_ts"
    _schema = None

    def _column_definitions(self):
        json = """
[
        {"field_name":"ID","column_name":"ID","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"EXTERNAL_ID","column_name":"EXTERNAL_ID","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"NAME","column_name":"NAME","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"DESCRIPTION","column_name":"DESCRIPTION","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"ACTION_ON_FAILURE_ACTION_TYPE","column_name":"ACTION_ON_FAILURE_ACTION_TYPE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"ACTION_ON_FAILURE_REVOKE_AFTER_NUMBER_OF_DAYS","column_name":"ACTION_ON_FAILURE_REVOKE_AFTER_NUMBER_OF_DAYS","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"INTEGER","data_length":null,"data_format":null},
        {"field_name":"ACTION_ON_FAILURE_RISK","column_name":"ACTION_ON_FAILURE_RISK","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"ACTION_ON_FAILURE_SHOULD_USER_MANAGER_BE_NOTIFIED","column_name":"ACTION_ON_FAILURE_SHOULD_USER_MANAGER_BE_NOTIFIED","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"CREATED_BY_DISPLAY_NAME","column_name":"CREATED_BY_DISPLAY_NAME","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"CREATED_BY_RESOURCE_TYPE","column_name":"CREATED_BY_RESOURCE_TYPE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"CREATED_BY_VALUE","column_name":"CREATED_BY_VALUE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"CREATED_ON","column_name":"CREATED_ON","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
        {"field_name":"ETAG","column_name":"ETAG","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"IS_DETECTIVE_VIOLATION_CHECK_ENABLED","column_name":"IS_DETECTIVE_VIOLATION_CHECK_ENABLED","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"LIFECYCLE_STATE","column_name":"LIFECYCLE_STATE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"OWNER_DISPLAY_NAME","column_name":"OWNER_DISPLAY_NAME","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"OWNER_VALUE","column_name":"OWNER_VALUE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"OWNERSHIP_COLLECTION_ID","column_name":"OWNERSHIP_COLLECTION_ID","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"RULES","column_name":"RULES","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
        {"field_name":"TAGS","column_name":"TAGS","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"UPDATED_BY_DISPLAY_NAME","column_name":"UPDATED_BY_DISPLAY_NAME","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"UPDATED_BY_RESOURCE_TYPE","column_name":"UPDATED_BY_RESOURCE_TYPE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"UPDATED_BY_VALUE","column_name":"UPDATED_BY_VALUE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"UPDATED_ON","column_name":"UPDATED_ON","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
        {"field_name":"EVENT_OBJECT_TYPE","column_name":"EVENT_OBJECT_TYPE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"OPERATION_TYPE","column_name":"OPERATION_TYPE","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"EVENT_TIMESTAMP","column_name":"EVENT_TIMESTAMP","column_expression":"SYSTIMESTAMP","skip_column":false,"data_type":"TIMESTAMP WITH TIME ZONE","data_length":null,"data_format":"YYYY-MM-DD\\"T\\"HH24:MI:SS.FFTZH:TZM"},
        {"field_name":"ATTRIBUTES","column_name":"ATTRIBUTES","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
        {"field_name":"TENANCY_ID","column_name":"TENANCY_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
        {"field_name":"SERVICE_INSTANCE_ID","column_name":"SERVICE_INSTANCE_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null}
]
"""
        return json


class AccessGuardrailStateTable(BaseStateTable, AccessGuardrailTimeSeriesTable):
    _table_name = "access_guardrail_state"

    def get_unique_contraint_definition_details(self):
        return {
            "name": "DFA_UNQ_AGR_ST_CONST",
            "columns": [
                "ID",
                "SERVICE_INSTANCE_ID",
                "TENANCY_ID",
            ],
        }
