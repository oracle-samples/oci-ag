# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from dfa.adw.tables.base_table import BaseStateTable, BaseTable


class CloudPolicyTimeSeriesTable(BaseTable):
    _table_name = "cloud_policy_ts"
    _schema = None

    def _column_definitions(self):
        json = """
[
    {"field_name":"ID","column_name":"ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"COMPARTMENT_ID","column_name":"COMPARTMENT_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"DESCRIPTION","column_name":"DESCRIPTION","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"EXTERNAL_ID","column_name":"EXTERNAL_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"LOCATION","column_name":"LOCATION","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"NAME","column_name":"NAME","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_STATEMENT_ID","column_name":"POLICY_STATEMENT_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"RESOURCE_TYPE","column_name":"RESOURCE_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"STATEMENT","column_name":"STATEMENT","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"SUBJECT_ID","column_name":"SUBJECT_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"SUBJECT_NAME","column_name":"SUBJECT_NAME","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"SUBJECT_TYPE","column_name":"SUBJECT_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"TARGET_ID","column_name":"TARGET_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"VERB","column_name":"VERB","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"EVENT_OBJECT_TYPE","column_name":"EVENT_OBJECT_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"OPERATION_TYPE","column_name":"OPERATION_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"EVENT_TIMESTAMP","column_name":"EVENT_TIMESTAMP","column_expression":"SYSTIMESTAMP","skip_column":false,"data_type":"TIMESTAMP WITH TIME ZONE","data_length":null,"data_format":"YYYY-MM-DD\\"T\\"HH24:MI:SS.FFTZH:TZM"},
    {"field_name":"ATTRIBUTES","column_name":"ATTRIBUTES","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"TENANCY_ID","column_name":"TENANCY_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"SERVICE_INSTANCE_ID","column_name":"SERVICE_INSTANCE_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null}
]
"""

        return json


class CloudPolicyStateTable(BaseStateTable, CloudPolicyTimeSeriesTable):
    _table_name = "cloud_policy_state"

    def get_unique_contraint_definition_details(self):
        return {
            "name": "DFA_UNQ_CP_ST_CONST",
            "columns": [
                "ID",
                "POLICY_STATEMENT_ID",
                "SERVICE_INSTANCE_ID",
                "TENANCY_ID",
            ],
        }
