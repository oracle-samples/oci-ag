# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from dfa.adw.tables.base_table import BaseStateTable, BaseTable


class CloudGroupTimeSeriesTable(BaseTable):
    _table_name = "cloud_group_ts"
    _schema = None

    def _column_definitions(self):
        json = """
[
    {"field_name":"ID","column_name":"ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"EXTERNAL_ID","column_name":"EXTERNAL_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"TARGET_ID","column_name":"TARGET_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"COMPARTMENT_ID","column_name":"COMPARTMENT_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"NAME","column_name":"NAME","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"DOMAIN_ID","column_name":"DOMAIN_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"IDENTITY_OPERATION_TYPE","column_name":"IDENTITY_OPERATION_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"IDENTITY_EXTERNAL_ID","column_name":"IDENTITY_EXTERNAL_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"IDENTITY_GLOBAL_ID","column_name":"IDENTITY_GLOBAL_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"IDENTITY_TARGET_IDENTITY_ID","column_name":"IDENTITY_TARGET_IDENTITY_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"GROUP_MEMBERSHIP_TYPE","column_name":"GROUP_MEMBERSHIP_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"EVENT_OBJECT_TYPE","column_name":"EVENT_OBJECT_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"OPERATION_TYPE","column_name":"OPERATION_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"EVENT_TIMESTAMP","column_name":"EVENT_TIMESTAMP","column_expression":"SYSTIMESTAMP","skip_column":false,"data_type":"TIMESTAMP WITH TIME ZONE","data_length":null,"data_format":"YYYY-MM-DD\\"T\\"HH24:MI:SS.FFTZH:TZM"},
    {"field_name":"ATTRIBUTES","column_name":"ATTRIBUTES","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"TENANCY_ID","column_name":"TENANCY_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"SERVICE_INSTANCE_ID","column_name":"SERVICE_INSTANCE_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null}
]
"""

        return json


class CloudGroupStateTable(BaseStateTable, CloudGroupTimeSeriesTable):
    _table_name = "cloud_group_state"

    def get_unique_contraint_definition_details(self):
        return {
            "name": "DFA_UNQ_CG_ST_CONST",
            "columns": [
                "ID",
                "IDENTITY_GLOBAL_ID",
                "IDENTITY_TARGET_IDENTITY_ID",
                "TARGET_ID",
                "SERVICE_INSTANCE_ID",
                "TENANCY_ID",
            ],
        }
