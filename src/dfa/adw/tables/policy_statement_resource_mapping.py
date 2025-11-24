# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from dfa.adw.tables.base_table import BaseStateTable, BaseTable


class PolicyStatementResourceMappingTimeSeriesTable(BaseTable):
    _table_name = "policy_statement_resource_mapping_ts"
    _schema = None

    def _column_definitions(self):
        json = """
[
    {"field_name":"ID","column_name":"ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"COMPARTMENT_ID","column_name":"COMPARTMENT_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_EXTERNAL_ID","column_name":"POLICY_EXTERNAL_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"POLICY_STATEMENT_ID","column_name":"POLICY_STATEMENT_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"RESOURCE_ID","column_name":"RESOURCE_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"RESOURCE_EXTERNAL_ID","column_name":"RESOURCE_EXTERNAL_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"TARGET_ID","column_name":"TARGET_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"EVENT_OBJECT_TYPE","column_name":"EVENT_OBJECT_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"OPERATION_TYPE","column_name":"OPERATION_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"EVENT_TIMESTAMP","column_name":"EVENT_TIMESTAMP","column_expression":"SYSTIMESTAMP","skip_column":false,"data_type":"TIMESTAMP WITH TIME ZONE","data_length":null,"data_format":"YYYY-MM-DD\\"T\\"HH24:MI:SS.FFTZH:TZM"},
    {"field_name":"ATTRIBUTES","column_name":"ATTRIBUTES","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"TENANCY_ID","column_name":"TENANCY_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"SERVICE_INSTANCE_ID","column_name":"SERVICE_INSTANCE_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null}
]
"""
        return json


class PolicyStatementResourceMappingStateTable(
    BaseStateTable, PolicyStatementResourceMappingTimeSeriesTable
):
    _table_name = "policy_statement_resource_mapping_state"

    def get_unique_contraint_definition_details(self):
        return {
            "name": "DFA_UNQ_PSRM_ST_CONST",
            "columns": [
                "ID",
                "POLICY_STATEMENT_ID",
                "RESOURCE_ID",
                "SERVICE_INSTANCE_ID",
                "TENANCY_ID",
            ],
        }
