# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from dfa.adw.tables.base_table import BaseStateTable, BaseTable


class OrchestratedSystemTimeSeriesTable(BaseTable):
    _table_name = "orchestrated_system_ts"
    _schema = None

    def _column_definitions(self):
        json = """
[
    {"field_name":"ID","column_name":"ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"NAME","column_name":"NAME","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"TYPE","column_name":"TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"STATE","column_name":"STATE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"CREATED_BY","column_name":"CREATED_BY","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"CREATED_ON","column_name":"CREATED_ON","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
    {"field_name":"UPDATED_ON","column_name":"UPDATED_ON","column_expression":null,"column_expression_type":null,"source_path":null,"source_column":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
    {"field_name":"TARGET_MODE","column_name":"TARGET_MODE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"OWNERSHIP_COLLECTION_ID","column_name":"OWNERSHIP_COLLECTION_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"PRIMARY_OWNER","column_name":"PRIMARY_OWNER","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"TENANCY_ID","column_name":"TENANCY_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"EVENT_OBJECT_TYPE","column_name":"EVENT_OBJECT_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"OPERATION_TYPE","column_name":"OPERATION_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"EVENT_TIMESTAMP","column_name":"EVENT_TIMESTAMP","column_expression":"SYSTIMESTAMP","skip_column":false,"data_type":"TIMESTAMP WITH TIME ZONE","data_length":null,"data_format":"YYYY-MM-DD\\"T\\"HH24:MI:SS.FFTZH:TZM"},
    {"field_name":"ATTRIBUTES","column_name":"ATTRIBUTES","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"SERVICE_INSTANCE_ID","column_name":"SERVICE_INSTANCE_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null}
]
"""

        return json


class OrchestratedSystemStateTable(BaseStateTable, OrchestratedSystemTimeSeriesTable):
    _table_name = "orchestrated_system_state"

    def get_unique_contraint_definition_details(self):
        return {
            "name": "DFA_UNQ_OS_ST_CONST",
            "columns": ["ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"],
        }
