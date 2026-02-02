# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from dfa.adw.tables.base_table import BaseStateTable, BaseTable
from dfa.adw.connection import AdwConnection

class IdentityTimeSeriesTable(BaseTable):
    _table_name = "identity_ts"
    _schema = None

    def _column_definitions(self):
        json = """
[
    {"field_name":"ID","column_name":"ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"AG_STATUS","column_name":"AG_STATUS","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"AG_SUB_TYPE","column_name":"AG_SUB_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"DISPLAY_NAME","column_name":"DISPLAY_NAME","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"LOCATION","column_name":"LOCATION","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"RISK","column_name":"RISK","column_expression":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
    {"field_name":"AG_RISK_ATTRIBUTES","column_name":"AG_RISK_ATTRIBUTES","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"STATUS","column_name":"STATUS","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"USERNAME","column_name":"USERNAME","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"LAST_NAME","column_name":"LAST_NAME","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"FIRST_NAME","column_name":"FIRST_NAME","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"IDENTITY_ATTRIBUTES","column_name":"IDENTITY_ATTRIBUTES","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"TI_EXTERNAL_ID","column_name":"TI_EXTERNAL_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"TI_ID","column_name":"TI_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"TI_TARGET_ID","column_name":"TI_TARGET_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"TI_DOMAIN_ID","column_name":"TI_DOMAIN_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"TI_IDENTITY_NAME","column_name":"TI_IDENTITY_NAME","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"TI_IDENTITY_STATUS","column_name":"TI_IDENTITY_STATUS","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"TI_OPERATION_TYPE","column_name":"TI_OPERATION_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"TI_EVENT_TIMESTAMP","column_name":"TI_EVENT_TIMESTAMP","column_expression":"SYSTIMESTAMP","skip_column":false,"data_type":"TIMESTAMP WITH TIME ZONE","data_length":null,"data_format":"YYYY-MM-DD\\"T\\"HH24:MI:SS.FFTZH:TZM"},
    {"field_name":"TI_ATTRIBUTES","column_name":"TI_ATTRIBUTES","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"IDENTITY_TYPE","column_name":"IDENTITY_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"EVENT_OBJECT_TYPE","column_name":"EVENT_OBJECT_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"OPERATION_TYPE","column_name":"OPERATION_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"EVENT_TIMESTAMP","column_name":"EVENT_TIMESTAMP","column_expression":"SYSTIMESTAMP","skip_column":false,"data_type":"TIMESTAMP WITH TIME ZONE","data_length":null,"data_format":"YYYY-MM-DD\\"T\\"HH24:MI:SS.FFTZH:TZM"},
    {"field_name":"TENANCY_ID","column_name":"TENANCY_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"SERVICE_INSTANCE_ID","column_name":"SERVICE_INSTANCE_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null}
]
"""

        return json


class IdentityStateTable(BaseStateTable, IdentityTimeSeriesTable):
    _table_name = "identity_state"

    def get_unique_contraint_definition_details(self):
        return {
            "name": "DFA_UNQ_ID_ST_CONST",
            "columns": ["ID", "TI_ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"],
        }

    def _after_create(self):
        super()._after_create()
        self.logger.info("Generating DDL to add additional index to table %s", self.get_table_name())

        ti_id_index_ddl = f"""
            CREATE INDEX {self.get_schema()}.DFA_TI_ID_ST_CONST ON \
            {self.get_schema()}.{self.get_table_name()} (TI_ID, SERVICE_INSTANCE_ID, TENANCY_ID)
        """
        AdwConnection.get_cursor().execute(ti_id_index_ddl)
