# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from dfa.adw.tables.base_table import BaseTable


class AuditEventsTable(BaseTable):
    _table_name = "audit_events"
    _schema = None

    def _column_definitions(self):
        json = """
[
    {"field_name":"SOURCE","column_name":"SOURCE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"AUDIT_EVENT_TYPE","column_name":"AUDIT_EVENT_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"AUDIT_EVENT_TYPE_VERSION","column_name":"AUDIT_EVENT_TYPE_VERSION","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"CONTENT_TYPE","column_name":"CONTENT_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"REGION","column_name":"REGION","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"AVAILABILITY_DOMAIN","column_name":"AVAILABILITY_DOMAIN","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"IDENTITY_HOST","column_name":"IDENTITY_HOST","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"IDENTITY_USER_AGENT","column_name":"IDENTITY_USER_AGENT","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"IDENTITY_PRINCIPAL_ID","column_name":"IDENTITY_PRINCIPAL_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"REQUEST_TIME","column_name":"REQUEST_TIME","column_expression":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
    {"field_name":"REQUEST_ID","column_name":"REQUEST_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"REQUEST_PATH","column_name":"REQUEST_PATH","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"REQUEST_ACTION","column_name":"REQUEST_ACTION","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"REQUEST_PARAMETERS","column_name":"REQUEST_PARAMETERS","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"REQUEST_HEADERS","column_name":"REQUEST_HEADERS","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"REQUEST_PAYLOAD","column_name":"REQUEST_PAYLOAD","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"RESPONSE_TIME","column_name":"RESPONSE_TIME","column_expression":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
    {"field_name":"RESPONSE_STATUS","column_name":"RESPONSE_STATUS","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"RESPONSE_HEADERS","column_name":"RESPONSE_HEADERS","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"RESPONSE_PAYLOAD","column_name":"RESPONSE_PAYLOAD","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"STATE_CHANGE","column_name":"STATE_CHANGE","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"EVENT_OBJECT_TYPE","column_name":"EVENT_OBJECT_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"OPERATION_TYPE","column_name":"OPERATION_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"EVENT_TIMESTAMP","column_name":"EVENT_TIMESTAMP","column_expression":"SYSTIMESTAMP","skip_column":false,"data_type":"TIMESTAMP WITH TIME ZONE","data_length":null,"data_format":"YYYY-MM-DD\\"T\\"HH24:MI:SS.FFTZH:TZM"},
    {"field_name":"ATTRIBUTES","column_name":"ATTRIBUTES","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"TENANCY_ID","column_name":"TENANCY_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null},
    {"field_name":"SERVICE_INSTANCE_ID","column_name":"SERVICE_INSTANCE_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":32767,"data_format":null}
]
"""

        return json
