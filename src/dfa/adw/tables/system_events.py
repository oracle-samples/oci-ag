# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from dfa.adw.tables.base_table import BaseTable


class SystemEventsTimeSeriesTable(BaseTable):
    _table_name = "system_events"
    _schema = None

    def get_index_definition_details(self):
        definitions = super().get_index_definition_details()
        for definition in definitions:
            definition["expressions"] = {"EVENT_TIMESTAMP": 'SYS_EXTRACT_UTC("EVENT_TIMESTAMP")'}
        return definitions

    def _column_definitions(self):
        return r"""
[
    {"field_name":"SOURCE_EVENT_TIMESTAMP","column_name":"SOURCE_EVENT_TIMESTAMP","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":4000,"data_format":null},
    {"field_name":"SOURCE_EVENT_TYPE","column_name":"SOURCE_EVENT_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":4000,"data_format":null},
    {"field_name":"SOURCE_EVENT_ID","column_name":"SOURCE_EVENT_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":4000,"data_format":null},
    {"field_name":"SOURCE_EVENT_TYPE_VERSION","column_name":"SOURCE_EVENT_TYPE_VERSION","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":4000,"data_format":null},
    {"field_name":"SOURCE_ORIGIN","column_name":"SOURCE_ORIGIN","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":4000,"data_format":null},
    {"field_name":"SOURCE_ACTION","column_name":"SOURCE_ACTION","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":4000,"data_format":null},
    {"field_name":"SOURCE_REQUEST_ID","column_name":"SOURCE_REQUEST_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":4000,"data_format":null},
    {"field_name":"DATA","column_name":"DATA","column_expression":null,"skip_column":false,"data_type":"CLOB","data_length":null,"data_format":null},
    {"field_name":"TENANCY_ID","column_name":"TENANCY_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":4000,"data_format":null},
    {"field_name":"SERVICE_INSTANCE_ID","column_name":"SERVICE_INSTANCE_ID","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":4000,"data_format":null},
    {"field_name":"EVENT_OBJECT_TYPE","column_name":"EVENT_OBJECT_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":4000,"data_format":null},
    {"field_name":"OPERATION_TYPE","column_name":"OPERATION_TYPE","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":4000,"data_format":null},
    {"field_name":"EVENT_TIMESTAMP","column_name":"EVENT_TIMESTAMP","column_expression":"SYSTIMESTAMP","skip_column":false,"data_type":"TIMESTAMP WITH TIME ZONE","data_length":null,"data_format":"YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM"}
]
"""
