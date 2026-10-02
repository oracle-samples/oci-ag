# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import base64
import io
import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from common.ocihelpers.stream import DataEnablementStream
from dfa.adw.query_builders.base_query_builder import _resolve_query_builder_class, get_query_builder
from dfa.adw.tables.system_events import SystemEventsTimeSeriesTable
from dfa.etl.event_transformer import EventTransformer
from dfa.etl.file_transformer import FileTransformer
from dfa.etl.stream_transformer import StreamTransformer
from handlers import audit_handler


@pytest.fixture
def envelope():
    # Sanitized shape from exportedSystemEvents.json: target updates are still
    # CREATE envelopes, and source timestamps carry nanosecond precision.
    return {
        "headers": {
            "eventType": "com.oracle.idm.agcs.data.enablement.systemEvents",
            "messageType": "SYSTEM_EVENTS",
            "operation": "CREATE",
            "eventTypeVersion": "1.0",
            "eventTime": "2026-09-23T16:24:10.664413064Z",
            "tenancyId": "test-tenancy",
            "serviceInstanceId": "test-service-instance",
        },
        "data": {
            "eventTimestamp": "2026-09-23T16:24:10.539873738Z",
            "serviceInstanceId": "test-service-instance",
            "sourceEventType": "com.oracle.idm.agcs.targetOperation.updated",
            "sourceEventId": "test-source-event",
            "sourceEventTypeVersion": "1.0",
            "sourceOrigin": "REST_API",
            "sourceAction": "UPDATE_TARGET_OPERATION",
            "sourceRequestId": "test-request",
            "data": {
                "id": "test-operation",
                "resourceType": "TARGET_OPERATION",
                "changes": ["status", "operationDetails"],
                "oldValue": {"status": "INPROGRESS", "inputData": []},
                "newValue": {
                    "status": "FAILED",
                    "operationDetails": [{"source": "ICF_GATEWAY", "eventTime": 1790180650397}],
                },
            },
        },
    }


@pytest.fixture(autouse=True)
def dependencies():
    # Dynamically loaded builders retain their imported connection class;
    # isolate them from tests that replace that class rather than its methods.
    _resolve_query_builder_class.cache_clear()
    with (
        patch("dfa.etl.stream_transformer.DataEnablementStream"),
        patch("dfa.etl.file_transformer.BaseObjectStorage"),
        patch("dfa.adw.connection.AdwConnection.get_cursor") as get_cursor,
        patch("dfa.adw.connection.AdwConnection.commit"),
        patch("dfa.adw.connection.AdwConnection.rollback_and_close"),
    ):
        get_cursor.return_value.getbatcherrors.return_value = []
        yield get_cursor.return_value
    _resolve_query_builder_class.cache_clear()


@pytest.mark.parametrize("as_object", [False, True])
@pytest.mark.parametrize("as_batch", [False, True])
def test_stream_preserves_source_fields_and_inserts_only_ts(envelope, dependencies, as_object, as_batch):
    original = deepcopy(envelope)
    if as_batch:
        created = deepcopy(envelope["data"])
        created["sourceEventId"] = "created-event"
        created["sourceEventType"] = "com.oracle.idm.agcs.targetOperation.created"
        created["sourceAction"] = "CREATE_TARGET_OPERATION"
        created["data"] = {"id": "test-operation", "value": {"status": "PENDING"}}
        envelope["data"] = [envelope["data"], created]
    message = SimpleNamespace(value=envelope) if as_object else {"value": envelope}
    messages = (
        {"SYSTEM_EVENTS": {"CREATE": [message]}}
        if as_object
        else DataEnablementStream.sort_connector_hub_source_stream_messages([message])
    )
    transformer = EventTransformer()
    transformer.transform_messages(messages)
    rows = transformer.get_prepared_events()
    assert len(rows) == (2 if as_batch else 1)
    row = rows[0]
    assert row["tenancy_id"] == "test-tenancy"
    assert row["service_instance_id"] == "test-service-instance"
    assert row["event_timestamp"] == "23-Sep-26 04:24:10.664413 PM"
    assert row["source_event_timestamp"] == "2026-09-23T16:24:10.539873738Z"
    assert row["source_event_id"] == "test-source-event"
    assert row["source_event_type"] == original["data"]["sourceEventType"]
    assert row["source_event_type_version"] == "1.0"
    assert row["source_origin"] == "REST_API"
    assert row["source_action"] == "UPDATE_TARGET_OPERATION"
    assert row["source_request_id"] == "test-request"
    assert row["event_object_type"] == "SYSTEM_EVENTS"
    assert row["operation_type"] == "CREATE"
    assert json.loads(row["data"]) == original["data"]["data"]
    if as_batch:
        assert json.loads(rows[1]["data"]) == created["data"]

    transformer.load_data()
    sql, bindings = dependencies.executemany.call_args.args
    assert 'INSERT INTO "SYSTEM_EVENTS"' in sql
    assert "UPDATE" not in sql
    assert len(bindings) == len(rows)
    assert bindings[0]["SOURCE_EVENT_TIMESTAMP"] == row["source_event_timestamp"]
    assert json.loads(bindings[0]["DATA"]) == original["data"]["data"]


@pytest.mark.parametrize("file_format", ["json", "encoded_json", "jsonl"])
@pytest.mark.parametrize("is_timeseries", [False, True])
def test_file_routing_and_payload(envelope, dependencies, file_format, is_timeseries):
    transformer = FileTransformer(
        "namespace", "bucket", "events.jsonl" if file_format == "jsonl" else "events.json", is_timeseries
    )
    if file_format == "jsonl":
        content = json.dumps({"headers": envelope["headers"]}) + "\n" + json.dumps(envelope["data"])
    else:
        payload = deepcopy(envelope)
        if file_format == "encoded_json":
            payload["data"] = json.dumps([payload["data"]])
        content = json.dumps(payload)
    response = MagicMock()
    response.data.content = content.encode()
    transformer._object_storage_client.download.return_value = response
    transformer.extract_data()
    transformer.transform_data()
    transformer.load_data()
    assert transformer.get_prepared_events() == []
    dependencies.executemany.assert_not_called()
    dependencies.execute.assert_not_called()


@pytest.mark.parametrize("is_timeseries", [False, True])
def test_generic_stream_skips_system_events(envelope, dependencies, is_timeseries):
    transformer = StreamTransformer(is_timeseries=is_timeseries)
    transformer.transform_messages({"SYSTEM_EVENTS": {"CREATE": [{"value": envelope}]}})
    transformer.load_data()
    assert transformer.get_prepared_events() == []
    dependencies.executemany.assert_not_called()
    assert get_query_builder("SYSTEM_EVENTS", "CREATE", [], False) is None


def test_unsupported_version_is_skipped(envelope, dependencies):
    envelope["headers"]["eventTypeVersion"] = "2.0"
    transformer = EventTransformer()
    transformer.transform_messages({"SYSTEM_EVENTS": {"CREATE": [{"value": envelope}]}})
    transformer.load_data()
    assert transformer.get_prepared_events() == []
    dependencies.executemany.assert_not_called()


def test_empty_insert_and_large_open_payload(dependencies):
    builder = get_query_builder("SYSTEM_EVENTS", "CREATE", [], True)
    builder.execute_sql_for_events()
    dependencies.executemany.assert_not_called()
    row = SystemEventsTimeSeriesTable().get_default_row()
    row["data"] = json.dumps({"futureField": "x" * 40000})
    builder.events = [row]
    builder.execute_sql_for_events()
    assert dependencies.executemany.call_args.args[1][0]["DATA"] == row["data"]
    columns = SystemEventsTimeSeriesTable().get_column_list_definition_for_table_ddl()
    assert next(c for c in columns if c["column_name"] == "DATA")["data_type"] == "CLOB"


def test_audit_handler_loads_mixed_batch_into_separate_tables(envelope, dependencies):
    audit = deepcopy(envelope)
    audit["headers"]["messageType"] = "AUDIT_EVENTS"
    audit["data"] = {"source": "test-audit", "eventType": "com.oracle.test.audit"}
    messages = []
    for event in (envelope, audit):
        event = deepcopy(event)
        event["data"] = json.dumps(event["data"])
        messages.append({"value": base64.b64encode(base64.b64encode(json.dumps(event).encode())).decode()})

    with patch("handlers.audit_handler.bootstrap_base_environment_variables"):
        audit_handler.handler(MagicMock(), io.BytesIO(json.dumps(messages).encode()))

    assert dependencies.executemany.call_count == 2
    inserts = {call.args[0].split('"')[1]: call.args[1] for call in dependencies.executemany.call_args_list}
    assert set(inserts) == {"AUDIT_EVENTS", "SYSTEM_EVENTS"}
    assert len(inserts["AUDIT_EVENTS"]) == len(inserts["SYSTEM_EVENTS"]) == 1
    assert inserts["AUDIT_EVENTS"][0]["SOURCE"] == "test-audit"
    assert inserts["SYSTEM_EVENTS"][0]["SOURCE_EVENT_ID"] == "test-source-event"
    assert json.loads(inserts["SYSTEM_EVENTS"][0]["DATA"]) == envelope["data"]["data"]


def test_event_transformer_only_accepts_system_events_in_timeseries_mode():
    assert EventTransformer().is_valid_object_type("SYSTEM_EVENTS")
    assert not EventTransformer(is_timeseries=False).is_valid_object_type("SYSTEM_EVENTS")
