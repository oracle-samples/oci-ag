# Unit tests to cover handler modules without hitting external services.

import io
import json
import types

import pytest

# Handlers under test
import handlers.audit_handler as audit_handler
import handlers.file_handler as file_handler
import handlers.file_to_timeseries_handler as file_ts_handler
import handlers.stream_handler as stream_handler
import handlers.stream_to_timeseries_handler as stream_ts_handler


class FakeCtx:
    def __init__(self, config: dict[str, str]):
        self._cfg = config

    def Config(self):
        # Emulate Fn context Config().get(...)
        return types.SimpleNamespace(get=self._cfg.get)


def test_audit_handler_happy_path(monkeypatch):
    # No-op bootstrap
    monkeypatch.setattr(audit_handler, "bootstrap_base_environment_variables", lambda cfg: None)

    # Stub decode/sort to be pure functions
    class DummyStream:
        @classmethod
        def decode_connector_hub_source_stream_messages(cls, messages):
            return messages

        @classmethod
        def sort_connector_hub_source_stream_messages(cls, messages):
            # Return as-is
            return messages

    monkeypatch.setattr(audit_handler, "DataEnablementStream", DummyStream)

    # Stub transformer to avoid DB
    class DummyTransformer:
        def transform_messages(self, messages):
            return None

        def load_data(self):
            return None

    monkeypatch.setattr(audit_handler, "AuditTransformer", DummyTransformer)

    body = [{"value": json.dumps({"headers": {"messageType": "AUDIT", "operation": "CREATE"}})}]
    data = io.BytesIO(json.dumps(body).encode("utf-8"))
    ctx = FakeCtx({})
    # Expect no exception
    audit_handler.handler(ctx, data)


def test_stream_handler_happy_path(monkeypatch):
    monkeypatch.setattr(stream_handler, "bootstrap_base_environment_variables", lambda cfg: None)

    class DummyStream:
        @classmethod
        def decode_connector_hub_source_stream_messages(cls, messages):
            return messages

        @classmethod
        def sort_connector_hub_source_stream_messages(cls, messages):
            return messages

    monkeypatch.setattr(stream_handler, "DataEnablementStream", DummyStream)

    class DummyTransformer:
        def transform_messages(self, messages):
            return None

        def load_data(self):
            return None

    monkeypatch.setattr(stream_handler, "StreamTransformer", DummyTransformer)

    body = [{"value": json.dumps({"headers": {"messageType": "STREAM", "operation": "CREATE"}})}]
    data = io.BytesIO(json.dumps(body).encode("utf-8"))
    ctx = FakeCtx({})
    stream_handler.handler(ctx, data)


def test_stream_to_timeseries_handler_happy_path(monkeypatch):
    monkeypatch.setattr(stream_ts_handler, "bootstrap_base_environment_variables", lambda cfg: None)

    class DummyStream:
        @classmethod
        def decode_connector_hub_source_stream_messages(cls, messages):
            return messages

        @classmethod
        def sort_connector_hub_source_stream_messages(cls, messages):
            return messages

    monkeypatch.setattr(stream_ts_handler, "DataEnablementStream", DummyStream)

    class DummyTransformer:
        def __init__(self, is_timeseries=False):
            self.is_timeseries = is_timeseries

        def transform_messages(self, messages):
            return None

        def load_data(self):
            return None

    monkeypatch.setattr(stream_ts_handler, "StreamTransformer", DummyTransformer)

    body = [{"value": json.dumps({"headers": {"messageType": "STREAM", "operation": "CREATE"}})}]
    data = io.BytesIO(json.dumps(body).encode("utf-8"))
    ctx = FakeCtx({})
    stream_ts_handler.handler(ctx, data)


def test_file_handler_happy_path(monkeypatch):
    monkeypatch.setattr(file_handler, "bootstrap_base_environment_variables", lambda cfg: None)

    class DummyFileTransformer:
        def __init__(self, namespace, bucket, object_name, is_timeseries: bool = False):
            self.namespace = namespace
            self.bucket = bucket
            self.object_name = object_name
            self.is_timeseries = is_timeseries

        def extract_data(self):
            return None

        def transform_data(self):
            return None

        def load_data(self):
            return None

    monkeypatch.setattr(file_handler, "FileTransformer", DummyFileTransformer)

    body = {
        "data": {
            "resourceName": "obj.jsonl",
            "additionalDetails": {"bucketName": "b", "namespace": "ns"},
        }
    }
    data = io.BytesIO(json.dumps(body).encode("utf-8"))
    ctx = FakeCtx({})
    file_handler.handler(ctx, data)


def test_file_to_timeseries_handler_happy_path(monkeypatch):
    monkeypatch.setattr(file_ts_handler, "bootstrap_base_environment_variables", lambda cfg: None)

    class DummyFileTransformer:
        def __init__(self, namespace, bucket, object_name, is_timeseries: bool = False):
            self.namespace = namespace
            self.bucket = bucket
            self.object_name = object_name
            self.is_timeseries = is_timeseries

        def extract_data(self):
            return None

        def transform_data(self):
            return None

        def load_data(self):
            return None

    monkeypatch.setattr(file_ts_handler, "FileTransformer", DummyFileTransformer)

    body = {
        "data": {
            "resourceName": "obj.jsonl",
            "additionalDetails": {"bucketName": "b", "namespace": "ns"},
        }
    }
    data = io.BytesIO(json.dumps(body).encode("utf-8"))
    ctx = FakeCtx({})
    file_ts_handler.handler(ctx, data)
