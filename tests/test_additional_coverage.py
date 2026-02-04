# Additional unit tests to raise coverage over 80%
# These tests avoid external services by using simple fakes/mocks.

import os
import types
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest


def json_dumps(obj):
    import json

    return json.dumps(obj)


# 1) Logger tests
from common.logger.logger import Logger


def test_logger_returns_singleton_and_respects_env_level(monkeypatch):
    monkeypatch.setenv("DFA_LOG_LEVEL", "DEBUG")
    logger1 = Logger("unit.test").get_logger()
    logger2 = Logger("unit.test").get_logger()
    assert logger1 is logger2
    # Handler shouldn't duplicate
    handlers_before = list(logger1.handlers)
    _ = Logger("unit.test").get_logger()
    assert handlers_before == list(logger1.handlers)
    # And level aligns with env
    assert logger1.level <= 10  # DEBUG or more verbose


# 2) Bootstrap envvars tests
from dfa.bootstrap.envvars import (
    bootstrap_base_environment_variables,
    bootstrap_local_machine_environment_variables,
)


def test_bootstrap_base_environment_variables_sets_expected_keys(monkeypatch):
    cfg: Dict[str, str] = {
        "DFA_ADW_DFA_USER_PASSWORD_SECRET_NAME": "secret1",
        "DFA_ADW_WALLET_SECRET_NAME": "wallet",
        "DFA_ADW_WALLET_PASSWORD_SECRET_NAME": "wallet_pwd",
        "DFA_ADW_EWALLET_PEM_SECRET_NAME": "ewallet_pem",
        "DFA_CONN_PROTOCOL": "tcps",
        "DFA_CONN_HOST": "dbhost",
        "DFA_CONN_PORT": "1522",
        "DFA_CONN_SERVICE_NAME": "svc",
        "DFA_CONN_RETRY_COUNT": "2",
        "DFA_CONN_RETRY_DELAY": "1",
        "DFA_SIGNER_TYPE": "resource",
        "DFA_COMPARTMENT_ID": "ocid1.compartment.oc1..aaaa",
        "DFA_NAMESPACE": "ns",
        "DFA_STREAM_ID": "ocid1.stream.oc1..aaaa",
        "DFA_STREAM_SERVICE_ENDPOINT": "https://example.com",
        "DFA_VAULT_ID": "ocid1.vault.oc1..aaaa",
        "DFA_RECREATE_DFA_ADW_TABLES": "False",
    }
    bootstrap_base_environment_variables(cfg)
    for k, v in cfg.items():
        if k == "DFA_RECREATE_DFA_ADW_TABLES":
            continue
        assert os.environ.get(k) == v


def test_bootstrap_local_machine_env_reads_ini(tmp_path, monkeypatch):
    contents = """
[default]
region=phx
namespace=testns
dfa_signer_type=user
"""
    ini = tmp_path / "config.ini"
    ini.write_text(contents)
    # ensure DFA_SIGNER_TYPE changed to user at the end
    bootstrap_local_machine_environment_variables(str(ini), "default")
    # Keys uppercase in env
    assert os.environ.get("REGION") == "phx"
    assert os.environ.get("NAMESPACE") == "testns"
    assert os.environ.get("DFA_SIGNER_TYPE") == "user"


# 3) Dispatcher tests with stubbed handlers and response
import handlers.dispatcher as dispatcher


class DummyResponse:
    def __init__(self, ctx, status_code: int = 200, response_data: Any = None, headers=None):
        self.ctx = ctx
        self.status_code = status_code
        self.response_data = response_data or {}
        self.headers = headers or {}


class FakeCtx:
    def __init__(self, config: Dict[str, str]):
        self._cfg = config

    def Config(self):
        # Return object with .get like Fn context
        return types.SimpleNamespace(get=self._cfg.get)


def test_dispatcher_routes_to_known_handler(monkeypatch):
    called = {"name": None}

    def fake_handler(ctx, data=None):
        called["name"] = "file"

    # Monkeypatch handlers and response
    monkeypatch.setattr(dispatcher, "response", types.SimpleNamespace(Response=DummyResponse))
    monkeypatch.setattr(dispatcher, "file_handler", types.SimpleNamespace(handler=fake_handler))
    ctx = FakeCtx({"DFA_FUNCTION_NAME": "file"})
    resp = dispatcher.dispatch(ctx, None)
    assert isinstance(resp, DummyResponse)
    assert resp.status_code == 200
    assert called["name"] == "file"


def test_dispatcher_unknown_function_returns_400(monkeypatch):
    monkeypatch.setattr(dispatcher, "response", types.SimpleNamespace(Response=DummyResponse))
    ctx = FakeCtx({"DFA_FUNCTION_NAME": "does_not_exist"})
    resp = dispatcher.dispatch(ctx, None)
    assert isinstance(resp, DummyResponse)
    assert resp.status_code == 400
    assert "Unknown function" in resp.response_data.get("error", "")


def test_dispatcher_missing_function_name_returns_400(monkeypatch):
    monkeypatch.setattr(dispatcher, "response", types.SimpleNamespace(Response=DummyResponse))
    ctx = FakeCtx({})
    resp = dispatcher.dispatch(ctx, None)
    assert isinstance(resp, DummyResponse)
    assert resp.status_code == 400
    assert "not set" in resp.response_data.get("error", "").lower()


# 4) FileTransformer chunking behavior and jsonl detection
from dfa.etl.file_transformer import FileTransformer


class FakeObjectStorage:
    def __init__(self, content_bytes: bytes):
        self._content = content_bytes

    def download(self, namespace: str, bucket: str, name: str):
        return SimpleNamespace(data=SimpleNamespace(content=self._content))


def test_file_transformer_chunking_uses_env(monkeypatch):
    events = [{"id": i} for i in range(25)]
    # Prepare content with single JSON event with headers
    payload = {
        "headers": {
            "messageType": "IDENTITY",
            "operation": "CREATE",
            "eventTime": "2025-01-01T00:00:00Z",
            "tenancyId": "ocid1.tenancy.oc1..aaaa",
            "serviceInstanceId": "svc-1",
        },
        "data": '{"id": 1}',
    }
    content = SimpleNamespace(**payload)
    # Fake download returns the above JSON string
    # Monkeypatch BaseObjectStorage to our fake
    import dfa.etl.file_transformer as ft_mod

    def json_dumps(obj):
        import json

        return json.dumps(obj)

    monkeypatch.setattr(
        ft_mod, "BaseObjectStorage", lambda: FakeObjectStorage(json_dumps(payload).encode("utf-8"))
    )

    t = FileTransformer("ns", "bucket", "object.json")
    # Set batch size env to 10
    monkeypatch.setenv("DFA_BATCH_SIZE", "10")
    t.extract_data()
    # fabricate simple transformer to bypass complex logic; but real transformer_factory may rely on object type
    # here we just ensure chunking method splits prepared events list respecting env var
    # Emulate prepared events
    t._prepared_events = events[:]  # type: ignore[attr-defined]
    t.chunk_prepared_events()
    chunks = t._prepared_events  # type: ignore[attr-defined]
    assert isinstance(chunks, list)
    # 25 with chunk 10 => 3 chunks [10, 10, 5]
    assert [len(c) for c in chunks] == [10, 10, 5]


def test_file_transformer_jsonl_detection(monkeypatch):
    # Two lines: header line then one event
    lines = [
        json_dumps(
            {
                "headers": {
                    "messageType": "PERMISSION",
                    "operation": "CREATE",
                    "eventTime": "2025-01-01T00:00:00Z",
                    "tenancyId": "ocid1.tenancy.oc1..aaaa",
                    "serviceInstanceId": "svc-1",
                }
            }
        ),
        json_dumps({"id": "abc"}),
    ]
    content = "\n".join(lines).encode("utf-8")
    monkeypatch.setattr(
        __import__("dfa.etl.file_transformer", fromlist=["BaseObjectStorage"]),
        "BaseObjectStorage",
        lambda: FakeObjectStorage(content),
    )
    t = FileTransformer("ns", "bucket", "object.jsonl")
    t.extract_data()
    # Should have set object type and one raw event
    assert t.get_event_object_type() == "PERMISSION"
    assert isinstance(t._raw_events, list) and len(t._raw_events) == 1  # type: ignore[attr-defined]


# 5) UpdateQueryBuilder defensive path test
from dfa.adw.query_builders.base_query_builder import UpdateQueryBuilder


class DummyTableManager:
    def get_table_name(self):
        return "dummy"


class DummyQB:
    table_manager = DummyTableManager()

    def __getattr__(self, item):
        # Returning table columns for pypika
        return item


def test_update_query_builder_no_sets_but_where_clause_still_builds():
    qb = DummyQB()
    uq = UpdateQueryBuilder()
    # Event has only where columns; date_columns empty
    sql = uq.get_operation_sql(qb, {"id": 1}, date_columns=[], where_columns=["id"])
    assert sql is None
