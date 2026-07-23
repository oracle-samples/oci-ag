# Additional unit tests to raise coverage over 80%
# These tests avoid external services by using simple fakes/mocks.

import os
import types
from types import SimpleNamespace
from typing import Any, Dict


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


def test_logger_sets_oci_sdk_loggers_to_warning_by_default(monkeypatch):
    monkeypatch.delenv("DFA_OCI_LOG_LEVEL", raising=False)
    _ = Logger("unit.oci.loggers").get_logger()

    import logging

    assert logging.getLogger("oci.circuit_breaker.circuit_breaker").level == logging.WARNING
    assert logging.getLogger("oci.retry").level == logging.WARNING


def test_logger_respects_explicit_oci_sdk_log_level(monkeypatch):
    monkeypatch.setenv("DFA_OCI_LOG_LEVEL", "ERROR")
    _ = Logger("unit.oci.loggers.error").get_logger()

    import logging

    assert logging.getLogger("oci.circuit_breaker.circuit_breaker").level == logging.ERROR


def test_vault_clients_use_retry_strategy(monkeypatch):
    import common.ocihelpers.vault as vault_mod

    sentinel_retry = object()
    kms_calls = []
    secret_calls = []

    monkeypatch.setattr(vault_mod, "build_default_oci_retry_strategy", lambda: sentinel_retry)
    monkeypatch.setattr(
        vault_mod.oci.key_management,
        "KmsVaultClient",
        lambda **kwargs: kms_calls.append(kwargs) or object(),
    )
    monkeypatch.setattr(
        vault_mod.oci.secrets,
        "SecretsClient",
        lambda **kwargs: secret_calls.append(kwargs) or object(),
    )

    dfa_vault = vault_mod.DfaVault()
    dfa_vault._DfaVault__get_config = lambda: {"region": "iad"}  # type: ignore[attr-defined]
    dfa_vault._DfaVault__get_signer = lambda: "signer"  # type: ignore[attr-defined]
    _ = dfa_vault._get_kms_vault_client()

    adw_secrets = vault_mod.AdwSecrets()
    adw_secrets._DfaBaseSecret__get_config = lambda: {"region": "iad"}  # type: ignore[attr-defined]
    adw_secrets._DfaBaseSecret__get_signer = lambda: "signer"  # type: ignore[attr-defined]
    _ = adw_secrets._DfaBaseSecret__get_secret_client()  # type: ignore[attr-defined]

    assert kms_calls[0]["retry_strategy"] is sentinel_retry
    assert secret_calls[0]["retry_strategy"] is sentinel_retry


def test_vault_retry_strategy_retries_throttled_service_errors(monkeypatch):
    import common.ocihelpers.vault as vault_mod

    captured_options = {}

    class RetryStrategyBuilder:
        def __init__(self, **kwargs):
            captured_options.update(kwargs)

        def get_retry_strategy(self):
            return "retry-strategy"

    monkeypatch.setattr(vault_mod.oci.retry, "RetryStrategyBuilder", RetryStrategyBuilder)

    assert vault_mod.build_default_oci_retry_strategy() == "retry-strategy"
    assert captured_options["service_error_check"] is True
    assert captured_options["service_error_retry_config"] == {429: []}


def test_secret_exists_scopes_lookup_to_configured_vault(monkeypatch):
    import common.ocihelpers.vault as vault_mod

    monkeypatch.setenv("DFA_COMPARTMENT_ID", "compartment-ocid")
    monkeypatch.setenv("DFA_VAULT_ID", "configured-vault-ocid")
    calls = []

    def list_secrets(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(data=[])

    secrets = vault_mod.AdwSecrets()
    secrets._DfaBaseSecret__vault_client = SimpleNamespace(list_secrets=list_secrets)  # type: ignore[attr-defined]

    assert not secrets._secret_exists("shared-secret")
    assert calls == [(("compartment-ocid",), {"name": "shared-secret", "vault_id": "configured-vault-ocid"})]


def test_adw_configuration_update_includes_connection_secret(monkeypatch):
    import common.ocihelpers.function as function_mod

    class FakeClient:
        def __init__(self):
            self.update_calls = []

        def get_application(self, application_ocid):
            assert application_ocid == "application-ocid"
            return SimpleNamespace(data=SimpleNamespace(config={"EXISTING_CONFIG": "preserved"}))

        def update_application(self, **kwargs):
            self.update_calls.append(kwargs)
            return SimpleNamespace(data="updated-application")

    fake_client = FakeClient()
    adw_details = SimpleNamespace(
        connection_strings=SimpleNamespace(all_connection_strings={"LOW": "db.example.com:1522/dbservice_low"})
    )
    monkeypatch.setenv("DFA_ADW_INSTANCE_OCID", "adw-ocid")
    monkeypatch.setattr(
        function_mod, "BaseAutonomousDatabase", lambda: SimpleNamespace(get_details=lambda _: adw_details)
    )

    function_configs = function_mod.DfaSetupADWFunctionConfigs()
    function_configs._BaseFunction__client = fake_client  # type: ignore[attr-defined]

    assert (
        function_configs.add_adw_connection_string_to_configuration("application-ocid", "connection-secret-ocid")
        == "updated-application"
    )
    assert len(fake_client.update_calls) == 1
    config = fake_client.update_calls[0]["update_application_details"].config
    assert config == {
        "EXISTING_CONFIG": "preserved",
        "DFA_CONN_HOST": "db.example.com",
        "DFA_CONN_SERVICE_NAME": "dbservice_low",
        "DFA_ADW_CONNECTION_SECRET_OCID": "connection-secret-ocid",
    }


# 2) Bootstrap envvars tests
from dfa.bootstrap.envvars import bootstrap_base_environment_variables, bootstrap_local_machine_environment_variables
from dfa.bootstrap.image_version import get_package_version, resolve_image_version


def test_bootstrap_base_environment_variables_sets_expected_keys(monkeypatch):
    cfg: Dict[str, str] = {
        "DFA_ADW_CONNECTION_SECRET_OCID": "ocid1.vaultsecret.oc1..connection",
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


def test_bootstrap_accepts_consolidated_connection_secret_without_legacy_secrets(monkeypatch):
    cfg: Dict[str, str] = {
        "DFA_ADW_CONNECTION_SECRET_OCID": "ocid1.vaultsecret.oc1..connection",
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
    }

    bootstrap_base_environment_variables(cfg)

    assert os.environ["DFA_ADW_CONNECTION_SECRET_OCID"] == cfg["DFA_ADW_CONNECTION_SECRET_OCID"]


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


def test_resolve_image_version_prefers_explicit_env(monkeypatch):
    monkeypatch.setenv("DFA_IMAGE_VERSION", "1.2.3-explicit")
    assert resolve_image_version() == "1.2.3-explicit"


def test_resolve_image_version_uses_package_version_and_git_commit(monkeypatch):
    import dfa.bootstrap.image_version as image_version

    monkeypatch.delenv("DFA_IMAGE_VERSION", raising=False)
    monkeypatch.setattr(image_version, "get_git_commit", lambda project_root=None: "abcdef1")

    package_version = get_package_version()
    assert resolve_image_version() == f"{package_version}-abcdef1"


def test_resolve_image_version_ignores_image_version_env(monkeypatch):
    import dfa.bootstrap.image_version as image_version

    monkeypatch.delenv("DFA_IMAGE_VERSION", raising=False)
    monkeypatch.setenv("IMAGE_VERSION", "1.2.3-custom")
    monkeypatch.setattr(image_version, "get_git_commit", lambda project_root=None: "abcdef1")

    package_version = get_package_version()
    assert resolve_image_version() == f"{package_version}-abcdef1"


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

    monkeypatch.setattr(ft_mod, "BaseObjectStorage", lambda: FakeObjectStorage(json_dumps(payload).encode("utf-8")))

    t = FileTransformer("ns", "bucket", "object.json")
    # Set batch size env to 10
    monkeypatch.setenv("DFA_BATCH_SIZE", "10")
    t.extract_data()
    # fabricate simple transformer to bypass complex logic; but real transformer_factory may rely on object type
    # here we just ensure chunking method splits prepared events list respecting env var
    # Emulate prepared events
    t._prepared_events = events[:]  # type: ignore[attr-defined]
    chunks = t.chunk_prepared_events()
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
