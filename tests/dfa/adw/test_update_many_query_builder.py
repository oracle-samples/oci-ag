import re
from datetime import datetime
from unittest.mock import MagicMock, patch

import oracledb
import pytest
from pypika import Table

from dfa.adw.query_builders.access_bundle import (
    AccessBundleStateDeleteQueryBuilder,
    AccessBundleStateUpdateQueryBuilder,
)
from dfa.adw.query_builders.access_guardrail import AccessGuardrailStateDeleteQueryBuilder
from dfa.adw.query_builders.approval_workflow import ApprovalWorkflowStateDeleteQueryBuilder
from dfa.adw.query_builders.audit_events import AuditEventsStateCreateQueryBuilder
from dfa.adw.query_builders.base_query_builder import (
    BaseQueryBuilder,
    InsertManyQueryBuilder,
    MergeManyQueryBuilder,
    UpdateManyQueryBuilder,
)
from dfa.adw.query_builders.cloud_group import CloudGroupStateUpdateQueryBuilder
from dfa.adw.query_builders.cloud_policy import CloudPolicyStateDeleteQueryBuilder
from dfa.adw.query_builders.identity import IdentityStateDeleteQueryBuilder, IdentityStateUpdateQueryBuilder
from dfa.adw.query_builders.orchestrated_system import OrchestratedSystemStateDeleteQueryBuilder
from dfa.adw.query_builders.permission import PermissionStateDeleteQueryBuilder
from dfa.adw.query_builders.permission_assignment import PermissionAssignmentStateDeleteQueryBuilder
from dfa.adw.query_builders.policy_statement_resource_mapping import (
    PolicyStatementResourceMappingStateDeleteQueryBuilder,
)
from dfa.adw.query_builders.resource import ResourceStateDeleteQueryBuilder
from dfa.adw.tables.access_bundle import AccessBundleStateTable
from dfa.adw.tables.access_guardrail import AccessGuardrailStateTable
from dfa.adw.tables.approval_workflow import ApprovalWorkflowStateTable
from dfa.adw.tables.base_table import BaseStateTable, SnapshotBatchTrackerTable
from dfa.adw.tables.cloud_group import CloudGroupStateTable
from dfa.adw.tables.cloud_policy import CloudPolicyStateTable
from dfa.adw.tables.global_identity_collection import GlobalIdentityCollectionStateTable
from dfa.adw.tables.identity import IdentityStateTable
from dfa.adw.tables.orchestrated_system import OrchestratedSystemStateTable
from dfa.adw.tables.ownership_collection import OwnershipCollectionStateTable
from dfa.adw.tables.permission import PermissionStateTable
from dfa.adw.tables.permission_assignment import PermissionAssignmentStateTable
from dfa.adw.tables.policy import PolicyStateTable
from dfa.adw.tables.policy_statement_resource_mapping import PolicyStatementResourceMappingStateTable
from dfa.adw.tables.resource import ResourceStateTable
from dfa.adw.tables.role import RoleStateTable


@pytest.fixture(autouse=True)
def _set_adw_schema(monkeypatch):
    monkeypatch.setenv("DFA_ADW_DFA_SCHEMA", "DFA")
    BaseStateTable._ensured_delete_index_names.clear()


def _normalize_sql(sql: str) -> str:
    # helper to collapse whitespace for simpler assertions
    return re.sub(r"\s+", " ", sql).strip()


def test_update_many_nullable_none_uses_equality():
    qb = Table("dummy")
    builder = UpdateManyQueryBuilder()

    events = [{"id": 1, "name": "Alice"}]
    # where on id, update should set NAME only; nullable_columns is None
    sql = builder.get_operation_sql(qb, events, date_columns=[], where_columns=["id"], nullable_columns=None)

    assert sql is not None
    norm = _normalize_sql(sql).lower()
    # basic shape
    assert "update" in norm and " where " in norm
    # should NOT use DECODE when nullable_columns is None
    assert "decode(" not in norm


def test_update_many_nullable_lowercase_uses_decode_for_where():
    qb = Table("dummy")
    builder = UpdateManyQueryBuilder()

    events = [{"id": None, "name": "Bob"}]
    # mark id as nullable via lowercase spelling
    sql = builder.get_operation_sql(qb, events, date_columns=[], where_columns=["id"], nullable_columns=["id"])

    assert sql is not None
    norm = _normalize_sql(sql).upper()
    assert "UPDATE" in norm and " WHERE " in norm
    # should use Oracle DECODE for NULL-safe equality on the where column
    assert "DECODE(" in norm


def test_update_many_nullable_uppercase_uses_decode_for_where():
    qb = Table("dummy")
    builder = UpdateManyQueryBuilder()

    events = [{"ID": None, "NAME": "Carol"}]
    # where uses lowercase, nullable passed in uppercase to verify case-insensitivity
    sql = builder.get_operation_sql(qb, events, date_columns=[], where_columns=["id"], nullable_columns=["ID"])

    assert sql is not None
    norm = _normalize_sql(sql).upper()
    assert "UPDATE" in norm and " WHERE " in norm
    # still should engage DECODE because nullable list is case-insensitive
    assert "DECODE(" in norm


def test_merge_many_binds_clob_columns_directly():
    qb = Table("dummy")
    qb.table_manager = MagicMock()
    qb.table_manager.get_schema.return_value = "dfa"
    qb.table_manager.get_table_name.return_value = "dummy_table"

    builder = MergeManyQueryBuilder()
    events = [{"id": "1", "attributes": '{"large":"payload"}', "name": "Alice"}]

    sql = builder.get_operation_sql(qb, events, date_columns=[], where_columns=["id"])

    norm = _normalize_sql(sql).upper()
    assert ':ATTRIBUTES AS "ATTRIBUTES"' in norm
    assert "TO_CLOB(:ATTRIBUTES)" not in norm
    assert 'MERGE INTO "DFA"."DUMMY_TABLE"' in norm


def test_state_delete_keys_are_indexed():
    def indexed_columns(table):
        indexes = [table.get_unique_contraint_definition_details()]
        indexes.extend(table.get_delete_index_definition_details())
        return [index["columns"] for index in indexes]

    def assert_delete_key_indexed(table, delete_key):
        delete_key = [column.upper() for column in delete_key]
        assert any(
            columns[: len(delete_key)] == delete_key for columns in indexed_columns(table)
        ), f"{table.get_table_name()} delete key is not indexed: {delete_key}"

    table_delete_keys = [
        (AccessBundleStateTable(), [["ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"]]),
        (AccessGuardrailStateTable(), [["ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"]]),
        (ApprovalWorkflowStateTable(), [["ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"]]),
        (
            CloudGroupStateTable(),
            [
                ["ID", "IDENTITY_GLOBAL_ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"],
                ["ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"],
            ],
        ),
        (CloudPolicyStateTable(), [["POLICY_STATEMENT_ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"]]),
        (
            GlobalIdentityCollectionStateTable(),
            [
                ["ID", "MEMBER_GLOBAL_ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"],
                ["ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"],
            ],
        ),
        (
            IdentityStateTable(),
            [
                ["TI_ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"],
                ["ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"],
            ],
        ),
        (OrchestratedSystemStateTable(), [["ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"]]),
        (OwnershipCollectionStateTable(), [["ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"]]),
        (PermissionStateTable(), [["ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"]]),
        (
            PermissionAssignmentStateTable(),
            [
                ["TARGET_IDENTITY_ID", "PERMISSION_ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"],
                ["TARGET_IDENTITY_ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"],
            ],
        ),
        (PolicyStateTable(), [["ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"]]),
        (
            PolicyStatementResourceMappingStateTable(),
            [["POLICY_STATEMENT_ID", "RESOURCE_ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"]],
        ),
        (ResourceStateTable(), [["ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"]]),
        (RoleStateTable(), [["ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"]]),
    ]

    for table, delete_keys in table_delete_keys:
        for delete_key in delete_keys:
            assert_delete_key_indexed(table, delete_key)


def _assert_bulk_delete(cursor, expected_bind_rows):
    executed_sql = cursor.executemany.call_args.args[0]
    bind_rows = cursor.executemany.call_args.args[1]
    assert cursor.executemany.call_count == 1
    assert '"ID"=:ID' in executed_sql
    assert '"SERVICE_INSTANCE_ID"=:SERVICE_INSTANCE_ID' in executed_sql
    assert '"TENANCY_ID"=:TENANCY_ID' in executed_sql
    assert bind_rows == expected_bind_rows


def _assert_bulk_delete_with_columns(cursor, expected_columns, expected_bind_rows):
    executed_sql = cursor.executemany.call_args.args[0]
    bind_rows = cursor.executemany.call_args.args[1]
    assert cursor.executemany.call_count == 1
    for column in expected_columns:
        assert f'"{column}"=:{column}' in executed_sql
    assert bind_rows == expected_bind_rows


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.get_cursor")
def test_access_bundle_delete_uses_bulk_delete(mock_get_cursor, mock_commit):
    cursor = MagicMock()
    cursor.getbatcherrors.return_value = []
    mock_get_cursor.return_value = cursor
    qb = AccessBundleStateDeleteQueryBuilder(
        [
            {"id": "bundle-1", "service_instance_id": "svc-1", "tenancy_id": "tenant-1"},
            {"id": "bundle-2", "service_instance_id": "svc-1", "tenancy_id": "tenant-1"},
        ]
    )

    with patch.object(qb.logger, "info") as mock_log:
        qb.execute_sql_for_events()

    _assert_bulk_delete(
        cursor,
        [
            {"ID": "bundle-1", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
            {"ID": "bundle-2", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
        ],
    )
    mock_log.assert_any_call("Bulk delete for access bundle delete request")
    mock_commit.assert_called_once()


@patch("dfa.adw.query_builders.base_query_builder.perf_counter", side_effect=[10.0, 12.5])
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.get_cursor")
def test_execute_sql_for_events_logs_runtime(mock_get_cursor, mock_commit, _mock_perf_counter):
    cursor = MagicMock()
    cursor.getbatcherrors.return_value = []
    mock_get_cursor.return_value = cursor
    qb = AccessBundleStateDeleteQueryBuilder(
        [
            {"id": "bundle-1", "service_instance_id": "svc-1", "tenancy_id": "tenant-1"},
        ]
    )

    with patch.object(qb.logger, "info") as mock_log:
        qb.execute_sql_for_events()

    mock_log.assert_any_call(
        "%s execute_sql_for_events runtime: %.3fs for %d event(s)",
        "access_bundle_state",
        2.5,
        1,
    )
    mock_commit.assert_called_once()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.get_cursor")
def test_access_guardrail_delete_uses_bulk_delete(mock_get_cursor, mock_commit):
    cursor = MagicMock()
    cursor.getbatcherrors.return_value = []
    mock_get_cursor.return_value = cursor
    qb = AccessGuardrailStateDeleteQueryBuilder(
        [
            {"id": "guardrail-1", "service_instance_id": "svc-1", "tenancy_id": "tenant-1"},
            {"id": "guardrail-2", "service_instance_id": "svc-1", "tenancy_id": "tenant-1"},
        ]
    )

    with patch.object(qb.logger, "info") as mock_log:
        qb.execute_sql_for_events()

    _assert_bulk_delete(
        cursor,
        [
            {"ID": "guardrail-1", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
            {"ID": "guardrail-2", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
        ],
    )
    mock_log.assert_any_call("Bulk delete for access guardrail delete request")
    mock_commit.assert_called_once()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.get_cursor")
def test_approval_workflow_delete_uses_bulk_delete(mock_get_cursor, mock_commit):
    cursor = MagicMock()
    cursor.getbatcherrors.return_value = []
    mock_get_cursor.return_value = cursor
    qb = ApprovalWorkflowStateDeleteQueryBuilder(
        [
            {"id": "workflow-1", "service_instance_id": "svc-1", "tenancy_id": "tenant-1"},
            {"id": "workflow-2", "service_instance_id": "svc-1", "tenancy_id": "tenant-1"},
        ]
    )

    with patch.object(qb.logger, "info") as mock_log:
        qb.execute_sql_for_events()

    _assert_bulk_delete(
        cursor,
        [
            {"ID": "workflow-1", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
            {"ID": "workflow-2", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
        ],
    )
    mock_log.assert_any_call("Bulk delete for approval workflow delete request")
    mock_commit.assert_called_once()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.get_cursor")
def test_cloud_policy_delete_uses_bulk_delete(mock_get_cursor, mock_commit):
    cursor = MagicMock()
    cursor.getbatcherrors.return_value = []
    mock_get_cursor.return_value = cursor
    qb = CloudPolicyStateDeleteQueryBuilder(
        [
            {
                "policy_statement_id": "statement-1",
                "service_instance_id": "svc-1",
                "tenancy_id": "tenant-1",
            },
            {
                "policy_statement_id": "statement-2",
                "service_instance_id": "svc-1",
                "tenancy_id": "tenant-1",
            },
        ]
    )

    with patch.object(qb.logger, "info") as mock_log:
        qb.execute_sql_for_events()

    _assert_bulk_delete_with_columns(
        cursor,
        ["POLICY_STATEMENT_ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"],
        [
            {
                "POLICY_STATEMENT_ID": "statement-1",
                "SERVICE_INSTANCE_ID": "svc-1",
                "TENANCY_ID": "tenant-1",
            },
            {
                "POLICY_STATEMENT_ID": "statement-2",
                "SERVICE_INSTANCE_ID": "svc-1",
                "TENANCY_ID": "tenant-1",
            },
        ],
    )
    mock_log.assert_any_call("Bulk delete for tgt access pol stmt delete request")
    mock_commit.assert_called_once()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.get_cursor")
def test_orchestrated_system_delete_uses_bulk_delete(mock_get_cursor, mock_commit):
    cursor = MagicMock()
    cursor.getbatcherrors.return_value = []
    mock_get_cursor.return_value = cursor
    qb = OrchestratedSystemStateDeleteQueryBuilder(
        [
            {"id": "system-1", "service_instance_id": "svc-1", "tenancy_id": "tenant-1"},
            {"id": "system-2", "service_instance_id": "svc-1", "tenancy_id": "tenant-1"},
        ]
    )

    with patch.object(qb.logger, "info") as mock_log:
        qb.execute_sql_for_events()

    _assert_bulk_delete(
        cursor,
        [
            {"ID": "system-1", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
            {"ID": "system-2", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
        ],
    )
    mock_log.assert_any_call("Bulk delete for orchestrated system delete request")
    mock_commit.assert_called_once()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.get_cursor")
def test_resource_delete_uses_bulk_delete(mock_get_cursor, mock_commit):
    cursor = MagicMock()
    cursor.getbatcherrors.return_value = []
    mock_get_cursor.return_value = cursor
    qb = ResourceStateDeleteQueryBuilder(
        [
            {"id": "resource-1", "service_instance_id": "svc-1", "tenancy_id": "tenant-1"},
            {"id": "resource-2", "service_instance_id": "svc-1", "tenancy_id": "tenant-1"},
        ]
    )

    with patch.object(qb.logger, "info") as mock_log:
        qb.execute_sql_for_events()

    _assert_bulk_delete(
        cursor,
        [
            {"ID": "resource-1", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
            {"ID": "resource-2", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
        ],
    )
    mock_log.assert_any_call("Bulk delete for resource delete request")
    mock_commit.assert_called_once()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.get_cursor")
def test_permission_delete_uses_bulk_delete(mock_get_cursor, mock_commit):
    cursor = MagicMock()
    cursor.getbatcherrors.return_value = []
    mock_get_cursor.return_value = cursor
    qb = PermissionStateDeleteQueryBuilder(
        [
            {"id": "permission-1", "service_instance_id": "svc-1", "tenancy_id": "tenant-1"},
            {"id": "permission-2", "service_instance_id": "svc-1", "tenancy_id": "tenant-1"},
        ]
    )

    with patch.object(qb.logger, "info") as mock_log:
        qb.execute_sql_for_events()

    _assert_bulk_delete(
        cursor,
        [
            {"ID": "permission-1", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
            {"ID": "permission-2", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
        ],
    )
    mock_log.assert_any_call("Bulk delete for permission delete request")
    mock_commit.assert_called_once()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.get_cursor")
def test_identity_delete_uses_bulk_delete_for_global_identity(mock_get_cursor, mock_commit):
    cursor = MagicMock()
    cursor.getbatcherrors.return_value = []
    mock_get_cursor.return_value = cursor
    qb = IdentityStateDeleteQueryBuilder(
        [
            {
                "id": "identity-1",
                "ti_id": "",
                "service_instance_id": "svc-1",
                "tenancy_id": "tenant-1",
            },
            {
                "id": "identity-2",
                "ti_id": "",
                "service_instance_id": "svc-1",
                "tenancy_id": "tenant-1",
            },
        ]
    )

    with patch.object(qb.logger, "info") as mock_log:
        qb.execute_sql_for_events()

    _assert_bulk_delete_with_columns(
        cursor,
        ["ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"],
        [
            {"ID": "identity-1", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
            {"ID": "identity-2", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
        ],
    )
    mock_log.assert_any_call("Bulk delete for identity delete request")
    mock_commit.assert_called_once()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.get_cursor")
def test_identity_delete_uses_bulk_delete_for_target_identity(mock_get_cursor, mock_commit):
    cursor = MagicMock()
    cursor.getbatcherrors.return_value = []
    mock_get_cursor.return_value = cursor
    qb = IdentityStateDeleteQueryBuilder(
        [
            {
                "id": "",
                "ti_id": "target-1",
                "service_instance_id": "svc-1",
                "tenancy_id": "tenant-1",
            },
            {
                "id": "",
                "ti_id": "target-2",
                "service_instance_id": "svc-1",
                "tenancy_id": "tenant-1",
            },
        ]
    )

    with patch.object(qb.logger, "info") as mock_log:
        qb.execute_sql_for_events()

    _assert_bulk_delete_with_columns(
        cursor,
        ["TI_ID", "SERVICE_INSTANCE_ID", "TENANCY_ID"],
        [
            {"TI_ID": "target-1", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
            {"TI_ID": "target-2", "SERVICE_INSTANCE_ID": "svc-1", "TENANCY_ID": "tenant-1"},
        ],
    )
    mock_log.assert_any_call("Bulk delete for target identity delete request")
    mock_commit.assert_called_once()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.get_cursor")
@patch.dict("os.environ", {"DFA_ADW_DFA_SCHEMA": "DFA"})
def test_policy_statement_resource_mapping_delete_uses_bulk_delete(mock_get_cursor, mock_commit):
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)
    cursor.getbatcherrors.return_value = []
    mock_get_cursor.return_value = cursor
    qb = PolicyStatementResourceMappingStateDeleteQueryBuilder(
        [
            {
                "id": "mapping-1",
                "policy_statement_id": "statement-1",
                "resource_id": "resource-1",
                "service_instance_id": "svc-1",
                "tenancy_id": "tenant-1",
            },
            {
                "id": "mapping-2",
                "policy_statement_id": "statement-2",
                "resource_id": "resource-2",
                "service_instance_id": "svc-1",
                "tenancy_id": "tenant-1",
            },
        ]
    )

    with patch.object(qb.logger, "info") as mock_log:
        qb.execute_sql_for_events()

    executed_sql = cursor.executemany.call_args.args[0]
    bind_rows = cursor.executemany.call_args.args[1]
    assert cursor.executemany.call_count == 1
    assert all("DELETE FROM" not in call.args[0].upper() for call in cursor.execute.call_args_list)
    assert '"POLICY_STATEMENT_ID"=:POLICY_STATEMENT_ID' in executed_sql
    assert '"RESOURCE_ID"=:RESOURCE_ID' in executed_sql
    assert '"SERVICE_INSTANCE_ID"=:SERVICE_INSTANCE_ID' in executed_sql
    assert '"TENANCY_ID"=:TENANCY_ID' in executed_sql
    assert bind_rows == [
        {
            "POLICY_STATEMENT_ID": "statement-1",
            "RESOURCE_ID": "resource-1",
            "SERVICE_INSTANCE_ID": "svc-1",
            "TENANCY_ID": "tenant-1",
        },
        {
            "POLICY_STATEMENT_ID": "statement-2",
            "RESOURCE_ID": "resource-2",
            "SERVICE_INSTANCE_ID": "svc-1",
            "TENANCY_ID": "tenant-1",
        },
    ]
    mock_log.assert_any_call("Bulk delete for policy statement resource mapping delete request")
    mock_commit.assert_called_once()


@patch("dfa.adw.tables.base_table.AdwConnection.get_cursor")
def test_policy_statement_resource_mapping_delete_index_is_created_when_missing(mock_get_cursor):
    cursor = MagicMock()
    cursor.fetchone.return_value = (0,)
    mock_get_cursor.return_value = cursor
    table = PolicyStatementResourceMappingStateTable()
    table._schema = "DFA"

    table.ensure_delete_indexes()

    assert cursor.execute.call_count == 2
    index_check_sql = cursor.execute.call_args_list[0].args[0]
    create_index_sql = cursor.execute.call_args_list[1].args[0]
    assert "ALL_INDEXES" in index_check_sql
    assert "DFA_PSRM_ST_DEL_IDX" in create_index_sql
    assert '"POLICY_STATEMENT_ID"' in create_index_sql
    assert '"RESOURCE_ID"' in create_index_sql
    assert '"SERVICE_INSTANCE_ID"' in create_index_sql
    assert '"TENANCY_ID"' in create_index_sql


def test_base_query_builder_event_sized_clob_uses_string_size_for_small_values():
    input_sizes = BaseQueryBuilder().get_input_sizes_for_events(
        [
            {"column_name": "ID", "data_type": "VARCHAR2", "data_length": 32767},
            {"column_name": "ATTRIBUTES", "data_type": "CLOB", "data_length": None},
        ],
        [{"id": "abc", "attributes": '{"small":true}'}],
    )

    assert input_sizes["ID"] == 3
    assert input_sizes["ATTRIBUTES"] == 14


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.get_cursor")
def test_permission_assignment_delete_with_permission_filters_input_sizes_to_sql_binds(mock_get_cursor, mock_commit):
    cursor = MagicMock()
    cursor.getbatcherrors.return_value = []
    mock_get_cursor.return_value = cursor

    qb = PermissionAssignmentStateDeleteQueryBuilder(
        [
            {
                "target_identity_id": "identity-1",
                "global_identity_id": "global-1",
                "permission_id": "permission-1",
                "service_instance_id": "svc-1",
                "tenancy_id": "tenant-1",
            }
        ]
    )

    qb.execute_sql_for_events()

    input_size_names = set(cursor.setinputsizes.call_args.kwargs)
    assert input_size_names == {
        "TARGET_IDENTITY_ID",
        "PERMISSION_ID",
        "SERVICE_INSTANCE_ID",
        "TENANCY_ID",
    }
    assert cursor.setinputsizes.call_args.kwargs == {
        "TARGET_IDENTITY_ID": len("identity-1"),
        "PERMISSION_ID": len("permission-1"),
        "SERVICE_INSTANCE_ID": len("svc-1"),
        "TENANCY_ID": len("tenant-1"),
    }
    executed_sql = cursor.executemany.call_args.args[0]
    assert ":global_identity_id" not in executed_sql
    assert ":GLOBAL_IDENTITY_ID" not in executed_sql
    assert "GLOBAL_IDENTITY_ID" not in executed_sql
    assert ":TARGET_IDENTITY_ID" in executed_sql
    assert ":PERMISSION_ID" in executed_sql
    assert ":SERVICE_INSTANCE_ID" in executed_sql
    assert ":TENANCY_ID" in executed_sql
    bind_rows = cursor.executemany.call_args.args[1]
    assert bind_rows == [
        {
            "TARGET_IDENTITY_ID": "identity-1",
            "PERMISSION_ID": "permission-1",
            "SERVICE_INSTANCE_ID": "svc-1",
            "TENANCY_ID": "tenant-1",
        }
    ]
    assert "GLOBAL_IDENTITY_ID" not in input_size_names
    assert "global_identity_id" not in input_size_names
    mock_commit.assert_called_once()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.get_cursor")
def test_permission_assignment_delete_without_permission_filters_input_sizes_to_sql_binds(mock_get_cursor, mock_commit):
    cursor = MagicMock()
    cursor.getbatcherrors.return_value = []
    mock_get_cursor.return_value = cursor

    qb = PermissionAssignmentStateDeleteQueryBuilder(
        [
            {
                "target_identity_id": "identity-1",
                "global_identity_id": "global-1",
                "permission_id": "",
                "service_instance_id": "svc-1",
                "tenancy_id": "tenant-1",
            }
        ]
    )

    qb.execute_sql_for_events()

    input_size_names = set(cursor.setinputsizes.call_args.kwargs)
    assert input_size_names == {
        "TARGET_IDENTITY_ID",
        "SERVICE_INSTANCE_ID",
        "TENANCY_ID",
    }
    assert cursor.setinputsizes.call_args.kwargs == {
        "TARGET_IDENTITY_ID": len("identity-1"),
        "SERVICE_INSTANCE_ID": len("svc-1"),
        "TENANCY_ID": len("tenant-1"),
    }
    executed_sql = cursor.executemany.call_args.args[0]
    assert ":global_identity_id" not in executed_sql
    assert ":GLOBAL_IDENTITY_ID" not in executed_sql
    assert "GLOBAL_IDENTITY_ID" not in executed_sql
    assert ":TARGET_IDENTITY_ID" in executed_sql
    assert ":SERVICE_INSTANCE_ID" in executed_sql
    assert ":TENANCY_ID" in executed_sql
    bind_rows = cursor.executemany.call_args.args[1]
    assert bind_rows == [
        {
            "TARGET_IDENTITY_ID": "identity-1",
            "SERVICE_INSTANCE_ID": "svc-1",
            "TENANCY_ID": "tenant-1",
        }
    ]
    assert "GLOBAL_IDENTITY_ID" not in input_size_names
    assert "global_identity_id" not in input_size_names
    mock_commit.assert_called_once()


@patch("dfa.adw.query_builders.audit_events.AdwConnection.commit")
@patch("dfa.adw.query_builders.audit_events.AdwConnection.get_cursor")
def test_audit_events_insert_uses_event_sized_input_sizes(mock_get_cursor, mock_commit):
    cursor = MagicMock()
    cursor.getbatcherrors.return_value = []
    mock_get_cursor.return_value = cursor

    qb = AuditEventsStateCreateQueryBuilder(
        [
            {
                "source": "audit",
                "audit_event_type": "com.oracle.test",
                "request_time": 123,
                "request_payload": '{"a":1}',
                "event_object_type": "AUDIT_EVENTS",
                "operation_type": "CREATE",
                "tenancy_id": "tenant-1",
            }
        ]
    )

    qb.execute_sql_for_events()

    assert cursor.setinputsizes.call_args.kwargs["SOURCE"] == len("audit")
    assert cursor.setinputsizes.call_args.kwargs["AUDIT_EVENT_TYPE"] == len("com.oracle.test")
    assert cursor.setinputsizes.call_args.kwargs["REQUEST_PAYLOAD"] == len('{"a":1}')
    assert cursor.setinputsizes.call_args.kwargs["REQUEST_TIME"] == oracledb.NUMBER
    assert "SERVICE_INSTANCE_ID" not in cursor.setinputsizes.call_args.kwargs
    bind_rows = cursor.executemany.call_args.args[1]
    assert bind_rows[0]["SOURCE"] == "audit"
    assert bind_rows[0]["REQUEST_PAYLOAD"] == '{"a":1}'
    mock_commit.assert_called_once()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.get_cursor")
def test_merge_unique_constraint_races_log_info_and_retry_update(mock_get_cursor, mock_commit):
    class BatchError:
        def __init__(self, offset, full_code, message):
            self.offset = offset
            self.full_code = full_code
            self.message = message

    cursor = MagicMock()
    cursor.getbatcherrors.side_effect = [
        [BatchError(0, "ORA-00001", "ORA-00001: unique constraint violated")],
        [],
    ]
    mock_get_cursor.return_value = cursor

    qb = AccessBundleStateUpdateQueryBuilder(
        [
            {
                "id": "ab-1",
                "external_id": "ext-1",
                "tenancy_id": "tenant-1",
                "service_instance_id": "svc-1",
            }
        ]
    )
    qb.logger = MagicMock()
    qb.table_manager = MagicMock()
    qb.table_manager.get_table_name.return_value = "access_bundle_state"
    qb.table_manager.get_schema.return_value = "DFA"
    qb.table_manager.get_unique_contraint_definition_details.return_value = {
        "columns": ["ID", "TENANCY_ID", "SERVICE_INSTANCE_ID"]
    }
    qb.table_manager.get_nullable_constraint_columns.return_value = []
    qb.table_manager.get_column_list_definition_for_table_ddl.return_value = [
        {"column_name": "ID", "data_type": "VARCHAR2", "data_length": 32767},
        {"column_name": "EXTERNAL_ID", "data_type": "VARCHAR2", "data_length": 32767},
        {"column_name": "TENANCY_ID", "data_type": "VARCHAR2", "data_length": 32767},
        {"column_name": "SERVICE_INSTANCE_ID", "data_type": "VARCHAR2", "data_length": 32767},
    ]

    qb.executemany_state_merge_for_events()

    qb.logger.warning.assert_not_called()
    qb.logger.info.assert_any_call(
        "%d %s merge row(s) hit unique constraint races; retrying as bulk updates",
        1,
        "access_bundle_state",
    )
    assert cursor.executemany.call_count == 2
    mock_commit.assert_called()


@patch("dfa.adw.connection.AdwConnection.get_cursor")
@patch("dfa.adw.connection.AdwConnection.commit")
def test_delete_rows_older_than_event_timestamp_scopes_by_tenant_and_service(mock_commit, mock_get_cursor):
    cursor = MagicMock()
    mock_get_cursor.return_value = cursor

    qb = AccessBundleStateUpdateQueryBuilder([])
    qb.delete_rows_older_than_event_timestamp(
        "14-Apr-26 09:40:20.331306 PM",
        tenancy_id="tenant-1",
        service_instance_id="svc-1",
    )

    executed_sql = cursor.execute.call_args.args[0]
    bind_values = cursor.execute.call_args.args[1]
    normalized = _normalize_sql(executed_sql).upper()
    assert 'DELETE FROM "ACCESS_BUNDLE_STATE"' in normalized
    assert '"EVENT_TIMESTAMP" < TO_TIMESTAMP(:COMPLETION_TIMESTAMP,' in normalized
    assert '"TENANCY_ID" = :TENANCY_ID' in normalized
    assert '"SERVICE_INSTANCE_ID" = :SERVICE_INSTANCE_ID' in normalized
    assert "DD-MON-RR HH24:MI:SS.FF6" in normalized
    assert bind_values["COMPLETION_TIMESTAMP"] == "14-Apr-26 21:40:20.331306"
    assert bind_values["TENANCY_ID"] == "tenant-1"
    assert bind_values["SERVICE_INSTANCE_ID"] == "svc-1"
    mock_commit.assert_called_once()


@patch("dfa.adw.connection.AdwConnection.get_cursor")
def test_snapshot_helper_queries_use_scope_and_snapshot(mock_get_cursor):
    cursor = MagicMock()
    mock_get_cursor.return_value = cursor
    cursor.fetchone.side_effect = [(1,)]

    qb = AccessBundleStateUpdateQueryBuilder([])
    qb._snapshot_batch_tracker_table = MagicMock()
    qb._snapshot_batch_tracker_table.get_schema.return_value = "DFA"
    qb._snapshot_batch_tracker_table.get_table_name.return_value = "SNAPSHOT_BATCH_TRACKER"

    completed_count = qb._snapshot_get_completed_batch_count("snapshot-1", "tenant-1", "svc-1")

    assert completed_count == 1
    assert cursor.execute.call_count == 1
    first_bind = cursor.execute.call_args_list[0].args[1]
    assert first_bind["ENTITY_TYPE"] == "ACCESS_BUNDLE"
    assert first_bind["TENANCY_ID"] == "tenant-1"
    assert first_bind["SERVICE_INSTANCE_ID"] == "svc-1"
    assert first_bind["SNAPSHOT_ID"] == "snapshot-1"


@patch("dfa.adw.connection.AdwConnection.get_cursor")
def test_snapshot_get_earliest_batch_timestamp_uses_scope_and_formats_datetime(mock_get_cursor):
    cursor = MagicMock()
    mock_get_cursor.return_value = cursor
    cursor.fetchone.side_effect = [(datetime(2026, 4, 14, 21, 40, 20, 331306),)]

    qb = AccessBundleStateUpdateQueryBuilder([])
    qb._snapshot_batch_tracker_table = MagicMock()
    qb._snapshot_batch_tracker_table.get_schema.return_value = "DFA"
    qb._snapshot_batch_tracker_table.get_table_name.return_value = "SNAPSHOT_BATCH_TRACKER"

    earliest_timestamp = qb._snapshot_get_earliest_batch_timestamp("snapshot-1", "tenant-1", "svc-1")

    assert earliest_timestamp == "14-Apr-26 21:40:20.331306"
    first_bind = cursor.execute.call_args_list[0].args[1]
    assert first_bind["ENTITY_TYPE"] == "ACCESS_BUNDLE"
    assert first_bind["TENANCY_ID"] == "tenant-1"
    assert first_bind["SERVICE_INSTANCE_ID"] == "svc-1"
    assert first_bind["SNAPSHOT_ID"] == "snapshot-1"


@patch("dfa.adw.connection.AdwConnection.get_cursor")
def test_snapshot_cleanup_lock_targets_deterministic_tracker_row(mock_get_cursor):
    cursor = MagicMock()
    mock_get_cursor.return_value = cursor
    cursor.fetchone.return_value = ("batch-1",)

    qb = AccessBundleStateUpdateQueryBuilder([])
    qb._snapshot_batch_tracker_table = MagicMock()
    qb._snapshot_batch_tracker_table.get_schema.return_value = "DFA"
    qb._snapshot_batch_tracker_table.get_table_name.return_value = "SNAPSHOT_BATCH_TRACKER"

    assert qb._try_acquire_snapshot_cleanup_lock("snapshot-1", "tenant-1", "svc-1")

    lock_sql = cursor.execute.call_args.args[0]
    assert "MIN(BATCH_ID)" in lock_sql
    assert "BATCH_ID = (" in lock_sql
    assert "ROWNUM = 1" not in lock_sql
    assert "FOR UPDATE NOWAIT" in lock_sql


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
def test_ensure_helper_table_exists_ensures_supporting_objects_for_existing_table(mock_commit):
    qb = AccessBundleStateUpdateQueryBuilder([])
    tracker_table = MagicMock()

    qb._ensure_helper_table_exists(tracker_table)

    tracker_table.create.assert_called_once()
    tracker_table.ensure_supporting_objects.assert_called_once()
    mock_commit.assert_called_once()


@patch("dfa.adw.connection.AdwConnection.get_cursor")
def test_snapshot_batch_tracker_ensures_primary_key_when_missing(mock_get_cursor):
    cursor = MagicMock()
    cursor.fetchone.return_value = (0,)
    mock_get_cursor.return_value = cursor

    tracker_table = SnapshotBatchTrackerTable()
    tracker_table._schema = "DFA"

    tracker_table.ensure_supporting_objects()

    assert cursor.execute.call_count == 2
    constraint_check_sql = cursor.execute.call_args_list[0].args[0]
    add_constraint_sql = cursor.execute.call_args_list[1].args[0]
    assert "ALL_CONSTRAINTS" in constraint_check_sql
    assert "PK_SNAPSHOT_BATCH_TRACKER" in add_constraint_sql
    assert "PRIMARY KEY" in add_constraint_sql


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.rollback")
def test_finalize_snapshot_cleanup_if_ready_returns_when_completed_count_not_reached(mock_rollback):
    qb = IdentityStateUpdateQueryBuilder([])
    qb._snapshot_get_completed_batch_count = MagicMock(return_value=2)
    qb._try_acquire_snapshot_cleanup_lock = MagicMock()
    qb._snapshot_get_earliest_batch_timestamp = MagicMock()
    qb.delete_rows_older_than_event_timestamp = MagicMock()
    qb._delete_snapshot_batch_tracking = MagicMock()

    qb.finalize_snapshot_cleanup_if_ready(
        "snapshot-1",
        num_of_batches=3,
        tenancy_id="tenant-1",
        service_instance_id="svc-1",
    )

    qb.delete_rows_older_than_event_timestamp.assert_not_called()
    qb._delete_snapshot_batch_tracking.assert_not_called()
    qb._try_acquire_snapshot_cleanup_lock.assert_not_called()
    assert qb._snapshot_get_completed_batch_count.call_count == 1
    mock_rollback.assert_called_once()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.rollback_and_close")
def test_finalize_snapshot_cleanup_if_ready_raises_transient_tracker_error(mock_rollback_and_close):
    qb = AccessBundleStateUpdateQueryBuilder([])
    qb._snapshot_get_completed_batch_count = MagicMock(side_effect=oracledb.DatabaseError("transient"))
    qb._try_acquire_snapshot_cleanup_lock = MagicMock(return_value=True)
    qb._snapshot_get_earliest_batch_timestamp = MagicMock(return_value="14-Apr-26 21:40:20.331306")
    qb.delete_rows_older_than_event_timestamp = MagicMock()
    qb._delete_snapshot_batch_tracking = MagicMock()

    with pytest.raises(oracledb.DatabaseError):
        qb.finalize_snapshot_cleanup_if_ready(
            "snapshot-1",
            num_of_batches=3,
            tenancy_id="tenant-1",
            service_instance_id="svc-1",
        )

    mock_rollback_and_close.assert_called_once()
    qb.delete_rows_older_than_event_timestamp.assert_not_called()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
def test_finalize_snapshot_cleanup_if_ready_runs_delete_and_clears_tracking(mock_commit):
    qb = AccessBundleStateUpdateQueryBuilder([])
    qb._snapshot_get_completed_batch_count = MagicMock(return_value=3)
    qb._try_acquire_snapshot_cleanup_lock = MagicMock(return_value=True)
    qb._snapshot_get_earliest_batch_timestamp = MagicMock(return_value="14-Apr-26 21:40:20.331306")
    qb.delete_rows_older_than_event_timestamp = MagicMock()
    qb._delete_snapshot_batch_tracking = MagicMock()

    qb.finalize_snapshot_cleanup_if_ready(
        "snapshot-1",
        num_of_batches=3,
        tenancy_id="tenant-1",
        service_instance_id="svc-1",
    )

    qb.delete_rows_older_than_event_timestamp.assert_called_once_with(
        "14-Apr-26 21:40:20.331306",
        tenancy_id="tenant-1",
        service_instance_id="svc-1",
        commit=True,
    )
    qb._delete_snapshot_batch_tracking.assert_called_once_with(
        "snapshot-1",
        "tenant-1",
        "svc-1",
        commit=True,
    )
    mock_commit.assert_not_called()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.rollback")
def test_finalize_snapshot_cleanup_if_ready_returns_without_earliest_timestamp(mock_rollback):
    qb = AccessBundleStateUpdateQueryBuilder([])
    qb._snapshot_get_completed_batch_count = MagicMock(return_value=3)
    qb._try_acquire_snapshot_cleanup_lock = MagicMock(return_value=True)
    qb._snapshot_get_earliest_batch_timestamp = MagicMock(return_value=None)
    qb.delete_rows_older_than_event_timestamp = MagicMock()
    qb._delete_snapshot_batch_tracking = MagicMock()

    qb.finalize_snapshot_cleanup_if_ready(
        snapshot_id="snapshot-1",
        num_of_batches=3,
        tenancy_id="tenant-1",
        service_instance_id="svc-1",
    )

    qb.delete_rows_older_than_event_timestamp.assert_not_called()
    qb._delete_snapshot_batch_tracking.assert_not_called()
    mock_rollback.assert_called_once()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.rollback_and_close")
def test_finalize_snapshot_cleanup_if_ready_raises_transient_delete_error(mock_rollback_and_close):
    qb = AccessBundleStateUpdateQueryBuilder([])
    qb._snapshot_get_completed_batch_count = MagicMock(return_value=3)
    qb._try_acquire_snapshot_cleanup_lock = MagicMock(return_value=True)
    qb._snapshot_get_earliest_batch_timestamp = MagicMock(return_value="14-Apr-26 21:40:20.331306")
    qb.delete_rows_older_than_event_timestamp = MagicMock(side_effect=oracledb.DatabaseError("transient"))
    qb._delete_snapshot_batch_tracking = MagicMock()

    with pytest.raises(oracledb.DatabaseError):
        qb.finalize_snapshot_cleanup_if_ready(
            "snapshot-1",
            num_of_batches=3,
            tenancy_id="tenant-1",
            service_instance_id="svc-1",
        )

    mock_rollback_and_close.assert_called_once()
    assert qb.delete_rows_older_than_event_timestamp.call_count == 1
    qb._delete_snapshot_batch_tracking.assert_not_called()


@patch("dfa.adw.query_builders.base_query_builder.sleep")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.commit")
@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.rollback_and_close")
def test_finalize_snapshot_cleanup_if_ready_retries_deadlock_delete(
    mock_rollback_and_close,
    mock_commit,
    mock_sleep,
):
    class OracleError:
        code = 60

    qb = AccessBundleStateUpdateQueryBuilder([])
    qb._snapshot_get_completed_batch_count = MagicMock(return_value=3)
    qb._try_acquire_snapshot_cleanup_lock = MagicMock(return_value=True)
    qb._snapshot_get_earliest_batch_timestamp = MagicMock(return_value="14-Apr-26 21:40:20.331306")
    qb.delete_rows_older_than_event_timestamp = MagicMock(side_effect=[oracledb.DatabaseError(OracleError()), None])
    qb._delete_snapshot_batch_tracking = MagicMock()

    qb.finalize_snapshot_cleanup_if_ready(
        "snapshot-1",
        num_of_batches=3,
        tenancy_id="tenant-1",
        service_instance_id="svc-1",
    )

    assert qb.delete_rows_older_than_event_timestamp.call_count == 2
    assert qb._delete_snapshot_batch_tracking.call_count == 1
    mock_rollback_and_close.assert_called_once()
    mock_sleep.assert_called_once_with(qb.STALE_ROW_DELETE_RETRY_DELAY_SECONDS)
    mock_commit.assert_not_called()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.rollback")
def test_finalize_snapshot_cleanup_if_ready_defers_until_passed_num_of_batches_reached(
    mock_rollback,
):
    qb = AccessBundleStateUpdateQueryBuilder([])
    qb._snapshot_get_completed_batch_count = MagicMock(return_value=0)
    qb._try_acquire_snapshot_cleanup_lock = MagicMock()
    qb._snapshot_get_earliest_batch_timestamp = MagicMock()
    qb.delete_rows_older_than_event_timestamp = MagicMock()
    qb._delete_snapshot_batch_tracking = MagicMock()

    qb.finalize_snapshot_cleanup_if_ready(
        "snapshot-1",
        num_of_batches=1,
        tenancy_id="tenant-1",
        service_instance_id="svc-1",
    )

    qb.delete_rows_older_than_event_timestamp.assert_not_called()
    qb._delete_snapshot_batch_tracking.assert_not_called()
    assert qb._snapshot_get_completed_batch_count.call_count == 1
    mock_rollback.assert_called_once()


def test_finalize_snapshot_cleanup_if_ready_returns_immediately_without_num_of_batches():
    qb = AccessBundleStateUpdateQueryBuilder([])
    qb._snapshot_get_completed_batch_count = MagicMock()
    qb._try_acquire_snapshot_cleanup_lock = MagicMock()
    qb._snapshot_get_earliest_batch_timestamp = MagicMock()
    qb.delete_rows_older_than_event_timestamp = MagicMock()
    qb._delete_snapshot_batch_tracking = MagicMock()

    qb.finalize_snapshot_cleanup_if_ready(
        "snapshot-1",
        tenancy_id="tenant-1",
        service_instance_id="svc-1",
    )

    qb._snapshot_get_completed_batch_count.assert_not_called()
    qb._try_acquire_snapshot_cleanup_lock.assert_not_called()
    qb._snapshot_get_earliest_batch_timestamp.assert_not_called()
    qb.delete_rows_older_than_event_timestamp.assert_not_called()
    qb._delete_snapshot_batch_tracking.assert_not_called()


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.rollback")
def test_finalize_snapshot_cleanup_if_ready_returns_when_lock_busy(mock_rollback):
    qb = AccessBundleStateUpdateQueryBuilder([])
    qb._snapshot_get_completed_batch_count = MagicMock(return_value=3)
    qb._try_acquire_snapshot_cleanup_lock = MagicMock(return_value=False)
    qb._snapshot_get_earliest_batch_timestamp = MagicMock(return_value="14-Apr-26 21:40:20.331306")
    qb.delete_rows_older_than_event_timestamp = MagicMock()
    qb._delete_snapshot_batch_tracking = MagicMock()

    qb.finalize_snapshot_cleanup_if_ready(
        "snapshot-1",
        num_of_batches=3,
        tenancy_id="tenant-1",
        service_instance_id="svc-1",
    )

    qb._snapshot_get_earliest_batch_timestamp.assert_not_called()
    qb.delete_rows_older_than_event_timestamp.assert_not_called()
    qb._delete_snapshot_batch_tracking.assert_not_called()
    mock_rollback.assert_called_once()
