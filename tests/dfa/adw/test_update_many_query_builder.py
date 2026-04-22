import re
from datetime import datetime
from unittest.mock import MagicMock, patch

import oracledb
import pytest
from pypika import Table

from dfa.adw.query_builders.access_bundle import AccessBundleStateUpdateQueryBuilder
from dfa.adw.query_builders.base_query_builder import MergeManyQueryBuilder, UpdateManyQueryBuilder
from dfa.adw.query_builders.identity import IdentityStateUpdateQueryBuilder


def _normalize_sql(sql: str) -> str:
    # helper to collapse whitespace for simpler assertions
    return re.sub(r"\s+", " ", sql).strip()


def test_update_many_nullable_none_uses_equality():
    qb = Table("dummy")
    builder = UpdateManyQueryBuilder()

    events = [{"id": 1, "name": "Alice"}]
    # where on id, update should set NAME only; nullable_columns is None
    sql = builder.get_operation_sql(
        qb, events, date_columns=[], where_columns=["id"], nullable_columns=None
    )

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
    sql = builder.get_operation_sql(
        qb, events, date_columns=[], where_columns=["id"], nullable_columns=["id"]
    )

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
    sql = builder.get_operation_sql(
        qb, events, date_columns=[], where_columns=["id"], nullable_columns=["ID"]
    )

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


def test_insert_many_clob_columns_use_clob_input_size():
    input_sizes = MergeManyQueryBuilder().get_input_sizes(
        [
            {"column_name": "ID", "data_type": "VARCHAR2", "data_length": 32767},
            {"column_name": "ATTRIBUTES", "data_type": "CLOB", "data_length": None},
        ]
    )

    assert input_sizes["ID"] == 32767
    assert input_sizes["id"] == 32767
    assert input_sizes["ATTRIBUTES"] == oracledb.DB_TYPE_CLOB
    assert input_sizes["attributes"] == oracledb.DB_TYPE_CLOB


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
    mock_commit.assert_called_once()


@patch("dfa.adw.connection.AdwConnection.get_cursor")
@patch("dfa.adw.connection.AdwConnection.commit")
def test_delete_rows_older_than_event_timestamp_scopes_by_tenant_and_service(
    mock_commit, mock_get_cursor
):
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
    assert bind_values["completion_timestamp"] == "14-Apr-26 21:40:20.331306"
    assert bind_values["tenancy_id"] == "tenant-1"
    assert bind_values["service_instance_id"] == "svc-1"
    mock_commit.assert_called_once()


@patch("dfa.adw.connection.AdwConnection.get_cursor")
@patch("dfa.adw.connection.AdwConnection.commit")
def test_register_snapshot_batch_started_merges_status(mock_commit, mock_get_cursor):
    cursor = MagicMock()
    mock_get_cursor.return_value = cursor

    qb = AccessBundleStateUpdateQueryBuilder([])
    qb._snapshot_batch_tracker_table = MagicMock()
    qb._snapshot_batch_tracker_table.create = MagicMock()
    qb._snapshot_batch_tracker_table.get_schema.return_value = "DFA"
    qb._snapshot_batch_tracker_table.get_table_name.return_value = "SNAPSHOT_BATCH_TRACKER"

    qb.register_snapshot_batch_started(
        snapshot_id="snapshot-1",
        batch_id="batch-1",
        event_timestamp="14-Apr-26 21:40:20.331306",
        tenancy_id="tenant-1",
        service_instance_id="svc-1",
    )

    qb._snapshot_batch_tracker_table.create.assert_called_once()
    merge_sql = cursor.execute.call_args.args[0]
    bind_values = cursor.execute.call_args.args[1]
    normalized = _normalize_sql(merge_sql).upper()
    assert "MERGE INTO DFA.SNAPSHOT_BATCH_TRACKER" in normalized
    assert "TO_TIMESTAMP(:UPDATED_AT, 'DD-MON-RR HH24:MI:SS.FF6') AS UPDATED_AT" in normalized
    assert bind_values["entity_type"] == "ACCESS_BUNDLE"
    assert bind_values["tenancy_id"] == "tenant-1"
    assert bind_values["service_instance_id"] == "svc-1"
    assert bind_values["snapshot_id"] == "snapshot-1"
    assert bind_values["batch_id"] == "batch-1"
    assert bind_values["updated_at"] == "14-Apr-26 21:40:20.331306"
    assert bind_values["status"] == "STARTED"
    assert mock_commit.call_count == 2


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
    assert first_bind["entity_type"] == "ACCESS_BUNDLE"
    assert first_bind["tenancy_id"] == "tenant-1"
    assert first_bind["service_instance_id"] == "svc-1"
    assert first_bind["snapshot_id"] == "snapshot-1"


@patch("dfa.adw.connection.AdwConnection.get_cursor")
def test_snapshot_get_earliest_batch_timestamp_uses_scope_and_formats_datetime(mock_get_cursor):
    cursor = MagicMock()
    mock_get_cursor.return_value = cursor
    cursor.fetchone.side_effect = [(datetime(2026, 4, 14, 21, 40, 20, 331306),)]

    qb = AccessBundleStateUpdateQueryBuilder([])
    qb._snapshot_batch_tracker_table = MagicMock()
    qb._snapshot_batch_tracker_table.get_schema.return_value = "DFA"
    qb._snapshot_batch_tracker_table.get_table_name.return_value = "SNAPSHOT_BATCH_TRACKER"

    earliest_timestamp = qb._snapshot_get_earliest_batch_timestamp(
        "snapshot-1", "tenant-1", "svc-1"
    )

    assert earliest_timestamp == "14-Apr-26 21:40:20.331306"
    first_bind = cursor.execute.call_args_list[0].args[1]
    assert first_bind["entity_type"] == "ACCESS_BUNDLE"
    assert first_bind["tenancy_id"] == "tenant-1"
    assert first_bind["service_instance_id"] == "svc-1"
    assert first_bind["snapshot_id"] == "snapshot-1"


def test_finalize_snapshot_cleanup_if_ready_returns_when_completed_count_not_reached():
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


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.close")
def test_finalize_snapshot_cleanup_if_ready_raises_transient_tracker_error(mock_close):
    qb = AccessBundleStateUpdateQueryBuilder([])
    qb._snapshot_get_completed_batch_count = MagicMock(
        side_effect=oracledb.DatabaseError("transient")
    )
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

    mock_close.assert_called_once()
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
        commit=False,
    )
    qb._delete_snapshot_batch_tracking.assert_called_once_with(
        "snapshot-1",
        "tenant-1",
        "svc-1",
        commit=False,
    )
    mock_commit.assert_called_once()


def test_finalize_snapshot_cleanup_if_ready_returns_without_earliest_timestamp():
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


@patch("dfa.adw.query_builders.base_query_builder.AdwConnection.close")
def test_finalize_snapshot_cleanup_if_ready_raises_transient_delete_error(mock_close):
    qb = AccessBundleStateUpdateQueryBuilder([])
    qb._snapshot_get_completed_batch_count = MagicMock(return_value=3)
    qb._try_acquire_snapshot_cleanup_lock = MagicMock(return_value=True)
    qb._snapshot_get_earliest_batch_timestamp = MagicMock(return_value="14-Apr-26 21:40:20.331306")
    qb.delete_rows_older_than_event_timestamp = MagicMock(
        side_effect=oracledb.DatabaseError("transient")
    )
    qb._delete_snapshot_batch_tracking = MagicMock()

    with pytest.raises(oracledb.DatabaseError):
        qb.finalize_snapshot_cleanup_if_ready(
            "snapshot-1",
            num_of_batches=3,
            tenancy_id="tenant-1",
            service_instance_id="svc-1",
        )

    mock_close.assert_called_once()
    assert qb.delete_rows_older_than_event_timestamp.call_count == 1
    qb._delete_snapshot_batch_tracking.assert_not_called()


def test_finalize_snapshot_cleanup_if_ready_defers_until_passed_num_of_batches_reached():
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


def test_finalize_snapshot_cleanup_if_ready_returns_when_lock_busy():
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
