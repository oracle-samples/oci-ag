import re
from unittest.mock import MagicMock, patch

from pypika import Table

from dfa.adw.query_builders.access_bundle import AccessBundleStateUpdateQueryBuilder
from dfa.adw.query_builders.base_query_builder import UpdateManyQueryBuilder


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
    normalized = _normalize_sql(executed_sql).upper()
    assert 'DELETE FROM "ACCESS_BUNDLE_STATE"' in normalized
    assert '"EVENT_TIMESTAMP"<' in normalized
    assert '"TENANCY_ID"=' in normalized
    assert '"SERVICE_INSTANCE_ID"=' in normalized
    mock_commit.assert_called_once()
