import re

from pypika import Table

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
