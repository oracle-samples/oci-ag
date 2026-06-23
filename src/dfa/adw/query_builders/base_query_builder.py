# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
# pylint: disable=too-many-lines

import importlib.util
import inspect
import os
import re
from datetime import datetime, timedelta, timezone
from functools import lru_cache, wraps
from pathlib import Path
from time import perf_counter, sleep
from typing import Any, Optional, cast

import oracledb
from pypika import CustomFunction, Order, Parameter, Query, Table
from pypika.functions import ToDate

from common.logger.logger import Logger
from dfa.adw.connection import AdwConnection
from dfa.adw.tables.base_table import SnapshotBatchTrackerTable, StreamOffsetTrackerTable


class InsertManyQueryBuilder:
    def get_operation_sql(self, query_builder, events, date_columns):
        event = events[0]

        insert_column_list = []
        insert_parameters = []
        for event_group_column_name in event.keys():
            if event_group_column_name in date_columns:
                continue
            bind_name = event_group_column_name.upper()
            insert_column_list.append(bind_name)
            insert_parameters.append(Parameter(f":{bind_name}"))
        parameter_set = tuple(insert_parameters)

        insert_sql = Query.into(query_builder).insert(*parameter_set).get_sql()

        table_name = query_builder.table_manager.get_table_name().upper()
        column_list_str = ", ".join(insert_column_list)
        insert_sql = insert_sql.replace(f'"{table_name}"', f'"{table_name}" ({column_list_str}) ')
        return insert_sql


class UpdateManyQueryBuilder:
    def get_operation_sql(
        self,
        query_builder: Any,
        events: list[dict[str, Any]],
        date_columns: list[str],
        where_columns: list[str],
        nullable_columns: list[str] | None = None,
    ) -> str | None:
        event = events[0]
        update_sql: Any = None
        where_columns = [col.lower() for col in where_columns]
        date_columns = [col.lower() for col in date_columns]
        nullable_columns = [col.lower() for col in (nullable_columns or [])]
        for column_name, _ in event.items():
            if column_name.lower() in where_columns:
                continue
            if column_name.lower() in date_columns:
                continue
            bind_name = column_name.upper()
            if update_sql is None:
                update_sql = Query.update(query_builder).set(bind_name, Parameter(f":{bind_name}"))
            else:
                update_sql = update_sql.set(bind_name, Parameter(f":{bind_name}"))

        if update_sql is None:
            return None

        # Use Oracle DECODE for NULL-safe equality on executemany binds
        decode = CustomFunction("DECODE", ["expr1", "expr2", "ret_equal", "ret_not_equal"])
        for where_column_name in where_columns:
            bind_name = where_column_name.upper()
            column = getattr(query_builder, bind_name)
            param = Parameter(f":{bind_name}")
            if where_column_name in nullable_columns:
                # For nullable columns, treat NULL = NULL as a match using DECODE
                update_sql = update_sql.where(decode(column, param, 1, 0) == 1)
            else:
                # For non-nullable columns, simple equality is sufficient
                update_sql = update_sql.where(column == param)

        complete_update_stmt = update_sql.get_sql()

        return complete_update_stmt


class MergeManyQueryBuilder:
    """
    Builds an Oracle MERGE (upsert) statement suitable for executemany with dict bindings.

    Behavior:
    - WHEN MATCHED: updates all non-key, non-date columns using incoming values
    - WHEN NOT MATCHED: inserts all columns using incoming values
    - The ON condition uses the provided where_columns (unique/primary key columns)
    - date_columns are excluded from SET (caller can manage them if needed)
    """

    # pylint: disable=too-many-locals
    def get_operation_sql(
        self,
        query_builder,
        events: list[dict[str, Any]],
        date_columns: list[str],
        where_columns: list[str],
        nullable_columns: list[str] | None = None,
    ) -> str:
        assert len(events) > 0, "events cannot be empty for MERGE"

        example = events[0]
        all_cols = list(example.keys())
        key_cols = [c.lower() for c in where_columns]
        date_cols = [c.lower() for c in date_columns]
        nullable_cols = [c.lower() for c in (nullable_columns or [])]

        # Columns used in UPDATE SET (exclude keys and date columns)
        updatable_cols = [c for c in all_cols if c.lower() not in key_cols and c.lower() not in date_cols]

        # Qualify table name with schema
        schema = query_builder.table_manager.get_schema().upper()
        table = query_builder.table_manager.get_table_name().upper()
        qualified_table = f'"{schema}"."{table}"'

        # USING subquery with bind variables once per executemany row
        select_bind_parts = []
        for col in all_cols:
            bind_name = col.upper()
            select_bind_parts.append(f':{bind_name} AS "{bind_name}"')
        using_subquery = f"SELECT {', '.join(select_bind_parts)} FROM DUAL"

        # Build ON clause with composite keys if needed
        on_conditions = []
        for k in where_columns:
            if k.lower() in nullable_cols:
                on_conditions.append(f'DECODE(t."{k.upper()}", s."{k.upper()}", 1, 0) = 1')
            else:
                on_conditions.append(f't."{k.upper()}" = s."{k.upper()}"')
        on_clause = " AND ".join(on_conditions)

        # Build UPDATE SET list
        if len(updatable_cols) > 0:
            set_parts = [f't."{c.upper()}" = s."{c.upper()}"' for c in updatable_cols]
            update_clause = " WHEN MATCHED THEN UPDATE SET " + ", ".join(set_parts)
        else:
            # If nothing to update, skip UPDATE branch
            update_clause = ""

        # Build INSERT columns and values
        insert_cols = ", ".join([f'"{c.upper()}"' for c in all_cols])
        insert_vals = ", ".join([f's."{c.upper()}"' for c in all_cols])
        insert_clause = f" WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"

        merge_sql = (
            f"MERGE INTO {qualified_table} t "
            f"USING ({using_subquery}) s ON ({on_clause})"
            f"{update_clause}"
            f"{insert_clause}"
        )

        return merge_sql


class DeleteManyQueryBuilder:
    def get_operation_sql(
        self,
        query_builder: Any,
        where_columns: list[str],
        nullable_columns: list[str] | None = None,
    ) -> str:
        delete_sql: Any = Query.from_(query_builder).delete()
        where_columns = [col.lower() for col in where_columns]
        nullable_columns = [col.lower() for col in (nullable_columns or [])]

        decode = CustomFunction("DECODE", ["expr1", "expr2", "ret_equal", "ret_not_equal"])
        for where_column_name in where_columns:
            bind_name = where_column_name.upper()
            column = getattr(query_builder, bind_name)
            param = Parameter(f":{bind_name}")
            if where_column_name in nullable_columns:
                delete_sql = delete_sql.where(decode(column, param, 1, 0) == 1)
            else:
                delete_sql = delete_sql.where(column == param)

        return delete_sql.get_sql()


class StreamOffsetTrackerQueryBuilder(Table):
    table_manager = StreamOffsetTrackerTable()

    def __init__(self):
        super().__init__(self.table_manager.get_table_name().upper())

    def get_statement_for_select_max_offset_for_transformer(self, transformer):
        statement = (
            Query.from_(self)
            .select("END_OFFSET")
            .where(getattr(self, "APPLICATION") == transformer)
            .orderby("ID", order=Order.desc)
            .get_sql()
        )
        statement += " FETCH FIRST 1 ROW ONLY"

        return statement

    def get_insert_statement_for_stream_offset(self, offset, end_offset, application):
        offset_data = (offset, end_offset, application)
        statement = Query.into(self).columns("OFFSET", "END_OFFSET", "APPLICATION").insert(*offset_data).get_sql()
        return statement

    def get_statement_for_latest_unfinished_stream_offset_range(self, application):
        start_date = datetime.now()
        start_date = start_date.astimezone(timezone.utc)
        start_date = start_date - timedelta(hours=6)

        select_sql = (
            Query.from_(self)
            .select(self.ID, self.OFFSET, self.END_OFFSET)
            .where(self.END_DATE.isnull())
            .where(self.APPLICATION == application)
            .where(self.START_DATE < ToDate(start_date.strftime("%d-%b-%y %H:%M:%S"), "DD-MON-RR HH24:MI:SS"))
            .orderby("ID", order=Order.desc)
            .get_sql()
        )

        return select_sql

    def get_statement_for_offset_range_completion(self, offset, end_offset, application):
        end_date_now = datetime.now()
        end_date_now = end_date_now.astimezone(timezone.utc)
        statement = (
            Query.update(self)
            .set(
                self.END_DATE,
                ToDate(end_date_now.strftime("%d-%b-%y %H:%M:%S"), "DD-MON-RR HH24:MI:SS"),
            )
            .where(self.OFFSET == offset)
            .where(self.END_OFFSET == end_offset)
            .where(self.APPLICATION == application)
            .get_sql()
        )

        return statement


class BaseQueryBuilder:
    logger = Logger(__name__).get_logger()
    events: Optional[list[Any]] = None
    table_manager: Any = None
    MAX_DIRECT_STRING_BIND_SIZE = 32767
    MAX_DIRECT_CLOB_STRING_BIND_SIZE = 32767
    STALE_ROW_DELETE_MAX_ATTEMPTS = 3
    STALE_ROW_DELETE_RETRY_DELAY_SECONDS = 30
    _snapshot_batch_tracker_table = SnapshotBatchTrackerTable()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        BaseQueryBuilder._wrap_execute_sql_for_events(cls)

    @staticmethod
    def _wrap_execute_sql_for_events(subclass):
        method = subclass.__dict__.get("execute_sql_for_events")
        if not callable(method) or getattr(method, "__dfa_timed_execute_sql_for_events__", False):
            return

        @wraps(method)
        def _timed_execute_sql_for_events(self, *args, **kw):
            start_time = perf_counter()
            try:
                return method(self, *args, **kw)
            finally:
                duration = perf_counter() - start_time
                try:
                    table_name = self.table_manager.get_table_name().lower()
                except Exception:
                    table_name = self.__class__.__name__

                events = getattr(self, "events", None) or []
                try:
                    event_count = len(events)
                except TypeError:
                    event_count = 0

                try:
                    self.logger.info(
                        "%s execute_sql_for_events runtime: %.3fs for %d event(s)",
                        table_name,
                        duration,
                        event_count,
                    )
                except Exception:
                    pass

        setattr(_timed_execute_sql_for_events, "__dfa_timed_execute_sql_for_events__", True)
        if getattr(method, "__isabstractmethod__", False):
            _timed_execute_sql_for_events.__isabstractmethod__ = True
        setattr(subclass, "execute_sql_for_events", _timed_execute_sql_for_events)

    def _table(self) -> Table:
        return cast(Table, self)

    @staticmethod
    def _get_event_value(event: dict[str, Any], column_name: str):
        if column_name in event:
            return event[column_name]
        return event.get(column_name.lower())

    def get_input_sizes_for_events(
        self,
        columns_definition: list[dict[str, Any]],
        events: list[dict[str, Any]],
    ):
        input_sizes = {}
        for column in columns_definition:
            column_name = column["column_name"]
            data_type = column["data_type"].upper()

            if data_type.startswith("VARCHAR") or data_type == "CLOB":
                max_value_length = max(
                    (
                        len(str(value))
                        for event in events
                        for value in (self._get_event_value(event, column_name),)
                        if value is not None
                    ),
                    default=1,
                )
                if data_type == "CLOB" and max_value_length > self.MAX_DIRECT_CLOB_STRING_BIND_SIZE:
                    bind_size = oracledb.DB_TYPE_CLOB
                else:
                    max_column_length = column["data_length"] or self.MAX_DIRECT_STRING_BIND_SIZE
                    bind_size = max(1, min(max_value_length, max_column_length))
            elif data_type == "NUMBER":
                bind_size = oracledb.NUMBER
            else:
                bind_size = None

            input_sizes[column_name] = bind_size

        return input_sizes

    @staticmethod
    def _get_bind_names_for_sql(sql: str) -> set[str]:
        return set(re.findall(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)", sql))

    @classmethod
    def _filter_input_sizes_for_sql(
        cls,
        input_sizes: dict[str, Any],
        sql: str,
    ) -> dict[str, Any]:
        bind_names = cls._get_bind_names_for_sql(sql)
        return {name: size for name, size in input_sizes.items() if name in bind_names}

    @staticmethod
    def _uppercase_bind_rows(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [{name.upper(): value for name, value in event.items()} for event in events]

    @classmethod
    def _bind_rows_for_sql(
        cls,
        events: list[dict[str, Any]],
        sql: str,
    ) -> list[dict[str, Any]]:
        bind_names = cls._get_bind_names_for_sql(sql)
        return [
            {name: value for name, value in row.items() if name in bind_names}
            for row in cls._uppercase_bind_rows(events)
        ]

    @staticmethod
    def _normalize_cleanup_timestamp(completion_timestamp: str) -> str:
        for timestamp_format in ("%d-%b-%y %H:%M:%S.%f", "%d-%b-%y %I:%M:%S.%f %p"):
            try:
                parsed_timestamp = datetime.strptime(completion_timestamp, timestamp_format)
                return parsed_timestamp.strftime("%d-%b-%y %H:%M:%S.%f")
            except ValueError:
                continue

        raise ValueError(f"Unsupported cleanup timestamp format: {completion_timestamp}")

    def _get_cleanup_scope_values(
        self, tenancy_id: str | None = None, service_instance_id: str | None = None
    ) -> dict[str, str]:
        entity_type = self.table_manager.get_table_name().upper()
        if entity_type.endswith("_STATE"):
            entity_type = entity_type[: -len("_STATE")]
        elif entity_type.endswith("_TS"):
            entity_type = entity_type[: -len("_TS")]
        return {
            "ENTITY_TYPE": entity_type,
            "TENANCY_ID": tenancy_id or "-",
            "SERVICE_INSTANCE_ID": service_instance_id or "-",
        }

    def _ensure_helper_table_exists(self, table_manager):
        try:
            table_manager.create()
            if hasattr(table_manager, "ensure_supporting_objects"):
                table_manager.ensure_supporting_objects()
            AdwConnection.commit()
        except oracledb.DatabaseError as exc:
            error = exc.args[0] if exc.args else None
            if getattr(error, "code", None) != 955:
                raise
            AdwConnection.close()

    @staticmethod
    def _is_retryable_cleanup_error(exc: Exception) -> bool:
        return isinstance(exc, (oracledb.DatabaseError, oracledb.InterfaceError))

    @staticmethod
    def _get_database_error_code(exc: Exception) -> int | None:
        error = exc.args[0] if exc.args else None
        return getattr(error, "code", None)

    def _insert_snapshot_batch_completed(
        self,
        snapshot_id: str,
        batch_id: str,
        event_timestamp: str,
        tenancy_id: str | None = None,
        service_instance_id: str | None = None,
    ):
        tracker_table = self._snapshot_batch_tracker_table
        self._ensure_helper_table_exists(tracker_table)

        insert_sql = f"""
            INSERT INTO {tracker_table.get_schema()}.{tracker_table.get_table_name()} (
                ENTITY_TYPE,
                TENANCY_ID,
                SERVICE_INSTANCE_ID,
                SNAPSHOT_ID,
                BATCH_ID,
                UPDATED_AT
            ) VALUES (
                :ENTITY_TYPE,
                :TENANCY_ID,
                :SERVICE_INSTANCE_ID,
                :SNAPSHOT_ID,
                :BATCH_ID,
                TO_TIMESTAMP(:UPDATED_AT, 'DD-MON-RR HH24:MI:SS.FF6')
            )
        """
        bind_values = {
            **self._get_cleanup_scope_values(tenancy_id, service_instance_id),
            "SNAPSHOT_ID": snapshot_id,
            "BATCH_ID": batch_id,
            "UPDATED_AT": event_timestamp,
        }
        try:
            AdwConnection.get_cursor().execute(
                insert_sql,
                bind_values,
            )
            AdwConnection.commit()
        except Exception as e:
            AdwConnection.rollback()
            self.logger.warning(
                "Failed to insert snapshot batch completion for snapshot_id=%s, batch_id=%s: %s",
                snapshot_id,
                batch_id,
                e,
            )

    def register_snapshot_batch_completed(
        self,
        snapshot_id: str,
        batch_id: str,
        event_timestamp: str,
        tenancy_id: str | None = None,
        service_instance_id: str | None = None,
    ):
        self._insert_snapshot_batch_completed(
            snapshot_id=snapshot_id,
            batch_id=batch_id,
            event_timestamp=event_timestamp,
            tenancy_id=tenancy_id,
            service_instance_id=service_instance_id,
        )

    def _snapshot_get_completed_batch_count(
        self,
        snapshot_id: str,
        tenancy_id: str | None = None,
        service_instance_id: str | None = None,
    ) -> int:
        tracker_table = self._snapshot_batch_tracker_table
        query_sql = f"""
            SELECT COUNT(*)
            FROM {tracker_table.get_schema()}.{tracker_table.get_table_name()}
            WHERE ENTITY_TYPE = :ENTITY_TYPE
              AND TENANCY_ID = :TENANCY_ID
              AND SERVICE_INSTANCE_ID = :SERVICE_INSTANCE_ID
              AND SNAPSHOT_ID = :SNAPSHOT_ID
        """
        AdwConnection.get_cursor().execute(
            query_sql,
            {
                **self._get_cleanup_scope_values(tenancy_id, service_instance_id),
                "SNAPSHOT_ID": snapshot_id,
            },
        )
        return AdwConnection.get_cursor().fetchone()[0]

    def _snapshot_get_earliest_batch_timestamp(
        self,
        snapshot_id: str,
        tenancy_id: str | None = None,
        service_instance_id: str | None = None,
    ) -> str | None:
        tracker_table = self._snapshot_batch_tracker_table
        query_sql = f"""
            SELECT MIN(UPDATED_AT)
            FROM {tracker_table.get_schema()}.{tracker_table.get_table_name()}
            WHERE ENTITY_TYPE = :ENTITY_TYPE
              AND TENANCY_ID = :TENANCY_ID
              AND SERVICE_INSTANCE_ID = :SERVICE_INSTANCE_ID
              AND SNAPSHOT_ID = :SNAPSHOT_ID
        """
        AdwConnection.get_cursor().execute(
            query_sql,
            {
                **self._get_cleanup_scope_values(tenancy_id, service_instance_id),
                "SNAPSHOT_ID": snapshot_id,
            },
        )
        earliest_updated_at = AdwConnection.get_cursor().fetchone()[0]
        if earliest_updated_at is None:
            return None

        if isinstance(earliest_updated_at, datetime):
            return earliest_updated_at.strftime("%d-%b-%y %H:%M:%S.%f")

        return str(earliest_updated_at)

    def _try_acquire_snapshot_cleanup_lock(
        self,
        snapshot_id: str,
        tenancy_id: str | None = None,
        service_instance_id: str | None = None,
    ) -> bool:
        tracker_table = self._snapshot_batch_tracker_table
        lock_sql = f"""
            SELECT BATCH_ID
            FROM {tracker_table.get_schema()}.{tracker_table.get_table_name()}
            WHERE ENTITY_TYPE = :ENTITY_TYPE
              AND TENANCY_ID = :TENANCY_ID
              AND SERVICE_INSTANCE_ID = :SERVICE_INSTANCE_ID
              AND SNAPSHOT_ID = :SNAPSHOT_ID
              AND BATCH_ID = (
                  SELECT MIN(BATCH_ID)
                  FROM {tracker_table.get_schema()}.{tracker_table.get_table_name()}
                  WHERE ENTITY_TYPE = :ENTITY_TYPE
                    AND TENANCY_ID = :TENANCY_ID
                    AND SERVICE_INSTANCE_ID = :SERVICE_INSTANCE_ID
                    AND SNAPSHOT_ID = :SNAPSHOT_ID
              )
            FOR UPDATE NOWAIT
        """
        try:
            AdwConnection.get_cursor().execute(
                lock_sql,
                {
                    **self._get_cleanup_scope_values(tenancy_id, service_instance_id),
                    "SNAPSHOT_ID": snapshot_id,
                },
            )
            return AdwConnection.get_cursor().fetchone() is not None
        except oracledb.DatabaseError as exc:
            if self._get_database_error_code(exc) == 54:
                return False
            raise

    def _delete_snapshot_batch_tracking(
        self,
        snapshot_id: str,
        tenancy_id: str | None = None,
        service_instance_id: str | None = None,
        commit: bool = True,
    ):
        tracker_table = self._snapshot_batch_tracker_table
        delete_sql = f"""
            DELETE FROM {tracker_table.get_schema()}.{tracker_table.get_table_name()}
            WHERE ENTITY_TYPE = :ENTITY_TYPE
              AND TENANCY_ID = :TENANCY_ID
              AND SERVICE_INSTANCE_ID = :SERVICE_INSTANCE_ID
              AND SNAPSHOT_ID = :SNAPSHOT_ID
        """
        AdwConnection.get_cursor().execute(
            delete_sql,
            {
                **self._get_cleanup_scope_values(tenancy_id, service_instance_id),
                "SNAPSHOT_ID": snapshot_id,
            },
        )
        if commit:
            AdwConnection.commit()
        self.logger.info(
            "Deleted snapshot tracker rows for %s snapshot %s",
            self.table_manager.get_table_name().lower(),
            snapshot_id,
        )

    def finalize_snapshot_cleanup_if_ready(
        self,
        snapshot_id: str,
        num_of_batches: int | None = None,
        tenancy_id: str | None = None,
        service_instance_id: str | None = None,
    ):
        if num_of_batches is None:
            return

        # The completion marker header's numOfBatches is the source of truth for
        # how many snapshot batches must finish before cleanup can run. The
        # tracker table is only used to count completed batches for that
        # snapshot, not to derive the expected total.
        required_batch_count = num_of_batches

        for attempt in range(1, self.STALE_ROW_DELETE_MAX_ATTEMPTS + 1):
            try:
                completed_batch_count = self._snapshot_get_completed_batch_count(
                    snapshot_id,
                    tenancy_id,
                    service_instance_id,
                )

                if completed_batch_count < required_batch_count:
                    self.logger.info(
                        "Deferring stale row cleanup for %s snapshot %s; completed %d/%d batches",
                        self.table_manager.get_table_name().lower(),
                        snapshot_id,
                        completed_batch_count,
                        required_batch_count,
                    )
                    AdwConnection.rollback()
                    return

                if not self._try_acquire_snapshot_cleanup_lock(
                    snapshot_id,
                    tenancy_id,
                    service_instance_id,
                ):
                    self.logger.info(
                        "Deferring stale row cleanup for %s snapshot %s; cleanup already in progress",
                        self.table_manager.get_table_name().lower(),
                        snapshot_id,
                    )
                    AdwConnection.rollback()
                    return

                completion_timestamp = self._snapshot_get_earliest_batch_timestamp(
                    snapshot_id,
                    tenancy_id,
                    service_instance_id,
                )
                if completion_timestamp is None:
                    AdwConnection.rollback()
                    return
                self.delete_rows_older_than_event_timestamp(
                    completion_timestamp,
                    tenancy_id=tenancy_id,
                    service_instance_id=service_instance_id,
                    commit=True,
                )
                self._delete_snapshot_batch_tracking(snapshot_id, tenancy_id, service_instance_id, commit=True)
                return
            except Exception as exc:
                if self._is_retryable_cleanup_error(exc):
                    AdwConnection.rollback_and_close()
                    if self._get_database_error_code(exc) == 60 and attempt < self.STALE_ROW_DELETE_MAX_ATTEMPTS:
                        retry_message = (
                            "Retrying stale row cleanup for %s snapshot %s after ORA-00060 deadlock (attempt %d/%d)"
                        )
                        self.logger.warning(
                            retry_message,
                            self.table_manager.get_table_name().lower(),
                            snapshot_id,
                            attempt,
                            self.STALE_ROW_DELETE_MAX_ATTEMPTS,
                        )
                        sleep(self.STALE_ROW_DELETE_RETRY_DELAY_SECONDS)
                        continue
                else:
                    AdwConnection.rollback()
                raise

    def delete_rows_older_than_event_timestamp(
        self,
        completion_timestamp: str,
        tenancy_id: str | None = None,
        service_instance_id: str | None = None,
        commit: bool = True,
    ):
        self.logger.info(
            "Removing stale rows from %s older than %s",
            self.table_manager.get_table_name().lower(),
            completion_timestamp,
        )

        normalized_completion_timestamp = self._normalize_cleanup_timestamp(completion_timestamp)
        delete_sql = (
            f'DELETE FROM "{self.table_manager.get_table_name()}" '
            'WHERE "EVENT_TIMESTAMP" < '
            "TO_TIMESTAMP(:COMPLETION_TIMESTAMP, 'DD-MON-RR HH24:MI:SS.FF6')"
        )
        bind_values: dict[str, str] = {"COMPLETION_TIMESTAMP": normalized_completion_timestamp}
        if tenancy_id is not None:
            delete_sql += ' AND "TENANCY_ID" = :TENANCY_ID'
            bind_values["TENANCY_ID"] = tenancy_id

        if service_instance_id is not None:
            delete_sql += ' AND "SERVICE_INSTANCE_ID" = :SERVICE_INSTANCE_ID'
            bind_values["SERVICE_INSTANCE_ID"] = service_instance_id

        AdwConnection.get_cursor().execute(delete_sql, bind_values)
        if commit:
            AdwConnection.commit()

    def executemany_sql_for_events(self):
        self.logger.info(
            "Using bulk insert into time series table (%s) for %d events",
            self.table_manager.get_table_name().lower(),
            len(self.events),
        )

        if len(self.events) == 0:
            self.logger.info(
                "No events to process by %s time series query builder",
                self.table_manager.get_table_name().lower(),
            )
            return

        insert_statement = InsertManyQueryBuilder().get_operation_sql(self, self.events, [])
        input_sizes = self.get_input_sizes_for_events(
            self.table_manager.get_column_list_definition_for_table_ddl(),
            self.events,
        )
        input_sizes = self._filter_input_sizes_for_sql(input_sizes, insert_statement)
        AdwConnection.get_cursor().setinputsizes(**input_sizes)
        AdwConnection.get_cursor().executemany(
            insert_statement,
            self._bind_rows_for_sql(self.events, insert_statement),
            batcherrors=True,
        )

        batch_errors = list(AdwConnection.get_cursor().getbatcherrors())
        if batch_errors:
            self.logger.warning(
                "%s time series inserts encountered %d batch error(s)",
                self.table_manager.get_table_name().lower(),
                len(batch_errors),
            )
            # Log top-N distinct messages for signal; avoid full spam
            seen = set()
            top_msgs = []
            for be in batch_errors:
                msg = getattr(be, "message", str(be))
                if msg not in seen:
                    seen.add(msg)
                    top_msgs.append(msg)
                if len(top_msgs) >= 5:
                    break
            for m in top_msgs:
                self.logger.warning("batch error: %s", m)

        AdwConnection.commit()

    # pylint: disable=too-many-locals
    def executemany_merge_for_events(
        self,
        where_columns: list[str],
        date_columns: list[str] | None = None,
        nullable_columns: list[str] | None = None,
        events: list[dict[str, Any]] | None = None,
    ):
        """
        Generic helper to perform MERGE (upsert) for current events using the
        provided unique key columns. Intended for state tables
        """
        date_columns = date_columns or []

        active_events = self.events if events is None else events
        if not active_events or len(active_events) == 0:
            self.logger.info(
                "No events to process by %s merge query builder",
                self.table_manager.get_table_name().lower(),
            )
            return

        self.logger.info(
            "Using MERGE into %s for %d events (keys: %s)",
            self.table_manager.get_table_name().lower(),
            len(active_events),
            ",".join([c.lower() for c in where_columns]),
        )

        merge_sql = MergeManyQueryBuilder().get_operation_sql(
            self, active_events, date_columns, where_columns, nullable_columns
        )

        AdwConnection.get_cursor().execute("ALTER SESSION DISABLE PARALLEL DML")

        input_sizes = self.get_input_sizes_for_events(
            self.table_manager.get_column_list_definition_for_table_ddl(),
            active_events,
        )
        input_sizes = self._filter_input_sizes_for_sql(input_sizes, merge_sql)
        AdwConnection.get_cursor().setinputsizes(**input_sizes)
        AdwConnection.get_cursor().executemany(
            merge_sql,
            self._bind_rows_for_sql(active_events, merge_sql),
            batcherrors=True,
        )

        # MERGE is not immune to concurrent insert races. Two workers can both evaluate
        # “not matched” at about the same time. One inserts first; the other then tries its
        # insert branch and gets ORA-00001. At that point the row exists,
        # so retrying as UPDATE is a reasonable recovery.
        batch_errors = list(AdwConnection.get_cursor().getbatcherrors())
        if batch_errors:
            constraint_violating_rows = []
            other_batch_errors = []
            for be in batch_errors:
                if getattr(be, "full_code", None) == "ORA-00001":
                    constraint_violating_rows.append(active_events[be.offset])
                else:
                    other_batch_errors.append(be)

            if other_batch_errors:
                self.logger.warning(
                    "%s merge encountered %d unhandled batch error(s)",
                    self.table_manager.get_table_name().lower(),
                    len(other_batch_errors),
                )
                seen = set()
                top_msgs = []
                for be in other_batch_errors:
                    msg = getattr(be, "message", str(be))
                    if msg not in seen:
                        seen.add(msg)
                        top_msgs.append(msg)
                    if len(top_msgs) >= 5:
                        break
                for m in top_msgs:
                    self.logger.warning("batch error: %s", m)

            if constraint_violating_rows:
                AdwConnection.commit()
                self.logger.info(
                    "%d %s merge row(s) hit unique constraint races; retrying as bulk updates",
                    len(constraint_violating_rows),
                    self.table_manager.get_table_name().lower(),
                )
                update_sql = UpdateManyQueryBuilder().get_operation_sql(
                    self,
                    constraint_violating_rows,
                    date_columns,
                    where_columns,
                    nullable_columns,
                )
                if update_sql is not None:
                    update_input_sizes = self._filter_input_sizes_for_sql(input_sizes, update_sql)
                    AdwConnection.get_cursor().setinputsizes(**update_input_sizes)
                    AdwConnection.get_cursor().executemany(
                        update_sql,
                        self._bind_rows_for_sql(constraint_violating_rows, update_sql),
                        batcherrors=True,
                    )
                    update_batch_errors = list(AdwConnection.get_cursor().getbatcherrors())
                    for batch_error in update_batch_errors[:5]:
                        self.logger.warning(
                            "%s update retry failed - %s",
                            self.table_manager.get_table_name().lower(),
                            getattr(batch_error, "message", str(batch_error)),
                        )

        AdwConnection.commit()

    def executemany_state_merge_for_events(
        self,
        events: list[dict[str, Any]] | None = None,
    ):
        constraint_details = self.table_manager.get_unique_contraint_definition_details()
        nullable_columns = self.table_manager.get_nullable_constraint_columns()
        return self.executemany_merge_for_events(
            where_columns=constraint_details["columns"],
            date_columns=[],
            nullable_columns=nullable_columns,
            events=events,
        )

    def executemany_delete_for_events(
        self,
        where_columns: list[str],
        events: list[dict[str, Any]] | None = None,
        nullable_columns: list[str] | None = None,
    ):
        active_events = self.events if events is None else events
        if not active_events or len(active_events) == 0:
            self.logger.info(
                "No events to process by %s delete query builder",
                self.table_manager.get_table_name().lower(),
            )
            return

        self.logger.info(
            "Using bulk delete from %s for %d events (keys: %s)",
            self.table_manager.get_table_name().lower(),
            len(active_events),
            ",".join([c.lower() for c in where_columns]),
        )
        if hasattr(self.table_manager, "ensure_delete_indexes"):
            self.table_manager.ensure_delete_indexes()

        delete_sql = DeleteManyQueryBuilder().get_operation_sql(
            self,
            where_columns,
            nullable_columns,
        )
        input_sizes = self.get_input_sizes_for_events(
            self.table_manager.get_column_list_definition_for_table_ddl(),
            active_events,
        )
        input_sizes = self._filter_input_sizes_for_sql(input_sizes, delete_sql)
        AdwConnection.get_cursor().setinputsizes(**input_sizes)
        AdwConnection.get_cursor().executemany(
            delete_sql,
            self._bind_rows_for_sql(active_events, delete_sql),
            batcherrors=True,
        )

        batch_errors = list(AdwConnection.get_cursor().getbatcherrors())
        if batch_errors:
            self.logger.warning(
                "%s delete encountered %d batch error(s)",
                self.table_manager.get_table_name().lower(),
                len(batch_errors),
            )
            seen = set()
            top_msgs = []
            for be in batch_errors:
                msg = getattr(be, "message", str(be))
                if msg not in seen:
                    seen.add(msg)
                    top_msgs.append(msg)
                if len(top_msgs) >= 5:
                    break
            for m in top_msgs:
                self.logger.warning("batch error: %s", m)

        AdwConnection.commit()


@lru_cache(maxsize=None)
def _resolve_query_builder_class(event_object_type, operation, is_timeseries):
    if is_timeseries:
        class_name = f"{event_object_type.lower().title().replace('_', '')}\
TimeSeries{operation.lower().title().replace('_', '')}QueryBuilder"
    else:
        class_name = f"{event_object_type.lower().title().replace('_', '')}\
State{operation.lower().title().replace('_', '')}QueryBuilder"

    query_builders = Path(__file__).parent
    for file in os.listdir(query_builders):
        full_path = os.path.join(query_builders, file)
        if os.path.isfile(full_path) and file.endswith(".py") and not file.startswith("__"):
            module_name = file[:-3]
            if module_name.lower() == event_object_type.lower():
                spec = importlib.util.spec_from_file_location(module_name, full_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                for cls_name, cls_obj in inspect.getmembers(module):
                    if inspect.isclass(cls_obj) and cls_name == class_name:
                        return cls_obj
    return None


def get_query_builder(event_object_type, operation, events, is_timeseries=False):
    logger = Logger(__name__).get_logger()
    query_builders = Path(__file__).parent
    logger.info(
        "Looking for query builder class for %s/%s in %s",
        event_object_type,
        operation,
        query_builders,
    )
    try:
        query_builder_class = _resolve_query_builder_class(
            event_object_type,
            operation,
            is_timeseries,
        )
        if query_builder_class is not None:
            return query_builder_class(events)
    except Exception as e:
        logger.error("Error finding query builder for %s/%s: %s", event_object_type, operation, e)
    return None
