# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import importlib.util
import inspect
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import oracledb
from pypika import CustomFunction, Order, Parameter, Query, Table
from pypika.functions import ToDate

from common.logger.logger import Logger
from dfa.adw.connection import AdwConnection
from dfa.adw.tables.base_table import StreamOffsetTrackerTable


class InsertManyQueryBuilder:
    def get_operation_sql(self, query_builder, events, date_columns):
        event = events[0]

        insert_data = []
        insert_column_list = []
        insert_parameters = []
        for event_group_column_name in event.keys():
            if event_group_column_name in date_columns:
                continue
            insert_column_list.append(event_group_column_name.upper())
            insert_data.append(event[event_group_column_name])
            insert_parameters.append(Parameter(f":{event_group_column_name}"))
        parameter_set = tuple(insert_parameters)

        insert_sql = Query.into(query_builder).insert(*parameter_set).get_sql()

        table_name = query_builder.table_manager.get_table_name().upper()
        column_list_str = ", ".join(insert_column_list)
        insert_sql = insert_sql.replace(f'"{table_name}"', f'"{table_name}" ({column_list_str}) ')
        return insert_sql

    def get_input_sizes(self, columns_definition: list[dict[str, Any]]):
        input_sizes = {}
        for column in columns_definition:
            column_name = column["column_name"]
            data_type = column["data_type"].upper()
            if data_type.startswith("VARCHAR"):
                input_sizes[column_name] = column["data_length"]
            elif data_type == "NUMBER":
                input_sizes[column_name] = oracledb.NUMBER
            else:
                input_sizes[column_name] = None

        return input_sizes


class UpdateManyQueryBuilder:
    def get_operation_sql(
        self, query_builder, events, date_columns, where_columns, nullable_columns=None
    ):
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
            if update_sql is None:
                update_sql = Query.update(query_builder).set(
                    column_name.upper(), Parameter(f":{column_name}")
                )
            else:
                update_sql = update_sql.set(column_name.upper(), Parameter(f":{column_name}"))

        if update_sql is None:
            return None

        # Use Oracle DECODE for NULL-safe equality on executemany binds
        decode = CustomFunction("DECODE", ["expr1", "expr2", "ret_equal", "ret_not_equal"])
        for where_column_name in where_columns:
            column = getattr(query_builder, where_column_name.upper())
            param = Parameter(f":{where_column_name}")
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
    ) -> str:
        assert len(events) > 0, "events cannot be empty for MERGE"

        example = events[0]
        all_cols = list(example.keys())
        key_cols = [c.lower() for c in where_columns]
        date_cols = [c.lower() for c in date_columns]

        # Columns used in UPDATE SET (exclude keys and date columns)
        updatable_cols = [
            c for c in all_cols if c.lower() not in key_cols and c.lower() not in date_cols
        ]

        # Qualify table name with schema
        schema = query_builder.table_manager.get_schema().upper()
        table = query_builder.table_manager.get_table_name().upper()
        qualified_table = f'"{schema}"."{table}"'

        # USING subquery with bind variables once per executemany row
        select_bind_parts = []
        for col in all_cols:
            select_bind_parts.append(f':{col} AS "{col.upper()}"')
        using_subquery = f"SELECT {', '.join(select_bind_parts)} FROM DUAL"

        # Build ON clause with composite keys if needed
        on_conditions = []
        for k in where_columns:
            on_conditions.append(f"NVL(t.\"{k.upper()}\", '') = NVL(s.\"{k.upper()}\", '')")
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

    def get_input_sizes(self, columns_definition: list[dict[str, Any]]):
        # Reuse the same sizing logic as InsertMany
        return InsertManyQueryBuilder().get_input_sizes(columns_definition)


class AttibuteStatementHandler:
    MAX_ATTRIBUTE_SIZE = 32767
    CHUNK_SIZE = 30000

    @classmethod
    def prepare_attribute_column_for_insert_statement(cls, data, insert_statement):
        for data_key, data_value in data.items():
            if "attribute" in data_key:

                if len(data_value) >= AttibuteStatementHandler.MAX_ATTRIBUTE_SIZE:
                    moving_start = 0
                    moving_end = AttibuteStatementHandler.CHUNK_SIZE
                    keep_splitting = True
                    all_chunks = []
                    first_chunk = True
                    while keep_splitting:

                        chunk_string = ""
                        chunk_string = data_value[moving_start:moving_end]

                        if first_chunk:
                            chunk_for_insert = "to_clob('" + chunk_string + "')"
                            first_chunk = False
                        else:
                            chunk_for_insert = "'" + chunk_string + "'"
                        all_chunks.append(chunk_for_insert)

                        if moving_start > len(data_value):
                            keep_splitting = False

                        moving_start += AttibuteStatementHandler.CHUNK_SIZE
                        moving_end += AttibuteStatementHandler.CHUNK_SIZE

                    insert_statement = insert_statement.replace(
                        "'" + data[data_key] + "'", " || ".join(all_chunks)
                    )

        return insert_statement


class DeleteQueryBuilder:
    def get_operation_sql(self, query_builder, event, where_columns):
        final_delete_sql: Any = None
        delete_sql = Query.from_(query_builder).delete()
        for where in where_columns:
            delete_sql = delete_sql.where(getattr(query_builder, where.upper()) == event[where])
        final_delete_sql = delete_sql.get_sql()

        return final_delete_sql

    def delete_outdated_rows(self, query_builder, event):
        to_timestamp = CustomFunction("TO_TIMESTAMP", ["timestamp_string", "format_string"])

        where_clause = (getattr(query_builder, "ID") == event["id"]) & (
            getattr(query_builder, "EVENT_TIMESTAMP")
            < to_timestamp(event["event_timestamp"], "DD-Mon-YY HH:MI:SS.FF6 AM")
        )

        delete_sql = Query.from_(query_builder).delete().where(where_clause)
        final_delete_query = delete_sql.get_sql()

        return final_delete_query

    def remove_duplicates(self, events):
        unique_pairs = {}
        for event in events:
            key = (event["id"], event["event_timestamp"])
            if key not in unique_pairs:
                unique_pairs[key] = event
        return list(unique_pairs.values())


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
        statement = (
            Query.into(self)
            .columns("OFFSET", "END_OFFSET", "APPLICATION")
            .insert(*offset_data)
            .get_sql()
        )
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
            .where(
                self.START_DATE
                < ToDate(start_date.strftime("%d-%b-%y %H:%M:%S"), "DD-MON-RR HH24:MI:SS")
            )
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
        input_sizes = InsertManyQueryBuilder().get_input_sizes(
            self.table_manager.get_column_list_definition_for_table_ddl()
        )
        AdwConnection.get_cursor().setinputsizes(**input_sizes)
        AdwConnection.get_cursor().executemany(insert_statement, self.events, batcherrors=True)

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

    def executemany_merge_for_events(
        self,
        where_columns: list[str],
        date_columns: list[str] | None = None,
    ):
        """
        Generic helper to perform MERGE (upsert) for current events using the
        provided unique key columns. Intended for state tables that would
        otherwise do insert+update passes.
        """
        date_columns = date_columns or []

        if not self.events or len(self.events) == 0:
            self.logger.info(
                "No events to process by %s merge query builder",
                self.table_manager.get_table_name().lower(),
            )
            return

        self.logger.info(
            "Using MERGE into %s for %d events (keys: %s)",
            self.table_manager.get_table_name().lower(),
            len(self.events),
            ",".join([c.lower() for c in where_columns]),
        )

        merge_sql = MergeManyQueryBuilder().get_operation_sql(
            self, self.events, date_columns, where_columns
        )
        input_sizes = MergeManyQueryBuilder().get_input_sizes(
            self.table_manager.get_column_list_definition_for_table_ddl()
        )
        AdwConnection.get_cursor().setinputsizes(**input_sizes)
        AdwConnection.get_cursor().executemany(merge_sql, self.events, batcherrors=True)

        batch_errors = list(AdwConnection.get_cursor().getbatcherrors())
        if batch_errors:
            self.logger.warning(
                "%s merge encountered %d batch error(s)",
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


def get_query_builder(event_object_type, operation, events, is_timeseries=False):
    logger = Logger(__name__).get_logger()

    if is_timeseries:
        class_name = f"{event_object_type.lower().title().replace('_', '')}\
TimeSeries{operation.lower().title().replace('_', '')}QueryBuilder"
    else:
        class_name = f"{event_object_type.lower().title().replace('_', '')}\
State{operation.lower().title().replace('_', '')}QueryBuilder"

    query_builders = Path(__file__).parent
    logger.info("Looking for query builder class %s in %s", class_name, query_builders)
    for file in os.listdir(query_builders):
        full_path = os.path.join(query_builders, file)
        if os.path.isfile(full_path) and file.endswith(".py") and not file.startswith("__"):
            module_name = file[:-3]  # Remove .py extension
            if module_name.lower() == event_object_type.lower():
                try:
                    spec = importlib.util.spec_from_file_location(module_name, full_path)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    for cls_name, cls_obj in inspect.getmembers(module):
                        if inspect.isclass(cls_obj) and cls_name == class_name:
                            return cls_obj(events)
                except Exception as e:
                    logger.error("Error finding %s: %s", class_name, e)
    return None
