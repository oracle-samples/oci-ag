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


class BulkInsertQueryBuilder:
    def get_operation_sql(self, query_builder, events, date_columns):
        all_states = []
        for event in events:
            insert_data = []
            insert_column_list = []
            for event_group_column_name in event.keys():
                if event_group_column_name in date_columns:
                    continue
                insert_column_list.append(event_group_column_name.upper())
                insert_data.append(event[event_group_column_name])
            all_states.append(tuple(insert_data))

        # generate bulk insert query
        query = """INSERT ALL\n"""
        all_into_statements = []
        if len(all_states) > 0:
            for current_state in all_states:
                insert_sql = (
                    Query.into(query_builder)
                    .insert(*current_state)
                    .get_sql()
                    .replace("INSERT", "   ")
                )
                table_name = query_builder.table_manager.get_table_name().upper()
                column_list_str = ", ".join(insert_column_list)
                insert_sql = insert_sql.replace(
                    f'"{table_name}"', f'"{table_name}" ({column_list_str}) '
                )
                all_into_statements.append(insert_sql)

        query += """\n""".join(all_into_statements)
        query += """\nSELECT 1 FROM DUAL"""

        return [query]


class InsertQueryBuilder:
    def get_operation_sql(self, query_builder, event, date_columns):
        state_sets = None
        # for event in events:
        insert_data = []
        insert_column_list = []
        for event_group_column_name in event.keys():
            if event_group_column_name in date_columns:
                continue
            insert_column_list.append(event_group_column_name.upper())
            insert_data.append(event[event_group_column_name])
        state_sets = tuple(insert_data)

        insert_sql = Query.into(query_builder).insert(*state_sets).get_sql()
        table_name = query_builder.table_manager.get_table_name().upper()
        column_list_str = ", ".join(insert_column_list)
        insert_sql = insert_sql.replace(f'"{table_name}"', f'"{table_name}" ({column_list_str}) ')
        insert_sql = AttibuteStatementHandler.prepare_attribute_column_for_insert_statement(
            event, insert_sql
        )
        return insert_sql


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


class UpdateQueryBuilder:
    def get_operation_sql(
        self,
        query_builder,
        event,
        date_columns,
        where_columns,
        upsert_flag: bool = False,
    ):
        update_sql: Query = None
        for column_name, column_value in event.items():
            if column_name in where_columns:
                continue
            if column_name in date_columns:
                continue
            if update_sql is None:
                update_sql = Query.update(query_builder).set(column_name.upper(), column_value)
            else:
                update_sql = update_sql.set(column_name.upper(), column_value)

        for where_column_name in where_columns:
            update_sql = update_sql.where(
                getattr(query_builder, where_column_name.upper()) == event[where_column_name]
            )

        complete_update_stmt = update_sql.get_sql()
        if upsert_flag:
            complete_update_stmt = complete_update_stmt.replace("UPDATE", "UPSERT")

        update_sql = complete_update_stmt
        update_sql = AttibuteStatementHandler.prepare_attribute_column_for_insert_statement(
            event, update_sql
        )

        return update_sql


class UpdateManyQueryBuilder:
    def get_operation_sql(self, query_builder, events, date_columns, where_columns):
        event = events[0]
        update_sql = None
        # for event in events:
        update_sql = None
        for column_name, _ in event.items():
            if column_name in where_columns:
                continue
            if column_name in date_columns:
                continue
            if update_sql is None:
                update_sql = Query.update(query_builder).set(
                    column_name.upper(), Parameter(f":{column_name}")
                )
            else:
                update_sql = update_sql.set(column_name.upper(), Parameter(f":{column_name}"))

        for where_column_name in where_columns:
            update_sql = update_sql.where(
                getattr(query_builder, where_column_name.upper())
                == Parameter(f":{where_column_name}")
            )

        complete_update_stmt = update_sql.get_sql()

        return complete_update_stmt


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
        final_delete_sql = None
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

        self.logger.info(
            "%s time series inserts failed for %s events",
            self.table_manager.get_table_name().lower(),
            len(AdwConnection.get_cursor().getbatcherrors()),
        )
        for batch_error in AdwConnection.get_cursor().getbatcherrors():
            self.logger.info(
                "%s time series inserts failed for %s events, %s",
                self.table_manager.get_table_name().lower(),
                len(AdwConnection.get_cursor().getbatcherrors()),
                batch_error,
            )

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
