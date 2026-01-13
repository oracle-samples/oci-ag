# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json
import os
from abc import ABC, abstractmethod
from typing import ClassVar, Optional

from common.logger.logger import Logger
from dfa.adw.connection import AdwConnection


class BaseTable(ABC):
    logger = Logger(__name__).get_logger()
    _table_name: ClassVar[Optional[str]] = None
    _schema: ClassVar[Optional[str]] = None

    @abstractmethod
    def _column_definitions(self):
        pass

    def get_table_name(self):
        assert self._table_name is not None, "Table name is not set"
        return self._table_name.upper()

    def get_schema(self):
        if self._schema is None:
            if "DFA_ADW_DFA_SCHEMA" not in os.environ:
                raise Exception(
                    "DFA_ADW_DFA_SCHEMA environment variable not set - cannot process storage link"
                )
            self._schema = os.environ["DFA_ADW_DFA_SCHEMA"]

        return self._schema.upper()

    def _before_create(self):
        pass

    def create(self):
        if not self._table_exists():
            self.logger.info("Executing before create table activities")
            self._before_create()

            self.logger.info("Creating table %s", self.get_table_name())
            create_sql = self._get_create_ddl()

            AdwConnection.get_cursor().execute(create_sql)

            self.logger.info("Executing after create table activities")
            self._after_create()
        else:
            self.logger.info("Nothing to do - table %s has already exists", self.get_table_name())

    def _after_create(self):
        pass

    def get_create_table_sql(self):
        return self._get_create_ddl()

    def _get_create_ddl(self):
        self.logger.info("Generating DDL to create table %s", self.get_table_name())
        sql = f"""
            CREATE TABLE {self.get_schema()}.{self.get_table_name()}
            (
            {self._build_column_ddl()}
            )
        """
        return sql

    def _build_column_ddl(self):
        self.logger.info("Generating DDL for column definition for table %s", self.get_table_name())
        all_columns_ddl = self._get_all_columns_ddl()

        table_column_ddl = ""
        for column_ddl in all_columns_ddl:
            if "default" in column_ddl.keys():
                table_column_ddl += f' {column_ddl["name"]} {column_ddl["type"]}\
                    ({column_ddl["type_definition"]}) default {column_ddl["default"]},\n'
            else:
                if column_ddl["type_definition"]:
                    table_column_ddl += f'   {column_ddl["name"]} {column_ddl["type"]}\
                        ({column_ddl["type_definition"]}),\n'
                else:
                    table_column_ddl += f'   {column_ddl["name"]} {column_ddl["type"]},\n'

        return table_column_ddl[:-2]

    def _get_all_columns_ddl(self):
        table_definitions = []

        defintions = self.get_column_list_definition_for_table_ddl()

        for definition in defintions:
            column_definition = {}
            column_definition["name"] = definition["column_name"]
            column_definition["type"] = definition["data_type"]
            if definition["data_length"]:
                column_definition["type_definition"] = definition["data_length"]
            else:
                column_definition["type_definition"] = None
            table_definitions.append(column_definition)

        return table_definitions

    def _get_delete_ddl(self):
        self.logger.info("Generating DDL to drop table %s", self.get_table_name())
        sql = f"DROP TABLE {self.get_schema()}.{self.get_table_name()} PURGE"
        return sql

    def _before_delete(self):
        pass

    def delete(self):
        self.logger.info("Executing before delete table activities")
        self._before_delete()

        if self._table_exists():
            self.logger.info("Dropping table %s", self.get_table_name())
            delete_sql = self._get_delete_ddl()

            AdwConnection.get_cursor().execute(delete_sql)
        else:
            self.logger.info(
                "Nothing to do - table %s has already been dropped", self.get_table_name()
            )

    def _table_exists(self):
        exists = False
        self.logger.info("Checking if table %s exists", self.get_table_name())

        exists_sql = f"""
            SELECT
                COUNT(*)
            FROM
                ALL_OBJECTS
            WHERE
                OWNER = '{self.get_schema()}' and object_type = 'TABLE' and object_name = '{self.get_table_name()}'
            """
        AdwConnection.get_cursor().execute(exists_sql)
        table_count = AdwConnection.get_cursor().fetchone()

        if table_count[0] == 1:
            self.logger.info("Table %s exists", self.get_table_name())
            exists = True

        return exists

    def get_ordered_column_names_for_transformer(self):
        column_definitions = json.loads(self._column_definitions())
        column_names_for_transformer = []
        for definition in column_definitions:
            column_names_for_transformer.append(definition["column_name"].lower())

        return column_names_for_transformer

    def get_column_list_definition_for_table_ddl(self):
        column_definitions = json.loads(self._column_definitions())
        column_names_for_table_ddl = []
        for definition in column_definitions:
            column_names_for_table_ddl.append(definition)

        return column_names_for_table_ddl

    def get_default_row(self):
        column_definitions = json.loads(self._column_definitions())
        default_row = {}
        for definition in column_definitions:
            if definition["data_type"] == "CLOB":
                default_row[definition["column_name"].lower()] = json.dumps({})
            elif definition["data_type"].startswith("VARCHAR"):
                default_row[definition["column_name"].lower()] = ""
            else:
                default_row[definition["column_name"].lower()] = None
        return default_row


class BaseStateTable(BaseTable, ABC):
    @abstractmethod
    def get_unique_contraint_definition_details(self):
        pass

    def _build_unique_constraint_ddl(self):
        self.logger.info(
            "Generating DDL to add unique constraint to table %s", self.get_table_name()
        )

        ddl = ""
        if len(self.get_unique_contraint_definition_details()) > 0:
            constraint_columns = self.get_unique_contraint_definition_details()["columns"]
            constraint_columns_ddl = '"' + '", "'.join(constraint_columns) + '"'
            constraint = self.get_unique_contraint_definition_details()["name"]
            ddl = f"""
                ALTER TABLE {self.get_schema()}.{self.get_table_name()} ADD CONSTRAINT "{constraint}"
                UNIQUE ({constraint_columns_ddl})
                USING INDEX ENABLE
                """
        return ddl

    def _build_unique_index_ddl(self):
        self.logger.info("Generating DDL to add unique index to table %s", self.get_table_name())

        ddl = ""
        if len(self.get_unique_contraint_definition_details()) > 0:
            constraint_columns = self.get_unique_contraint_definition_details()["columns"]
            constraint_columns_ddl = '"' + '", "'.join(constraint_columns) + '"'
            constraint = self.get_unique_contraint_definition_details()["name"]
            ddl = f"""
                CREATE UNIQUE INDEX {self.get_schema()}.{constraint} ON \
                {self.get_schema()}.{self.get_table_name()} ({constraint_columns_ddl})
                """
        return ddl

    def _after_create(self):

        index_ddl = self._build_unique_index_ddl()
        if len(index_ddl) > 0:
            AdwConnection.get_cursor().execute(index_ddl)

        constraint_ddl = self._build_unique_constraint_ddl()
        if len(constraint_ddl) > 0:
            AdwConnection.get_cursor().execute(constraint_ddl)


class StreamOffsetTrackerTable(BaseTable):
    _table_name = "stream_offset_tracker"
    _schema = None

    def _column_definitions(self):
        return """
            [
                {"field_name":"ID","column_id":4,"column_name":"ID","column_expression":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
                {"field_name":"OFFSET","column_id":4,"column_name":"OFFSET","column_expression":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null},
                {"field_name":"APPLICATION","column_id":11,"column_name":"APPLICATION","column_expression":null,"skip_column":false,"data_type":"VARCHAR2","data_length":200,"data_format":null},
                {"field_name":"START_DATE","column_id":10,"column_name":"START_DATE","column_expression":null,"skip_column":false,"data_type":"DATE","data_length":null,"data_format":"DD/MM/RR"},
                {"field_name":"END_DATE","column_id":10,"column_name":"END_DATE","column_expression":null,"skip_column":false,"data_type":"DATE","data_length":null,"data_format":"DD/MM/RR"},
                {"field_name":"END_OFFSET","column_id":4,"column_name":"END_OFFSET","column_expression":null,"skip_column":false,"data_type":"NUMBER","data_length":null,"data_format":null}
            ]
            """
