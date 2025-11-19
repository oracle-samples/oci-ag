# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import atexit
import os

import oracledb

from common.logger.logger import Logger
from common.ocihelpers.vault import AdwSecrets


class SetupConnection:
    logger = Logger(__name__).get_logger()
    __connection = None
    __cursor = None

    def get_connection(self):
        if self.__connection is None:
            self.logger.info("Creating new connection - pulling all secrets from OCI vault")

            secrets_mgr = AdwSecrets()
            wallet_directory = secrets_mgr.get_wallet_save_directory()
            username = "ADMIN"
            password = secrets_mgr.get_password()
            wallet_password = secrets_mgr.get_wallet_password()
            secrets_mgr.save_wallet()
            secrets_mgr.save_ewallet_pem()

            dsn = f"{os.environ['DFA_CONN_PROTOCOL']}://{os.environ['DFA_CONN_HOST']}\
:{os.environ['DFA_CONN_PORT']}/{os.environ['DFA_CONN_SERVICE_NAME']}?\
wallet_location={wallet_directory}&retry_count={os.environ['DFA_CONN_RETRY_COUNT']}\
&retry_delay={os.environ['DFA_CONN_RETRY_DELAY']}"

            self.logger.info("Attempting to connect...")
            self.__connection = oracledb.connect(
                user=username,
                password=password,
                dsn=dsn,
                wallet_password=wallet_password,
            )
            atexit.register(self._close_all)

            self.logger.info("Connection established successfully!!")

        return self.__connection

    def get_cursor(self):
        if self.__cursor is None:
            self.__cursor = self.get_connection().cursor()

        return self.__cursor

    def _close_all(self):
        if self.__cursor:
            self.logger.info("Closing cursor(s)")
            self.__cursor.close()

        if self.__connection:
            self.logger.info("Closing connection(s)")
            self.__connection.close()

    def commit(self):
        self.__connection.commit()

        if self.__cursor is not None:
            self.logger.info("Open cursor - performing commit")
            self.__cursor.close()
        else:
            self.logger.info("No open cursor - nothing to commit")

        self.__cursor = None

    def close(self):
        self._close_all()


class UserSchema:
    logger = Logger(__name__).get_logger()
    __connection_mgr = None

    def _get_connection_mgr(self):
        if self.__connection_mgr is None:
            self.__connection_mgr = SetupConnection()
        return self.__connection_mgr

    def _user_schema_exists(self):
        exists = False
        dfa_schema = os.environ["DFA_ADW_DFA_SCHEMA"]
        sql = f"select count(*) from dba_users where username='{dfa_schema}'"

        self._get_connection_mgr().get_cursor().execute(sql)
        count_result = self._get_connection_mgr().get_cursor().fetchone()

        if count_result[0] == 1:
            exists = True

        return exists

    def create_user_password(self, adw_password):
        adw_secrets_mgr = AdwSecrets()
        secret_exists_flag = adw_secrets_mgr.dfa_user_password_exists()

        if not secret_exists_flag:
            self.logger.info("No secret found - storing password in configured DFA vault...")
            adw_secrets_mgr.save_dfa_user_password(adw_password)
            self.logger.info("ADW password successfully saved to configured DFA vault")

    def create_user_and_schema(self):

        self.logger.info("Checking if DFA_USER schema exists in configured DFA ADW instance")
        if not self._user_schema_exists():
            self.logger.info("DFA_USER schema does not exist - creating")

            self.logger.info("Pulling DFA_USER password from configured DFA vault")
            adw_secrets_mgr = AdwSecrets()
            dfa_user_password = adw_secrets_mgr.get_dfa_user_password()

            self.logger.info("Creating user...")
            self._get_connection_mgr().get_cursor().execute(
                self._get_create_user_sql(dfa_user_password)
            )

            grant_role_statements = self._get_roles_statements()
            self.logger.info("granting necessary roles...")
            for grant_statement in grant_role_statements:
                self._get_connection_mgr().get_cursor().execute(grant_statement)

            self.logger.info("enabling schema...")
            self._get_connection_mgr().get_cursor().execute(self._get_enable_schema_plsql())
            self.logger.info("enabling oml...")
            self._get_connection_mgr().get_cursor().execute(self._get_enable_oml())
            self.logger.info("alter schema for unlimited quota on data...")
            self._get_connection_mgr().get_cursor().execute(self._get_unlimited_quota_plsql())
            self.logger.info(
                "DFA_USER user and schema successfully created - the DFA_USER schema is ready for setup"
            )
        else:
            self.logger.info("DFA_USER user found - the DFA_USER schema is ready for setup")

    def _get_create_user_sql(self, password):
        if not password:
            self.logger.exception("Cannot generate create user sql - no password was provided")
            raise Exception("Cannot generate create user sql - no password was provided")

        dfa_schema = os.environ["DFA_ADW_DFA_SCHEMA"]
        sql = f"""CREATE USER {dfa_schema} IDENTIFIED BY "{password}" """

        return sql

    def _get_roles_statements(self) -> list:
        roles = [
            "GRANT ACCHK_READ TO DFA_USER",
            "GRANT ADB_MONITOR TO DFA_USER",
            "GRANT ADM_PARALLEL_EXECUTE_TASK TO DFA_USER",
            "GRANT ADPADMIN TO DFA_USER",
            "GRANT ADPUSER TO DFA_USER",
            "GRANT APEX_ADMINISTRATOR_READ_ROLE TO DFA_USER",
            "GRANT APEX_ADMINISTRATOR_ROLE TO DFA_USER",
            "GRANT AQ_ADMINISTRATOR_ROLE TO DFA_USER",
            "GRANT AQ_USER_ROLE TO DFA_USER",
            "GRANT AUDIT_ADMIN TO DFA_USER",
            "GRANT AUDIT_VIEWER TO DFA_USER",
            "GRANT CAPTURE_ADMIN TO DFA_USER",
            "GRANT CONNECT TO DFA_USER",
            "GRANT CONSOLE_ADMIN TO DFA_USER",
            "GRANT CONSOLE_DEVELOPER TO DFA_USER",
            "GRANT CONSOLE_MONITOR TO DFA_USER",
            "GRANT CONSOLE_OPERATOR TO DFA_USER",
            "GRANT CTXAPP TO DFA_USER",
            "GRANT DATAPUMP_CLOUD_EXP TO DFA_USER",
            "GRANT DATAPUMP_CLOUD_IMP TO DFA_USER",
            "GRANT DCAT_SYNC TO DFA_USER",
            "GRANT DV_ACCTMGR TO DFA_USER",
            "GRANT DV_OWNER TO DFA_USER",
            "GRANT DWROLE TO DFA_USER",
            "GRANT GATHER_SYSTEM_STATISTICS TO DFA_USER",
            "GRANT GRAPH_ADMINISTRATOR TO DFA_USER",
            "GRANT GRAPH_DEVELOPER TO DFA_USER",
            "GRANT HS_ADMIN_SELECT_ROLE TO DFA_USER",
            "GRANT LBAC_DBA TO DFA_USER",
            "GRANT LINEAGE_AUTHOR TO DFA_USER",
            "GRANT ODIADMIN TO DFA_USER",
            "GRANT OEM_ADVISOR TO DFA_USER",
            "GRANT OML_DEVELOPER TO DFA_USER",
            "GRANT OML_SYS_ADMIN TO DFA_USER",
            "GRANT OPTIMIZER_PROCESSING_RATE TO DFA_USER",
            "GRANT ORDS_ADMINISTRATOR_ROLE TO DFA_USER",
            "GRANT PDB_DBA TO DFA_USER",
            "GRANT PGX_SERVER_GET_INFO TO DFA_USER",
            "GRANT PGX_SERVER_MANAGE TO DFA_USER",
            "GRANT PGX_SESSION_ADD_PUBLISHED_GRAPH TO DFA_USER",
            "GRANT PGX_SESSION_COMPILE_ALGORITHM TO DFA_USER",
            "GRANT PGX_SESSION_CREATE TO DFA_USER",
            "GRANT PGX_SESSION_GET_PUBLISHED_GRAPH TO DFA_USER",
            "GRANT PGX_SESSION_MODIFY_MODEL TO DFA_USER",
            "GRANT PGX_SESSION_NEW_GRAPH TO DFA_USER",
            "GRANT PGX_SESSION_READ_MODEL TO DFA_USER",
            "GRANT PROVISIONER TO DFA_USER",
            "GRANT RESOURCE TO DFA_USER",
            "GRANT SELECT_CATALOG_ROLE TO DFA_USER",
            "GRANT SODA_APP TO DFA_USER",
            "GRANT ACCHK_READ TO DFA_USER WITH ADMIN OPTION",
            "GRANT ADB_MONITOR TO DFA_USER WITH ADMIN OPTION",
            "GRANT ADM_PARALLEL_EXECUTE_TASK TO DFA_USER WITH ADMIN OPTION",
            "GRANT ADPADMIN TO DFA_USER WITH ADMIN OPTION",
            "GRANT ADPUSER TO DFA_USER WITH ADMIN OPTION",
            "GRANT APEX_ADMINISTRATOR_READ_ROLE TO DFA_USER WITH ADMIN OPTION",
            "GRANT APEX_ADMINISTRATOR_ROLE TO DFA_USER WITH ADMIN OPTION",
            "GRANT AQ_ADMINISTRATOR_ROLE TO DFA_USER WITH ADMIN OPTION",
            "GRANT AQ_USER_ROLE TO DFA_USER WITH ADMIN OPTION",
            "GRANT AUDIT_ADMIN TO DFA_USER WITH ADMIN OPTION",
            "GRANT AUDIT_VIEWER TO DFA_USER WITH ADMIN OPTION",
            "GRANT CAPTURE_ADMIN TO DFA_USER WITH ADMIN OPTION",
            "GRANT CONNECT TO DFA_USER WITH ADMIN OPTION",
            "GRANT CONSOLE_ADMIN TO DFA_USER WITH ADMIN OPTION",
            "GRANT CONSOLE_DEVELOPER TO DFA_USER WITH ADMIN OPTION",
            "GRANT CONSOLE_MONITOR TO DFA_USER WITH ADMIN OPTION",
            "GRANT CONSOLE_OPERATOR TO DFA_USER WITH ADMIN OPTION",
            "GRANT CTXAPP TO DFA_USER WITH ADMIN OPTION",
            "GRANT DATAPUMP_CLOUD_EXP TO DFA_USER WITH ADMIN OPTION",
            "GRANT DATAPUMP_CLOUD_IMP TO DFA_USER WITH ADMIN OPTION",
            "GRANT DV_ACCTMGR TO DFA_USER WITH ADMIN OPTION",
            "GRANT DV_OWNER TO DFA_USER WITH ADMIN OPTION",
            "GRANT DWROLE TO DFA_USER WITH ADMIN OPTION",
            "GRANT GATHER_SYSTEM_STATISTICS TO DFA_USER WITH ADMIN OPTION",
            "GRANT GRAPH_ADMINISTRATOR TO DFA_USER WITH ADMIN OPTION",
            "GRANT GRAPH_DEVELOPER TO DFA_USER WITH ADMIN OPTION",
            "GRANT HS_ADMIN_SELECT_ROLE TO DFA_USER WITH ADMIN OPTION",
            "GRANT LBAC_DBA TO DFA_USER WITH ADMIN OPTION",
            "GRANT LINEAGE_AUTHOR TO DFA_USER WITH ADMIN OPTION",
            "GRANT OEM_ADVISOR TO DFA_USER WITH ADMIN OPTION",
            "GRANT OML_DEVELOPER TO DFA_USER WITH ADMIN OPTION",
            "GRANT OML_SYS_ADMIN TO DFA_USER WITH ADMIN OPTION",
            "GRANT OPTIMIZER_PROCESSING_RATE TO DFA_USER WITH ADMIN OPTION",
            "GRANT ORDS_ADMINISTRATOR_ROLE TO DFA_USER WITH ADMIN OPTION",
            "GRANT PDB_DBA TO DFA_USER WITH ADMIN OPTION",
            "GRANT PGX_SERVER_GET_INFO TO DFA_USER WITH ADMIN OPTION",
            "GRANT PGX_SERVER_MANAGE TO DFA_USER WITH ADMIN OPTION",
            "GRANT PGX_SESSION_ADD_PUBLISHED_GRAPH TO DFA_USER WITH ADMIN OPTION",
            "GRANT PGX_SESSION_COMPILE_ALGORITHM TO DFA_USER WITH ADMIN OPTION",
            "GRANT PGX_SESSION_CREATE TO DFA_USER WITH ADMIN OPTION",
            "GRANT PGX_SESSION_GET_PUBLISHED_GRAPH TO DFA_USER WITH ADMIN OPTION",
            "GRANT PGX_SESSION_MODIFY_MODEL TO DFA_USER WITH ADMIN OPTION",
            "GRANT PGX_SESSION_NEW_GRAPH TO DFA_USER WITH ADMIN OPTION",
            "GRANT PGX_SESSION_READ_MODEL TO DFA_USER WITH ADMIN OPTION",
            "GRANT PROVISIONER TO DFA_USER WITH ADMIN OPTION",
            "GRANT RESOURCE TO DFA_USER WITH ADMIN OPTION",
            "GRANT SELECT_CATALOG_ROLE TO DFA_USER WITH ADMIN OPTION",
            "GRANT SODA_APP TO DFA_USER WITH ADMIN OPTION",
            "ALTER USER DFA_USER DEFAULT ROLE ACCHK_READ,ADB_MONITOR,ADM_PARALLEL_EXECUTE_TASK,\
            ADPADMIN,ADPUSER,APEX_ADMINISTRATOR_READ_ROLE,APEX_ADMINISTRATOR_ROLE,\
            AQ_ADMINISTRATOR_ROLE,AQ_USER_ROLE,AUDIT_ADMIN,AUDIT_VIEWER,CAPTURE_ADMIN,CONNECT,\
            CONSOLE_ADMIN,CONSOLE_DEVELOPER,CONSOLE_MONITOR,CONSOLE_OPERATOR,CTXAPP,DATAPUMP_CLOUD_EXP,\
            DATAPUMP_CLOUD_IMP,DCAT_SYNC,DV_ACCTMGR,DV_OWNER,DWROLE,GATHER_SYSTEM_STATISTICS,\
            GRAPH_ADMINISTRATOR,GRAPH_DEVELOPER,HS_ADMIN_SELECT_ROLE,LBAC_DBA,LINEAGE_AUTHOR,ODIADMIN,\
            OEM_ADVISOR,OML_DEVELOPER,OML_SYS_ADMIN,OPTIMIZER_PROCESSING_RATE,ORDS_ADMINISTRATOR_ROLE,\
            PDB_DBA,PGX_SERVER_GET_INFO,PGX_SERVER_MANAGE,PGX_SESSION_ADD_PUBLISHED_GRAPH,\
            PGX_SESSION_COMPILE_ALGORITHM,PGX_SESSION_CREATE,PGX_SESSION_GET_PUBLISHED_GRAPH,\
            PGX_SESSION_MODIFY_MODEL,PGX_SESSION_NEW_GRAPH,PGX_SESSION_READ_MODEL,PROVISIONER,\
            RESOURCE,SELECT_CATALOG_ROLE,SODA_APP",
        ]

        return roles

    def _get_enable_schema_plsql(self) -> str:
        plsql = """
BEGIN
    ORDS_ADMIN.ENABLE_SCHEMA(
        p_enabled => TRUE,
        p_schema => 'DFA_USER',
        p_url_mapping_type => 'BASE_PATH',
        p_url_mapping_pattern => 'dfa_user',
        p_auto_rest_auth=> TRUE
    );
    -- ENABLE DATA SHARING
    C##ADP$SERVICE.DBMS_SHARE.ENABLE_SCHEMA(
            SCHEMA_NAME => 'DFA_USER',
            ENABLED => TRUE
    );
    commit;
END;
"""
        return plsql

    def _get_enable_oml(self) -> str:
        plsql = """
ALTER USER DFA_USER GRANT CONNECT THROUGH OML$PROXY
"""
        return plsql

    def _get_unlimited_quota_plsql(self) -> str:
        plsql = """
ALTER USER DFA_USER QUOTA UNLIMITED ON DATA
"""
        return plsql

    def _get_enable_resource_principal_for_user_plsql(self):
        dfa_schema = os.environ["DFA_ADW_DFA_SCHEMA"]
        plsql = f"""
begin
    DBMS_CLOUD_ADMIN.ENABLE_RESOURCE_PRINCIPAL(username => '{dfa_schema}');
end;
"""
        return plsql
