# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import os

from common.logger.logger import Logger
from common.ocihelpers.vault import AdwSecrets
from dfa.adw.connection import AdwConnection


class UserSchema:
    logger = Logger(__name__).get_logger()

    def _user_schema_exists(self):
        exists = False
        dfa_schema = os.environ["DFA_ADW_DFA_SCHEMA"]
        sql = f"select count(*) from dba_users where username='{dfa_schema}'"

        AdwConnection.get_cursor(username="ADMIN").execute(sql)
        count_result = AdwConnection.get_cursor(username="ADMIN").fetchone()

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
            AdwConnection.get_cursor(username="ADMIN").execute(
                self._get_create_user_sql(dfa_user_password)
            )

            grant_role_statements = self._get_roles_statements()
            self.logger.info("granting necessary roles...")
            for grant_statement in grant_role_statements:
                AdwConnection.get_cursor(username="ADMIN").execute(grant_statement)

            self.logger.info("enabling schema...")
            AdwConnection.get_cursor(username="ADMIN").execute(self._get_enable_schema_plsql())
            self.logger.info("enabling oml...")
            AdwConnection.get_cursor(username="ADMIN").execute(self._get_enable_oml())
            self.logger.info("alter schema for unlimited quota on data...")
            AdwConnection.get_cursor(username="ADMIN").execute(self._get_unlimited_quota_plsql())
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
