# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from common.ocihelpers.event import DfaCreateFileEventRule
from dfa.bootstrap.envvars import bootstrap_local_machine_environment_variables
import base64
import getpass
import os
import string
import zipfile
import secrets

from common.logger.logger import Logger
from common.ocihelpers.adw import BaseAutonomousDatabase
from common.ocihelpers.function import *
from common.ocihelpers.vault import AdwSecrets
from dfa.adw.user_schema import UserSchema
from dfa.adw.tables.access_bundle import *
from dfa.adw.tables.access_guardrail import *
from dfa.adw.tables.audit_events import *
from dfa.adw.tables.cloud_group import *
from dfa.adw.tables.cloud_policy import *
from dfa.adw.tables.global_identity_collection import *
from dfa.adw.tables.identity import *
from dfa.adw.tables.permission import *
from dfa.adw.tables.permission_assignment import *
from dfa.adw.tables.policy import *
from dfa.adw.tables.policy_statement_resource_mapping import *
from dfa.adw.tables.resource import *
from dfa.adw.tables.role import *
from dfa.adw.tables.approval_workflow import *

from common.ocihelpers.vcn import *
from common.ocihelpers.vault import *
from common.ocihelpers.adw import *
from common.ocihelpers.function import *
from common.ocihelpers.connector import *
from common.ocihelpers.iam import *

bootstrap_local_machine_environment_variables('config.ini', 'DFA')

logger = Logger(__name__).get_logger()

def generate_wallet_password():
    uppercase = string.ascii_letters.upper()
    characters = string.ascii_letters.lower()
    numbers = string.digits

    filler1 = ''.join(secrets.choice(characters) for _ in range(6))
    uppercase_letter = ''.join(secrets.choice(uppercase) for _ in range(1))
    filler2 = ''.join(secrets.choice(characters) for _ in range(6))
    number = ''.join(secrets.choice(numbers) for _ in range(1))

    return filler1 + uppercase_letter + filler2 + number

def create_adw_tables():
    is_table = IdentityStateTable()
    cgs_table = CloudGroupStateTable()
    rs_table = ResourceStateTable()
    cps_table = CloudPolicyStateTable()
    psmr_table = PolicyStatementResourceMappingStateTable()
    gics_table = GlobalIdentityCollectionStateTable()
    abs_table = AccessBundleStateTable()
    ps_table = PermissionStateTable()
    pas_table = PermissionAssignmentStateTable()
    pols_table = PolicyStateTable()
    roles_table = RoleStateTable()
    agrs_table = AccessGuardrailStateTable()
    aw_table = ApprovalWorkflowStateTable()
    aes_table = AuditEventsTable()

    if os.environ.get('CREATE_TIME_SERIES', 'false').lower() == 'true':
        its_table = IdentityTimeSeriesTable()
        cgts_table = CloudGroupTimeSeriesTable()
        rts_table = ResourceTimeSeriesTable()
        cpts_table = CloudPolicyTimeSeriesTable()
        psmts_table = PolicyStatementResourceMappingTimeSeriesTable()
        gicts_table = GlobalIdentityCollectionTimeSeriesTable()
        abts_table = AccessBundleTimeSeriesTable()
        pts_table = PermissionTimeSeriesTable()
        pats_table = PermissionAssignmentTimeSeriesTable()
        polts_table = PolicyTimeSeriesTable()
        rolets_table = RoleTimeSeriesTable()
        agrts_table = AccessGuardrailTimeSeriesTable()
        awts_table = ApprovalWorkflowTimeSeriesTable()

    ## Look for recreate environment variable to determine if DFA ADW tables need to be recreated (delete first)
    if 'DFA_RECREATE_DFA_ADW_TABLES' in os.environ:
        logger.info('Application configuration set for recreating tables option...')
        if os.environ.get('DFA_RECREATE_DFA_ADW_TABLES', 'false').lower() == 'true':
            logger.info('Application configuration set table recreation.  Dropping all DFA tables now...')
            is_table.delete()
            cgs_table.delete()
            rs_table.delete()
            cps_table.delete()
            psmr_table.delete()
            gics_table.delete()
            abs_table.delete()
            ps_table.delete()
            pas_table.delete()
            pols_table.delete()
            roles_table.delete()
            agrs_table.delete()
            aw_table.delete()
            aes_table.delete()

            if os.environ.get('CREATE_TIME_SERIES', 'false').lower() == 'true':
                its_table.delete()
                cgts_table.delete()
                rts_table.delete()
                cpts_table.delete()
                psmts_table.delete()
                gicts_table.delete()
                abts_table.delete()
                pts_table.delete()
                pats_table.delete()
                polts_table.delete()
                rolets_table.delete()
                agrts_table.delete()
                awts_table.delete()

    ## Create DFA ADW tables
    is_table.create()
    cgs_table.create()
    rs_table.create()
    cps_table.create()
    psmr_table.create()
    gics_table.create()
    abs_table.create()
    ps_table.create()
    pas_table.create()
    pols_table.create()
    roles_table.create()
    agrs_table.create()
    aw_table.create()
    aes_table.create()

    if os.environ.get('CREATE_TIME_SERIES', 'false').lower() == 'true':
        its_table.create()
        cgts_table.create()
        rts_table.create()
        cpts_table.create()
        psmts_table.create()
        gicts_table.create()
        abts_table.create()
        pts_table.create()
        pats_table.create()
        polts_table.create()
        rolets_table.create()
        agrts_table.create()
        awts_table.create()

def setup(application_ocid):
    function_mgr = DfaSetupADWFunctionConfigs()

    downloaded_wallet_base_filename = 'dfa_adw_wallet'
    wallet_extract_location = '%s/%s' % (os.environ['DFA_LOCAL_SAVE_DIRECTORY'] , downloaded_wallet_base_filename)
    wallet_zip_filename = '%s.zip' % wallet_extract_location
    wallet_filename = 'cwallet.sso'
    wallet_pem_filename = 'ewallet.pem'
    wallet_password = generate_wallet_password()
    secret_mgr = AdwSecrets()

    wallet_exists_flag = secret_mgr.dfa_wallet_secret_exists()
    wallet_pem_exists_flag = secret_mgr.dfa_wallet_pem_secret_exists()
    wallet_password_exists_flag = secret_mgr.dfa_wallet_password_secret_exists()
    dfa_user_password_exists_flag = secret_mgr.dfa_user_password_exists()
    logger.info('Setting ADW connection information')
    function_mgr.add_adw_connection_string_to_configuration(application_ocid)

    if not wallet_exists_flag or not wallet_pem_exists_flag or not wallet_password_exists_flag or not dfa_user_password_exists_flag:
        logger.info('Generate wallet credentials')
        wallet_credentials = BaseAutonomousDatabase().generate_wallet(ocid=os.environ['DFA_ADW_INSTANCE_OCID'], password=wallet_password)

        logger.info('storing wallet')
        with open(wallet_zip_filename, "wb") as f:
            f.write(wallet_credentials)

        logger.info('unzipping wallet')
        with zipfile.ZipFile(wallet_zip_filename, 'r') as zip_ref:
            zip_ref.extractall(wallet_extract_location)

        if not wallet_exists_flag:
            logger.info('reading wallet and creating secret')
            wallet_full_filepath = wallet_extract_location + '/' + wallet_filename
            with open(wallet_full_filepath, 'rb') as wallet_ref:
                wallet_contents = wallet_ref.read()
                wallet_secret_content = base64.b64encode(wallet_contents).decode()
                secret_mgr.create_wallet_secret(wallet_secret_content)

        if not wallet_pem_exists_flag:
            logger.info('reading wallet pem and creating secret')
            wallet_pem_full_filepath = wallet_extract_location + '/' + wallet_pem_filename
            with open(wallet_pem_full_filepath, 'r') as wallet_pem_ref:
                wallet_pem_contents = wallet_pem_ref.read()
                secret_mgr.create_wallet_pem_secret(wallet_pem_contents)

        if not wallet_password_exists_flag:
            logger.info('reading wallet secret and creating secret')
            secret_mgr.create_wallet_password_secret(wallet_password)

    else:
        logger.info('Wallet credentials are stored in configured vault - moving with setup')

    logger.info('Creating DFA ADW user and schema')
    UserSchema().create_user_and_schema()

    logger.info('Creating DFA Tables')
    create_adw_tables()


def main():
    # create vcn and related resources
    vcn_class = DfaCreateVCN()
    vcn_class.create_vcn()
    vcn_class.create_service_gateway()
    vcn_class.create_nat_gateway()
    vcn_class.create_nat_route_table()
    vcn_class.create_private_subnet()
    subnet_ids = [vcn_class.get_private_subnet_id()]

    # create vault, master encryption key, and adw admin password
    vault_class = DfaCreateVault()
    vault_class.create_vault()
    vault_class.create_key()
    secret_mgr = AdwSecrets()
    if not secret_mgr.dfa_user_password_exists():
        adw_password = getpass.getpass('Enter a password for the ADW instance. It must be 12 to 30 characters long, at least 1 uppercase, 1 lowercase, 1 numeric. It cannot contain double quotes or the word "admin".')
        UserSchema().create_user_password(adw_password=adw_password)

    # create adw instance
    adw_class = DfaCreateAutonomousDatabase()
    adw_class.create_adw(secret_mgr.get_dfa_user_password())

    # create function application
    application_class = DfaApplication()
    application_class.create_functions_application(subnet_ids)
    application_id = application_class.get_application_id()
    
    function_ids = []

    # create f2s transformer function
    f2s_class = DfaFileToStateFunction()
    f2s_class.create_file_to_state_transformer()
    f2s_function_id = f2s_class.get_file_to_state_transformer_function_id()
    function_ids.append(f2s_function_id)

    # create s2s transformer function
    s2s_class = DfaStreamToStateFunction()
    s2s_class.create_stream_to_state_transformer()
    s2s_function_id = s2s_class.get_stream_to_state_transformer_function_id()
    function_ids.append(s2s_function_id)

    # create audit transformer function
    audit_transformer_class = DfaAuditTransformerFunctions()
    audit_transformer_class.create_audit_transformer()
    audit_function_id = audit_transformer_class.get_audit_transformer_function_id()
    function_ids.append(audit_function_id)

    # create f2s event rule
    event_class = DfaCreateFileEventRule()
    event_class.create_rule(f2s_function_id, os.environ['DFA_BUCKET_NAME'], os.environ['RESOURCE_NAME_PREFIX'] + "_file_to_state")

    if os.environ.get('CREATE_TIME_SERIES', 'false').lower() == 'true':
        # create f2ts transformer function
        f2ts_class = DfaFileToTsFunction()
        f2ts_class.create_file_to_ts_transformer()
        f2ts_function_id = f2ts_class.get_file_to_ts_transformer_function_id()
        function_ids.append(f2ts_function_id)

        # create s2ts transformer function
        s2ts_class = DfaStreamToTsFunction()
        s2ts_class.create_stream_to_ts_transformer()
        s2ts_function_id = s2ts_class.get_stream_to_ts_transformer_function_id()
        function_ids.append(s2ts_function_id)

        # create f2ts event rule
        event_class.create_rule(f2ts_function_id, os.environ['DFA_BUCKET_NAME'], os.environ['RESOURCE_NAME_PREFIX'] + "_file_to_ts") 

        # create the s2ts connector
        s2ts_sch_class = DfaStreamToTsConnector()
        s2ts_sch_class.create_stream_to_ts_sch(s2ts_function_id)
    else:
        f2ts_function_id = None
        s2ts_function_id = None


    # create the s2s connector
    s2s_sch_class = DfaStreamToStateConnector()
    s2s_sch_class.create_stream_to_state_sch(s2s_function_id)

    # create the audit connector
    audit_sch_class = DfaAuditConnector()
    audit_sch_class.create_audit_sch(audit_function_id)

    logger.info(f'Waiting for adw instance to reach an available state...')
    adw_class.wait_for_active_adw()

    #  set up the secrets, database tables, dfa_user schema
    setup(application_id)

    # create the policy giving the appropriate permissions to the resources
    policy_class = DfaAccessPolicy()
    policy_class.create_access_policy()

    # create the dynamic group containing the transformer functions
    dynamic_group_class = DfaFunctionsDynamicGroup()
    dynamic_group_class.create_functions_dynamic_group(function_ids)

    logger.info(f'DFA has successfully been set up')

if __name__ == "__main__":
    main()