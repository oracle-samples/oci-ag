# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import os

import oci

from common.logger.logger import Logger
from common.ocihelpers.adw import BaseAutonomousDatabase
from common.ocihelpers.artifact import DfaTransformerArtifacts


class BaseFunction:
    logger = Logger(__name__).get_logger()
    __config = None
    _signer = None
    __client = None
    __artifact_manager = None
    _file_to_ts_transformer_function_id = None
    _file_to_ts_transformer_function_name = "file_to_ts_transformer"
    _file_to_state_transformer_function_id = None
    _file_to_state_transformer_function_name = "file_to_state_transformer"
    _stream_to_ts_transformer_function_id = None
    _stream_to_ts_transformer_function_name = "stream_to_ts_transformer"
    _stream_to_state_transformer_function_id = None
    _stream_to_state_transformer_function_name = "stream_to_state_transformer"
    _audit_transformer_function_id = None
    _audit_transformer_function_name = "audit_transformer"
    _function_application_id = None
    _function_application_name = None

    def __set_config(self):
        if os.environ["DFA_SIGNER_TYPE"] == "user":
            self.__config = oci.config.from_file(
                os.environ["DFA_CONFIG_LOCATION"], os.environ["DFA_CONFIG_PROFILE"]
            )
        else:
            self.__config = {}

    def __get_config(self):
        if self.__config is None:
            self.__set_config()

        return self.__config

    def __set_artifact_manager(self):
        self.__artifact_manager = DfaTransformerArtifacts()

    def _get_artifact_manager(self):
        if self.__artifact_manager is None:
            self.__set_artifact_manager()
        return self.__artifact_manager

    def __set_signer(self):
        token = None

        if os.environ["DFA_SIGNER_TYPE"] == "user":
            if os.environ["OCI_AUTH_TYPE"] == "security_token_file":
                config = self.__get_config()
                token_file = config["security_token_file"]

                with open(token_file, "r", encoding="utf-8") as f:
                    token = f.read()

                private_key = oci.signer.load_private_key_from_file(config["key_file"])
                self._signer = oci.auth.signers.SecurityTokenSigner(token, private_key)

            elif os.environ["OCI_AUTH_TYPE"] == "delegation_token_file":
                config = self.__get_config()
                token_file = config["delegation_token_file"]
                with open(token_file, "r", encoding="utf-8") as f:
                    token = f.read()
                self._signer = oci.auth.signers.InstancePrincipalsDelegationTokenSigner(
                    delegation_token=token
                )

            else:
                self.logger.exception(
                    "Please specify a valid OCI_AUTH_TYPE in config.ini. "
                    "Accepted values are 'security_token_file' and 'delegation_token_file'."
                )
                raise Exception(
                    "Specify a valid OCI_AUTH_TYPE in config.ini. "
                    "Accepted values are 'security_token_file' and 'delegation_token_file'."
                )
        else:
            self._signer = oci.auth.signers.get_resource_principals_signer()

    def __get_signer(self):
        if self._signer is None:
            self.__set_signer()

        return self._signer

    def __set_client(self):
        self.__client = oci.functions.FunctionsManagementClient(
            config=self.__get_config(), signer=self.__get_signer()
        )

    def _get_client(self):
        if self.__client is None:
            self.__set_client()

        return self.__client

    def set_application_name(self):
        self._function_application_name = (
            os.environ["RESOURCE_NAME_PREFIX"] + "_transformer_functions"
        )

    def get_function_application_name(self):
        if self._function_application_name is None:
            self.set_application_name()
        return self._function_application_name

    def get_application_id(self):
        if self._function_application_id is None:
            self._application_exists(self.get_function_application_name())
        return self._function_application_id

    def _application_exists(self, display_name):
        list_applications_response = self._get_client().list_applications(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"],
            display_name=display_name,
        )
        if len(list_applications_response.data) > 0:
            self._function_application_id = list_applications_response.data[0].id
            return True
        return False

    def _function_exists(self, display_name):
        list_functions_response = self._get_client().list_functions(
            application_id=self.get_application_id(), display_name=display_name
        )
        if len(list_functions_response.data) > 0:
            if "file_to_ts" in display_name:
                self._file_to_ts_transformer_function_id = list_functions_response.data[0].id
            elif "file_to_state" in display_name:
                self._file_to_state_transformer_function_id = list_functions_response.data[0].id
            elif "stream_to_ts" in display_name:
                self._stream_to_ts_transformer_function_id = list_functions_response.data[0].id
            elif "stream_to_state" in display_name:
                self._stream_to_state_transformer_function_id = list_functions_response.data[0].id
            elif "audit" in display_name:
                self._audit_transformer_function_id = list_functions_response.data[0].id
            return True
        return False


class DfaApplication(BaseFunction):

    def create_functions_application(self, subnet_ids):
        display_name = self.get_function_application_name()
        if not self._application_exists(display_name):
            create_application_response = self._get_client().create_application(
                create_application_details=oci.functions.models.CreateApplicationDetails(
                    compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                    display_name=display_name,
                    subnet_ids=subnet_ids,
                    shape="GENERIC_X86",
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                    config={
                        "DFA_STREAM_ID": os.environ["DFA_STREAM_ID"],
                        "DFA_CONN_RETRY_DELAY": "3",
                        "DFA_SIGNER_TYPE": "principal",
                        "DFA_ADW_DFA_USER_PASSWORD_SECRET_NAME": os.environ[
                            "DFA_ADW_DFA_USER_PASSWORD_SECRET_NAME"
                        ],
                        "DFA_CONN_SERVICE_NAME": "to-be-set",
                        "DFA_CONN_PROTOCOL": "tcps",
                        "DFA_CONN_PORT": "1522",
                        "DFA_NAMESPACE": os.environ["DFA_NAMESPACE"],
                        "DFA_TENANCY_ID": os.environ["DFA_TENANCY_ID"],
                        "DFA_ADW_WALLET_SECRET_NAME": os.environ["DFA_ADW_WALLET_SECRET_NAME"],
                        "DFA_ADW_WALLET_PASSWORD_SECRET_NAME": os.environ[
                            "DFA_ADW_WALLET_PASSWORD_SECRET_NAME"
                        ],
                        "DFA_ADW_EWALLET_PEM_SECRET_NAME": os.environ[
                            "DFA_ADW_EWALLET_PEM_SECRET_NAME"
                        ],
                        "DFA_ADW_DFA_SCHEMA": os.environ["DFA_ADW_DFA_SCHEMA"],
                        "DFA_COMPARTMENT_ID": os.environ["DFA_COMPARTMENT_ID"],
                        "DFA_VAULT_ID": os.environ["DFA_VAULT_ID"],
                        "DFA_CONN_HOST": "to-be-set",
                        "DFA_ADW_INSTANCE_OCID": os.environ["DFA_ADW_INSTANCE_OCID"],
                        "DFA_REALM_KEY": os.environ["DFA_REALM_KEY"],
                        "DFA_STREAM_SERVICE_ENDPOINT": os.environ["DFA_STREAM_SERVICE_ENDPOINT"],
                        "DFA_CONN_RETRY_COUNT": os.environ["DFA_CONN_RETRY_COUNT"],
                        "DFA_REGION_KEY": os.environ["DFA_REGION_KEY"],
                        "DFA_REGION_ID": os.environ["DFA_REGION_ID"],
                    },
                )
            )
            self._function_application_id = create_application_response.data.id
            self.logger.info("Successfully created the Function Application %s", display_name)
        else:
            self.logger.info("Function Application with the name %s already exists", display_name)
        return True


class DfaFileToTsFunction(BaseFunction):
    def get_file_to_ts_transformer_function_id(self):
        if self._file_to_ts_transformer_function_id is None:
            self._function_exists(self._file_to_ts_transformer_function_name)
        return self._file_to_ts_transformer_function_id

    def create_file_to_ts_transformer(self):
        display_name = self._file_to_ts_transformer_function_name
        if not self._function_exists(display_name):
            create_function_response = self._get_client().create_function(
                create_function_details=oci.functions.models.CreateFunctionDetails(
                    display_name=display_name,
                    application_id=self.get_application_id(),
                    memory_in_mbs=512,
                    image=os.environ["DFA_REGION_KEY"].lower()
                    + ".ocir.io/"
                    + os.environ["DFA_NAMESPACE"].lower()
                    + "/"
                    + self._get_artifact_manager().get_latest_image().display_name,
                    timeout_in_seconds=300,
                    provisioned_concurrency_config=(
                        oci.functions.models.NoneProvisionedConcurrencyConfig(strategy="NONE")
                        if int(os.environ["FILE_TO_TS_FUNCTION_PROVISIONED_CONCURRENCY"]) == 0
                        else oci.functions.models.ConstantProvisionedConcurrencyConfig(
                            strategy="CONSTANT",
                            count=int(os.environ["FILE_TO_TS_FUNCTION_PROVISIONED_CONCURRENCY"]),
                        )
                    ),
                    config={"DFA_FUNCTION_NAME": "file_to_ts"},
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self._file_to_ts_transformer_function_id = create_function_response.data.id
            self.logger.info("Successfully created the file_to_ts_transformer function")
        else:
            self.logger.info("file_to_ts_transformer function already exists in this application")
        return True


class DfaFileToStateFunction(BaseFunction):
    def get_file_to_state_transformer_function_id(self):
        if self._file_to_state_transformer_function_id is None:
            self._function_exists(self._file_to_state_transformer_function_name)
        return self._file_to_state_transformer_function_id

    def create_file_to_state_transformer(self):
        display_name = self._file_to_state_transformer_function_name
        if not self._function_exists(display_name):
            create_function_response = self._get_client().create_function(
                create_function_details=oci.functions.models.CreateFunctionDetails(
                    display_name=display_name,
                    application_id=self.get_application_id(),
                    memory_in_mbs=512,
                    image=os.environ["DFA_REGION_KEY"].lower()
                    + ".ocir.io/"
                    + os.environ["DFA_NAMESPACE"].lower()
                    + "/"
                    + self._get_artifact_manager().get_latest_image().display_name,
                    timeout_in_seconds=300,
                    provisioned_concurrency_config=(
                        oci.functions.models.NoneProvisionedConcurrencyConfig(strategy="NONE")
                        if int(os.environ["FILE_TO_STATE_FUNCTION_PROVISIONED_CONCURRENCY"]) == 0
                        else oci.functions.models.ConstantProvisionedConcurrencyConfig(
                            strategy="CONSTANT",
                            count=int(os.environ["FILE_TO_STATE_FUNCTION_PROVISIONED_CONCURRENCY"]),
                        )
                    ),
                    config={"DFA_FUNCTION_NAME": "file"},
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self._file_to_state_transformer_function_id = create_function_response.data.id
            self.logger.info("Successfully created the file_to_state_transformer function")
        else:
            self.logger.info(
                "file_to_state_transformer function already exists in this application"
            )
        return True


class DfaStreamToTsFunction(BaseFunction):
    def get_stream_to_ts_transformer_function_id(self):
        if self._stream_to_ts_transformer_function_id is None:
            self._function_exists(self._stream_to_ts_transformer_function_name)
        return self._stream_to_ts_transformer_function_id

    def create_stream_to_ts_transformer(self):
        display_name = self._stream_to_ts_transformer_function_name
        if not self._function_exists(display_name):
            create_function_response = self._get_client().create_function(
                create_function_details=oci.functions.models.CreateFunctionDetails(
                    display_name=display_name,
                    application_id=self.get_application_id(),
                    memory_in_mbs=512,
                    image=os.environ["DFA_REGION_KEY"].lower()
                    + ".ocir.io/"
                    + os.environ["DFA_NAMESPACE"].lower()
                    + "/"
                    + self._get_artifact_manager().get_latest_image().display_name,
                    timeout_in_seconds=300,
                    provisioned_concurrency_config=(
                        oci.functions.models.NoneProvisionedConcurrencyConfig(strategy="NONE")
                        if int(os.environ["STREAM_TO_TS_FUNCTION_PROVISIONED_CONCURRENCY"]) == 0
                        else oci.functions.models.ConstantProvisionedConcurrencyConfig(
                            strategy="CONSTANT",
                            count=int(os.environ["STREAM_TO_TS_FUNCTION_PROVISIONED_CONCURRENCY"]),
                        )
                    ),
                    config={"DFA_FUNCTION_NAME": "stream_to_ts"},
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self._stream_to_ts_transformer_function_id = create_function_response.data.id
            self.logger.info("Successfully created the stream_to_ts_transformer function")
        else:
            self.logger.info("stream_to_ts_transformer function already exists in this application")
        return True


class DfaStreamToStateFunction(BaseFunction):
    def get_stream_to_state_transformer_function_id(self):
        if self._stream_to_state_transformer_function_id is None:
            self._function_exists(self._stream_to_state_transformer_function_name)
        return self._stream_to_state_transformer_function_id

    def create_stream_to_state_transformer(self):
        display_name = self._stream_to_state_transformer_function_name
        if not self._function_exists(display_name):
            create_function_response = self._get_client().create_function(
                create_function_details=oci.functions.models.CreateFunctionDetails(
                    display_name=display_name,
                    application_id=self.get_application_id(),
                    memory_in_mbs=512,
                    image=os.environ["DFA_REGION_KEY"].lower()
                    + ".ocir.io/"
                    + os.environ["DFA_NAMESPACE"].lower()
                    + "/"
                    + self._get_artifact_manager().get_latest_image().display_name,
                    timeout_in_seconds=300,
                    provisioned_concurrency_config=(
                        oci.functions.models.NoneProvisionedConcurrencyConfig(strategy="NONE")
                        if int(os.environ["STREAM_TO_STATE_FUNCTION_PROVISIONED_CONCURRENCY"]) == 0
                        else oci.functions.models.ConstantProvisionedConcurrencyConfig(
                            strategy="CONSTANT",
                            count=int(
                                os.environ["STREAM_TO_STATE_FUNCTION_PROVISIONED_CONCURRENCY"]
                            ),
                        )
                    ),
                    config={"DFA_FUNCTION_NAME": "stream"},
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self._stream_to_state_transformer_function_id = create_function_response.data.id
            self.logger.info("Successfully created the stream_to_state_transformer function")
        else:
            self.logger.info(
                "stream_to_state_transformer function already exists in this application"
            )
        return True


class DfaAuditTransformerFunctions(BaseFunction):
    def get_audit_transformer_function_id(self):
        if self._audit_transformer_function_id is None:
            self._function_exists(self._audit_transformer_function_name)
        return self._audit_transformer_function_id

    def create_audit_transformer(self):
        display_name = self._audit_transformer_function_name
        if not self._function_exists(display_name):
            create_function_response = self._get_client().create_function(
                create_function_details=oci.functions.models.CreateFunctionDetails(
                    display_name=display_name,
                    application_id=self.get_application_id(),
                    memory_in_mbs=512,
                    image=os.environ["DFA_REGION_KEY"].lower()
                    + ".ocir.io/"
                    + os.environ["DFA_NAMESPACE"].lower()
                    + "/"
                    + self._get_artifact_manager().get_latest_image().display_name,
                    timeout_in_seconds=300,
                    provisioned_concurrency_config=(
                        oci.functions.models.NoneProvisionedConcurrencyConfig(strategy="NONE")
                        if int(os.environ["AUDIT_FUNCTION_PROVISIONED_CONCURRENCY"]) == 0
                        else oci.functions.models.ConstantProvisionedConcurrencyConfig(
                            strategy="CONSTANT",
                            count=int(os.environ["AUDIT_FUNCTION_PROVISIONED_CONCURRENCY"]),
                        )
                    ),
                    config={"DFA_FUNCTION_NAME": "audit"},
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self._audit_transformer_function_id = create_function_response.data.id
            self.logger.info("Successfully created the audit_transformer function")
        else:
            self.logger.info("audit_transformer function already exists in this application")
        return True


class DfaSetupADWFunctionConfigs(BaseFunction):
    def add_adw_connection_string_to_configuration(self, application_ocid):
        self.logger.info("Pulling details for configured ADW instance")
        adw_details = BaseAutonomousDatabase().get_details(os.environ["DFA_ADW_INSTANCE_OCID"])
        connection_host_and_service_name = adw_details.connection_strings.all_connection_strings[
            "HIGH"
        ].split("/")

        self.logger.info("Parsing connection host and service name for configured ADW instance")
        connection_host_and_port = connection_host_and_service_name[0].split(":")
        connection_service_name = connection_host_and_service_name[1]
        connection_host = connection_host_and_port[0]

        ## get existing configs
        self.logger.info("Pulling configurations for DFA OCI Function application")
        application_details = self._get_client().get_application(application_ocid)
        dfa_configs = {}

        if len(application_details.data.config) > 0:
            dfa_configs = application_details.data.config

        self.logger.info(
            "Adding connection host and service name configurations for DFA OCI Function application"
        )
        dfa_configs["DFA_CONN_SERVICE_NAME"] = connection_service_name
        dfa_configs["DFA_CONN_HOST"] = connection_host

        os.environ["DFA_CONN_SERVICE_NAME"] = dfa_configs["DFA_CONN_SERVICE_NAME"]
        os.environ["DFA_CONN_HOST"] = dfa_configs["DFA_CONN_HOST"]

        update_application_response = self._get_client().update_application(
            application_id=application_ocid,
            update_application_details=oci.functions.models.UpdateApplicationDetails(
                config=dfa_configs
            ),
        )

        return update_application_response.data
