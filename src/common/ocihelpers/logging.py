# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import os

import oci

from common.logger.logger import Logger


class BaseOCILogManagement:
    logger = Logger(__name__).get_logger()
    __signer = None
    __config = None
    __client = None
    _log_group_name = "Default_Group"
    _log_group_id = None

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

    def __set_signer(self):
        token = None

        if os.environ["DFA_SIGNER_TYPE"] == "user":
            if os.environ["OCI_AUTH_TYPE"] == "security_token_file":
                config = self.__get_config()
                token_file = config["security_token_file"]

                with open(token_file, "r", encoding="utf-8") as f:
                    token = f.read()

                private_key = oci.signer.load_private_key_from_file(config["key_file"])
                self.__signer = oci.auth.signers.SecurityTokenSigner(token, private_key)

            elif os.environ["OCI_AUTH_TYPE"] == "delegation_token_file":
                config = self.__get_config()
                token_file = config["delegation_token_file"]
                with open(token_file, "r", encoding="utf-8") as f:
                    token = f.read()
                self.__signer = oci.auth.signers.InstancePrincipalsDelegationTokenSigner(delegation_token=token)

            else:
                self.logger.exception(
                "Please specify a valid OCI_AUTH_TYPE in config.ini. " \
                "Accepted values are 'security_token_file' and 'delegation_token_file'."
                )
                raise Exception("Specify a valid OCI_AUTH_TYPE in config.ini. " \
                "Accepted values are 'security_token_file' and 'delegation_token_file'.")
        else:
            self.__signer = oci.auth.signers.get_resource_principals_signer()

    def __get_signer(self):
        if self.__signer is None:
            self.__set_signer()
        return self.__signer

    def __set_client(self):
        self.__client = oci.logging.LoggingManagementClient(
            config=self.__get_config(), signer=self.__get_signer()
        )

    def _get_client(self):
        if self.__client is None:
            self.__set_client()

        return self.__client

    def _get_log_group_name(self):
        return self._log_group_name

    def _set_log_group_id(self):
        list_log_groups_response = self._get_client().list_log_groups(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"],
            display_name=self._get_log_group_name(),
        )
        if len(list_log_groups_response.data) > 0:
            self._log_group_id = list_log_groups_response.data[0].id

    def _get_log_group_id(self):
        if self._log_group_id is None:
            self._set_log_group_id()
        return self._log_group_id


class DfaFunctionsLogs(BaseOCILogManagement):
    _log_name = None
    _log_id = None

    def _set_log_name(self):
        self._log_group_name = os.environ["RESOURCE_NAME_PREFIX"] + "_transformer_logs"

    def _get_log_name(self):
        if self._log_name is None:
            self._set_log_name()
        return self._log_group_name

    def get_log_id(self):
        if self._log_id is None:
            self._log_exists()
        return self._log_id

    def _log_exists(self):
        list_logs_response = self._get_client().list_logs(
            log_group_id=self._get_log_group_id(), display_name=self._get_log_name()
        )
        if len(list_logs_response.data) > 0:
            self._log_id = list_logs_response.data[0].id
            return True
        return False

    def create_log(self, application_id):
        if not self._log_exists():
            self._get_client().create_log(
                log_group_id=self._get_log_group_id(),
                create_log_details=oci.logging.models.CreateLogDetails(
                    display_name=self._get_log_name(),
                    log_type="SERVICE",
                    is_enabled=True,
                    configuration=oci.logging.models.Configuration(
                        compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                        source=oci.logging.models.OciService(
                            source_type="OCISERVICE",
                            service="functions",
                            resource=application_id,
                            category="invoke",
                        ),
                    ),
                ),
            )
            self.logger.info(
                "Successfully enabled logs %s for the functions application", self._get_log_name()
            )
        else:
            self.logger.info(
                "Logs with the name %s already exists for this log group %s",
                self._get_log_name(),
                self._get_log_group_name(),
            )
        return True
