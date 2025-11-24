# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import os
from abc import ABC

import oci

from common.logger.logger import Logger


class BaseAutonomousDatabase(ABC):
    logger = Logger(__name__).get_logger()

    _signer_type = None
    __signer = None
    __config = None
    __client = None

    def __init__(self):
        self._check_environment()

    def __set_config(self):
        if self._signer_type == "user":
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

        if self._signer_type == "user":
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
                self.__signer = oci.auth.signers.InstancePrincipalsDelegationTokenSigner(
                    delegation_token=token
                )

            else:
                self.logger.exception(
                    "Please specify a valid OCI_AUTH_TYPE in config.ini. "
                    "Accepted values are 'security_token_file' or 'delegation_token_file'."
                )
                raise Exception(
                    "Specify a valid OCI_AUTH_TYPE in config.ini. "
                    "Accepted values are 'security_token_file' and 'delegation_token_file'."
                )
        else:
            self.__signer = oci.auth.signers.get_resource_principals_signer()

    def __get_signer(self):
        if self.__signer is None:
            self.__set_signer()

        return self.__signer

    def __set_client(self):
        self.__client = oci.database.DatabaseClient(
            config=self.__get_config(), signer=self.__get_signer()
        )

    def _get_client(self):
        if self.__client is None:
            self.__set_client()

        return self.__client

    def _check_environment(self):
        self.logger.info("Performing envrionment checks for autonomous database manager")
        try:
            self._signer_type = os.environ["DFA_SIGNER_TYPE"]
            self.logger.info(
                "Signer type for autonomous database manager has been set to %s", self._signer_type
            )
        except KeyError:
            self.logger.info(
                "Cannot create autonomous database manager - Environment varaible DFA_SIGNER_TYPE does not exist"
            )

    def get_details(self, ocid):
        self.logger.info("Retrieving ADW instance details")
        response = self._get_client().get_autonomous_database(autonomous_database_id=ocid)
        return response.data

    def generate_wallet(self, ocid, password):
        generate_autonomous_database_wallet_response = self._get_client().generate_autonomous_database_wallet(
            autonomous_database_id=ocid,
            generate_autonomous_database_wallet_details=oci.database.models.GenerateAutonomousDatabaseWalletDetails(
                password=password, generate_type="ALL", is_regional=False
            ),
        )

        # Get the data from response
        return generate_autonomous_database_wallet_response.data.content


class DfaCreateAutonomousDatabase(BaseAutonomousDatabase):
    adw_id = None
    _adw_display_name = None

    def wait_for_active_adw(self):
        response = oci.wait_until(
            self._get_client(),
            self._get_client().get_autonomous_database(self.get_adw_id()),
            evaluate_response=lambda r: r.data.lifecycle_state == "AVAILABLE",
            max_wait_seconds=300,
            max_interval_seconds=10,
        )
        return response

    def __set_adw_display_name(self):
        self._adw_display_name = os.environ["RESOURCE_NAME_PREFIX"] + "_database"

    def get_adw_display_name(self):
        if self._adw_display_name is None:
            self.__set_adw_display_name()
        return self._adw_display_name

    def get_adw_id(self):
        if self.adw_id is None:
            self.__adw_exists(self.get_adw_display_name())
        return self.adw_id

    def __adw_exists(self, display_name):
        list_adw_response = self._get_client().list_autonomous_databases(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"], display_name=display_name
        )
        if len(list_adw_response.data):
            self.adw_id = list_adw_response.data[0].id
            os.environ["DFA_ADW_INSTANCE_OCID"] = list_adw_response.data[0].id
            return True
        return False

    def create_adw(self, adw_password):
        display_name = os.environ["RESOURCE_NAME_PREFIX"] + "_database"
        if not self.__adw_exists(display_name):
            db_name = (
                os.environ["RESOURCE_NAME_PREFIX"]
                .replace("_", "")
                .replace("-", "")
                .replace(" ", "")
                .lower()
                + "database"
            )
            create_adw_response = self._get_client().create_autonomous_database(
                create_autonomous_database_details=oci.database.models.CreateAutonomousDatabaseDetails(
                    admin_password=adw_password,
                    compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                    compute_model="ECPU",
                    compute_count=2,
                    data_storage_size_in_tbs=1,
                    db_version="19c",
                    db_name=db_name,
                    db_workload="DW",
                    display_name=display_name,
                    is_auto_scaling_enabled=True,
                    is_auto_scaling_for_storage_enabled=True,
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self.adw_id = create_adw_response.data.id
            os.environ["DFA_ADW_INSTANCE_OCID"] = create_adw_response.data.id
            self.logger.info("Successfully created the ADW instance %s", display_name)
        else:
            self.logger.info("ADW instance with the name %s already exists", display_name)
        return True
