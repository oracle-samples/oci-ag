# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import os

import oci

from common.logger.logger import Logger


class BaseConnector:
    logger = Logger(__name__).get_logger()
    __signer = None
    __config = None
    __client = None
    _audit_sch_id = None
    _audit_sch_name = None
    _stream_to_ts_sch_id = None
    _stream_to_ts_sch_name = None
    _stream_to_state_sch_id = None
    _stream_to_state_sch_name = None

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
        self.__client = oci.sch.ServiceConnectorClient(
            config=self.__get_config(), signer=self.__get_signer()
        )

    def _get_client(self):
        if self.__client is None:
            self.__set_client()

        return self.__client

    def _service_connector_exists(self, display_name):
        list_service_connectors_response = self._get_client().list_service_connectors(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"], display_name=display_name
        )
        if len(list_service_connectors_response.data.items) > 0:
            if "stream_to_ts" in display_name:
                self._stream_to_ts_sch_id = list_service_connectors_response.data.items[0].id
            elif "stream_to_state" in display_name:
                self._stream_to_state_sch_id = list_service_connectors_response.data.items[0].id
            elif "audit" in display_name:
                self._audit_sch_id = list_service_connectors_response.data.items[0].id
            return True
        return False


class DfaAuditConnector(BaseConnector):

    def __set_audit_sch_name(self):
        self._audit_sch_name = os.environ["RESOURCE_NAME_PREFIX"] + "_audit_ch"

    def get_audit_sch_name(self):
        if self._audit_sch_name is None:
            self.__set_audit_sch_name()
        return self._audit_sch_name

    def get_audit_sch_id(self):
        if self._audit_sch_id is None:
            self._service_connector_exists(self.get_audit_sch_name())
        return self._audit_sch_id

    def deactivate_audit_service_connector(self):
        try:
            self._get_client().deactivate_service_connector(
                service_connector_id=self.get_audit_sch_id()
            )
            self.logger.info("Successfully set the audit connector hub to an INACTIVE state")
        except Exception as e:
            self.logger.exception(
                "Exception while setting service connector to INACTIVE state - %s", e
            )
            raise Exception("Exception while setting service connector to INACTIVE state") from e

    def create_audit_sch(self, function_id):
        display_name = self.get_audit_sch_name()
        if not self._service_connector_exists(display_name):
            self._get_client().create_service_connector(
                create_service_connector_details=oci.sch.models.CreateServiceConnectorDetails(
                    compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                    display_name=display_name,
                    source=oci.sch.models.StreamingSourceDetails(
                        kind="streaming",
                        stream_id=os.environ["DFA_STREAM_ID"],
                        cursor=oci.sch.models.LatestStreamingCursor(kind="LATEST"),
                    ),
                    target=oci.sch.models.FunctionsTargetDetails(
                        kind="functions",
                        function_id=function_id,
                        batch_size_in_num=1000,
                        batch_time_in_sec=60,
                    ),
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self.logger.info("Successfully created the audit connector hub %s", display_name)
        else:
            self.logger.info("Audit connector hub with the name %s already exists", display_name)
        return True


class DfaStreamToTsConnector(BaseConnector):
    def __set_stream_to_ts_sch_name(self):
        self._stream_to_ts_sch_name = os.environ["RESOURCE_NAME_PREFIX"] + "_stream_to_ts_ch"

    def get_stream_to_ts_sch_name(self):
        if self._stream_to_ts_sch_name is None:
            self.__set_stream_to_ts_sch_name()
        return self._stream_to_ts_sch_name

    def get_stream_to_ts_sch_id(self):
        if self._stream_to_ts_sch_id is None:
            self._service_connector_exists(self.get_stream_to_ts_sch_name())
        return self._stream_to_ts_sch_id

    def deactivate_stream_to_ts_service_connector(self):
        try:
            self._get_client().deactivate_service_connector(
                service_connector_id=self.get_stream_to_ts_sch_id()
            )
            self.logger.info(
                "Successfully set the stream to timeseries connector hub to an INACTIVE state"
            )
        except Exception as e:
            self.logger.exception(
                "Exception while setting service connector to INACTIVE state - %s", e
            )
            raise Exception("Exception while setting service connector to INACTIVE state") from e

    def create_stream_to_ts_sch(self, function_id):
        display_name = self.get_stream_to_ts_sch_name()
        if not self._service_connector_exists(display_name):
            self._get_client().create_service_connector(
                create_service_connector_details=oci.sch.models.CreateServiceConnectorDetails(
                    compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                    display_name=display_name,
                    source=oci.sch.models.StreamingSourceDetails(
                        kind="streaming",
                        stream_id=os.environ["DFA_STREAM_ID"],
                        cursor=oci.sch.models.LatestStreamingCursor(kind="LATEST"),
                    ),
                    target=oci.sch.models.FunctionsTargetDetails(
                        kind="functions",
                        function_id=function_id,
                        batch_size_in_num=1000,
                        batch_time_in_sec=60,
                    ),
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self.logger.info(
                "Successfully created the stream to timeseries connector hub %s", display_name
            )
        else:
            self.logger.info(
                "Stream to timeseries connector hub with the name %s already exists", display_name
            )
        return True


class DfaStreamToStateConnector(BaseConnector):

    def __set_stream_to_state_sch_name(self):
        self._stream_to_state_sch_name = os.environ["RESOURCE_NAME_PREFIX"] + "_stream_to_state_ch"

    def get_stream_to_state_sch_name(self):
        if self._stream_to_state_sch_name is None:
            self.__set_stream_to_state_sch_name()
        return self._stream_to_state_sch_name

    def get_stream_to_state_sch_id(self):
        if self._stream_to_state_sch_id is None:
            self._service_connector_exists(self.get_stream_to_state_sch_name())
        return self._stream_to_state_sch_id

    def deactivate_stream_to_state_service_connector(self):
        try:
            self._get_client().deactivate_service_connector(
                service_connector_id=self.get_stream_to_state_sch_id()
            )
            self.logger.info(
                "Successfully set the stream to state connector hub to an INACTIVE state"
            )
        except Exception as e:
            self.logger.exception(
                "Exception while setting service connector to INACTIVE state - %s", e
            )
            raise Exception("Exception while setting service connector to INACTIVE state") from e

    def create_stream_to_state_sch(self, function_id):
        display_name = self.get_stream_to_state_sch_name()
        if not self._service_connector_exists(display_name):
            self._get_client().create_service_connector(
                create_service_connector_details=oci.sch.models.CreateServiceConnectorDetails(
                    compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                    display_name=display_name,
                    source=oci.sch.models.StreamingSourceDetails(
                        kind="streaming",
                        stream_id=os.environ["DFA_STREAM_ID"],
                        cursor=oci.sch.models.LatestStreamingCursor(kind="LATEST"),
                    ),
                    target=oci.sch.models.FunctionsTargetDetails(
                        kind="functions",
                        function_id=function_id,
                        batch_size_in_num=1000,
                        batch_time_in_sec=60,
                    ),
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self.logger.info(
                "Successfully created the stream to state connector hub %s", display_name
            )
        else:
            self.logger.info(
                "Stream to state connector hub with the name %s already exists", display_name
            )
        return True
