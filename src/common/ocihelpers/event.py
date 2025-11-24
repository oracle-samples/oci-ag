# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import json
import os

import oci

from common.logger.logger import Logger


class BaseEventRule:
    logger = Logger(__name__).get_logger()
    __config = None
    __signer = None
    __client = None

    def __set_config(self):
        if os.environ["DFA_SIGNER_TYPE"] == "user":
            self.__config = oci.config.from_file(
                os.environ["DFA_CONFIG_LOCATION"], os.environ["DFA_CONFIG_PROFILE"]
            )
        else:
            self.__config = {}

    def _get_config(self):
        if self.__config is None:
            self.__set_config()

        return self.__config

    def __set_signer(self):
        token = None

        if os.environ["DFA_SIGNER_TYPE"] == "user":
            if os.environ["OCI_AUTH_TYPE"] == "security_token_file":
                config = self._get_config()
                token_file = config["security_token_file"]

                with open(token_file, "r", encoding="utf-8") as f:
                    token = f.read()

                private_key = oci.signer.load_private_key_from_file(config["key_file"])
                self.__signer = oci.auth.signers.SecurityTokenSigner(token, private_key)

            elif os.environ["OCI_AUTH_TYPE"] == "delegation_token_file":
                config = self._get_config()
                token_file = config["delegation_token_file"]
                with open(token_file, "r", encoding="utf-8") as f:
                    token = f.read()
                self.__signer = oci.auth.signers.InstancePrincipalsDelegationTokenSigner(
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
            self.__signer = oci.auth.signers.get_resource_principals_signer()

    def _get_signer(self):
        if self.__signer is None:
            self.__set_signer()

        return self.__signer

    def __set_client(self):
        self.__client = oci.events.EventsClient(
            config=self._get_config(), signer=self._get_signer()
        )

    def _get_client(self):
        if self.__client is None:
            self.__set_client()

        return self.__client


class DfaCreateFileEventRule(BaseEventRule):
    def rule_exists(self, display_name):
        list_rules_response = self._get_client().list_rules(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"], display_name=display_name
        )
        if len(list_rules_response.data) > 0:
            return True
        return False

    def create_rule(self, function_ocid, df_bucket_name, display_name):
        rule_condition = {
            "eventType": "com.oraclecloud.objectstorage.createobject",
            "data": {
                "additionalDetails": {
                    "bucketName": df_bucket_name,
                }
            },
        }
        rule_json = json.dumps(rule_condition, indent=4)
        if not self.rule_exists(display_name):
            create_rule_details = oci.events.models.CreateRuleDetails(
                display_name=display_name,
                compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                is_enabled=True,
                condition=rule_json,
                actions=oci.events.models.ActionDetailsList(
                    actions=[
                        oci.events.models.CreateFaaSActionDetails(
                            action_type="FAAS", function_id=function_ocid, is_enabled=True
                        )
                    ]
                ),
                freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
            )

            response = self._get_client().create_rule(create_rule_details=create_rule_details)
            self.logger.info(
                "Successfully created event rule: %s (%s)",
                response.data.display_name,
                response.data.id,
            )
        else:
            self.logger.info("Event rule with the name %s already exists", display_name)

        return True
