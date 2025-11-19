# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import os

import oci

from common.logger.logger import Logger


class BaseIam:
    logger = Logger(__name__).get_logger()
    __signer = None
    __config = None
    __client = None
    __log_handler = None
    _functions_dynamic_group_name = None
    _functions_dynamic_group_id = None
    _access_policy_name = None
    _access_policy_id = None

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
        self.__client = oci.identity.IdentityClient(
            config=self.__get_config(), signer=self.__get_signer()
        )

    def _get_client(self):
        if self.__client is None:
            self.__set_client()

        return self.__client

    def __set_functions_dynamic_group_name(self):
        self._functions_dynamic_group_name = os.environ["RESOURCE_NAME_PREFIX"] + "_functions"

    def get_functions_dynamic_group_name(self):
        if self._functions_dynamic_group_name is None:
            self.__set_functions_dynamic_group_name()
        return self._functions_dynamic_group_name

    def get_functions_dynamic_group_id(self):
        if self._functions_dynamic_group_id is None:
            self.dynamic_group_exists(self.get_functions_dynamic_group_name())
        return self._functions_dynamic_group_id

    def dynamic_group_exists(self, display_name):
        list_dynamic_group_exists = self._get_client().list_dynamic_groups(
            compartment_id=os.environ["DFA_TENANCY_ID"], name=display_name
        )
        if len(list_dynamic_group_exists.data) > 0:
            self._functions_dynamic_group_id = list_dynamic_group_exists.data[0].id
            return True
        return False

    def __set_access_policy_name(self):
        self._access_policy_name = os.environ["RESOURCE_NAME_PREFIX"] + "_access"

    def get_access_policy_name(self):
        if self._access_policy_name is None:
            self.__set_access_policy_name()
        return self._access_policy_name

    def get_access_policy_id(self):
        if self._access_policy_id is None:
            self.policy_exists(self.get_access_policy_name())
        return self._access_policy_id

    def policy_exists(self, display_name):
        list_policies_response = self._get_client().list_policies(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"], name=display_name
        )
        if len(list_policies_response.data) > 0:
            self._access_policy_id = list_policies_response.data[0].id
            return True
        return False


class DfaFunctionsDynamicGroup(BaseIam):
    def create_functions_dynamic_group(self, ids):
        display_name = self.get_functions_dynamic_group_name()

        if ids and len(ids) > 0:
            id_clauses = [f"resource.id = '{f_id}'" for f_id in ids]
            matching_rule = "ANY {" + ", ".join(id_clauses) + "}"
        else:
            raise ValueError("ids must be a non-empty list of function OCIDs")

        if os.environ.get('MANUALLY_CREATE_DYNAMIC_GROUP', 'false').lower() == 'false':
            if not self.dynamic_group_exists(display_name):
                create_dynamic_group_response = self._get_client().create_dynamic_group(
                    create_dynamic_group_details=oci.identity.models.CreateDynamicGroupDetails(
                        compartment_id=os.environ["DFA_TENANCY_ID"],
                        name=display_name,
                        description="Dynamic group containing all the transformer function IDs",
                        matching_rule=matching_rule,
                        freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                    )
                )
                self._functions_dynamic_group_id = create_dynamic_group_response.data.id
                self.logger.info("Successfully created the functions dynamic group %s", display_name)
            else:
                self.logger.info(
                    "Functions dynamic group with the name %s already exists", display_name
                )
        else:
            self.logger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            self.logger.info("Please manually create a dynamic group named %s with the following matching rules: "
            "%s", display_name, matching_rule)
            self.logger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return True


class DfaAccessPolicy(BaseIam):

    def create_access_policy(self):
        display_name = self.get_access_policy_name()
        if not self.policy_exists(display_name):
            create_policy_response = self._get_client().create_policy(
                create_policy_details=oci.identity.models.CreatePolicyDetails(
                    compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                    name=display_name,
                    description="Policy statements to give appropriate access to the DFA resources",
                    statements=[
                        "Allow dynamic-group "
                        + os.environ['DYNAMIC_GROUP_DOMAIN']
                        + "/"
                        + self.get_functions_dynamic_group_name()
                        + " to manage vaults in compartment id "
                        + os.environ["DFA_COMPARTMENT_ID"],
                        "Allow dynamic-group "
                        + os.environ['DYNAMIC_GROUP_DOMAIN']
                        + "/"
                        + self.get_functions_dynamic_group_name()
                        + " to manage secret-family in compartment id "
                        + os.environ["DFA_COMPARTMENT_ID"],
                        "Allow dynamic-group "
                        + os.environ['DYNAMIC_GROUP_DOMAIN']
                        + "/"
                        + self.get_functions_dynamic_group_name()
                        + " to use keys in compartment id "
                        + os.environ["DFA_COMPARTMENT_ID"],
                        "Allow dynamic-group "
                        + os.environ['DYNAMIC_GROUP_DOMAIN']
                        + "/"
                        + self.get_functions_dynamic_group_name()
                        + " to manage autonomous-databases in compartment id "
                        + os.environ["DFA_COMPARTMENT_ID"],
                        "Allow dynamic-group "
                        + os.environ['DYNAMIC_GROUP_DOMAIN']
                        + "/"
                        + self.get_functions_dynamic_group_name()
                        + " to manage object-family in compartment id "
                        + os.environ["DFA_COMPARTMENT_ID"],
                        "Allow any-user to {STREAM_READ, STREAM_CONSUME} in compartment id "
                        + os.environ["DFA_COMPARTMENT_ID"]
                        + " where all {request.principal.type='serviceconnector', target.stream.id='"
                        + os.environ["DFA_STREAM_ID"]
                        + "', request.principal.compartment.id='"
                        + os.environ["DFA_COMPARTMENT_ID"]
                        + "'}",
                        "Allow any-user to use fn-function in compartment id "
                        + os.environ["DFA_COMPARTMENT_ID"]
                        + " where all {request.principal.type='serviceconnector', request.principal.compartment.id='"
                        + os.environ["DFA_COMPARTMENT_ID"]
                        + "'}",
                        "Allow any-user to use fn-invocation in compartment id "
                        + os.environ["DFA_COMPARTMENT_ID"]
                        + " where all {request.principal.type='serviceconnector', request.principal.compartment.id='"
                        + os.environ["DFA_COMPARTMENT_ID"]
                        + "'}",
                    ],
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self._functions_dynamic_group_id = create_policy_response.data.id
            self.logger.info("Successfully created the access policy %s", display_name)
        else:
            self.logger.info("Access policy with the name %s already exists", display_name)
        return True
