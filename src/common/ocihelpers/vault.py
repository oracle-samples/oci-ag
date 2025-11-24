# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import base64
import os
import secrets
import string

import oci

from common.logger.logger import Logger


class DfaVault:
    logger = Logger(__name__).get_logger()
    __config = None
    _signer = None
    __kms_vault_client = None
    __kms_mgmt_client = None

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

    def __set_kms_vault_client(self):
        self.__kms_vault_client = oci.key_management.KmsVaultClient(
            config=self.__get_config(), signer=self.__get_signer()
        )

    def _get_kms_vault_client(self):
        if self.__kms_vault_client is None:
            self.__set_kms_vault_client()

        return self.__kms_vault_client

    def __set_kms_mgmt_client(self):
        vault_details = self.get_vault_details()
        self.__kms_mgmt_client = oci.key_management.KmsManagementClient(
            config=self.__get_config(),
            signer=self.__get_signer(),
            service_endpoint=vault_details.management_endpoint,
        )

    def _get_kms_mgmt_client(self):
        if self.__kms_mgmt_client is None:
            self.__set_kms_mgmt_client()

        return self.__kms_mgmt_client

    def get_vault_details(self):
        vault_details = self._get_kms_vault_client().get_vault(vault_id=os.environ["DFA_VAULT_ID"])
        return vault_details.data

    def get_master_encryption_key(self):
        keys = self._get_kms_mgmt_client().list_keys(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"]
        )

        master_key = None
        for key in keys.data:
            if "master" in key.display_name:
                master_key = key
                break

        return master_key


class DfaCreateVault(DfaVault):
    vault_id = None
    master_key_id = None
    master_key_display_name = "master_encryption_key"

    def get_master_key_display_name(self):
        return self.master_key_display_name

    def get_master_key_id(self):
        if self.master_key_id is None:
            self.key_exists(self.get_master_key_display_name())
        return self.master_key_id

    def wait_for_active_vault(self, vault_id):
        response = oci.wait_until(
            self._get_kms_vault_client(),
            self._get_kms_vault_client().get_vault(vault_id),
            evaluate_response=lambda r: r.data.lifecycle_state == "ACTIVE",
            max_wait_seconds=1200,
            max_interval_seconds=10,
        )
        return response

    def __vault_exists(self, display_name):
        list_vaults_response = self._get_kms_vault_client().list_vaults(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"],
        )
        if len(list_vaults_response.data) > 0:
            for vault in list_vaults_response.data:
                if vault.display_name == display_name:
                    self.vault_id = vault.id
                    os.environ["DFA_VAULT_ID"] = vault.id
                    return True
        return False

    def create_vault(self):
        display_name = os.environ["RESOURCE_NAME_PREFIX"] + "_vault"
        if not self.__vault_exists(display_name) and "replace-me" in os.environ["DFA_VAULT_ID"]:
            create_vault_response = self._get_kms_vault_client().create_vault(
                create_vault_details=oci.key_management.models.CreateVaultDetails(
                    compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                    display_name=display_name,
                    vault_type="DEFAULT",
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self.vault_id = create_vault_response.data.id
            os.environ["DFA_VAULT_ID"] = create_vault_response.data.id
            self.wait_for_active_vault(os.environ["DFA_VAULT_ID"])
            self.logger.info("Successfully created the vault %s", display_name)
        else:
            self.logger.info("Vault with the name %s already exists", display_name)
            if "replace-me" not in os.environ["DFA_VAULT_ID"]:
                self.logger.info("Using provided vault id")
        return True

    def wait_for_active_key(self):
        response = oci.wait_until(
            self._get_kms_mgmt_client(),
            self._get_kms_mgmt_client().get_key(self.get_master_key_id()),
            evaluate_response=lambda r: r.data.lifecycle_state == "ENABLED",
            max_wait_seconds=1200,
            max_interval_seconds=10,
        )
        return response

    def key_exists(self, display_name):
        list_keys_response = self._get_kms_mgmt_client().list_keys(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"]
        )

        for key in list_keys_response.data:
            if display_name == key.display_name:
                self.master_key_id = key.id
                return True

        return False

    def create_key(self):
        display_name = self.get_master_key_display_name()

        if not self.key_exists(display_name):
            create_key_response = self._get_kms_mgmt_client().create_key(
                create_key_details=oci.key_management.models.CreateKeyDetails(
                    compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                    display_name=display_name,
                    key_shape=oci.key_management.models.KeyShape(algorithm="AES", length=32),
                    is_auto_rotation_enabled=False,
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self.master_key_id = create_key_response.data.id
            self.wait_for_active_key()
            self.logger.info(
                "Successfully created the master encryption key %s for the vault", display_name
            )
        else:
            self.logger.info(
                "Master encryption key with the name %s already exists for this vault", display_name
            )
        return True


class DfaBaseSecret:
    logger = Logger(__name__).get_logger()
    _signer_type = None
    _signer = None
    __config = None
    __vault_client = None
    __secret_client = None

    def _secret_exists(self, secret_name):
        exists_flag = False

        secrets_list = self.__get_vault_client().list_secrets(
            os.environ["DFA_COMPARTMENT_ID"], name=secret_name
        )

        if len(secrets_list.data) > 0:
            exists_flag = True

        return exists_flag

    def _get_secret_ocid(self, secret_name):
        secrets_list = self.__get_vault_client().list_secrets(
            os.environ["DFA_COMPARTMENT_ID"],
            name=secret_name,
            vault_id=os.environ["DFA_VAULT_ID"],
        )
        secret_ocid = secrets_list.data[0].id

        return secret_ocid

    def _get_secret_value(self, secret_ocid):
        response = self.__get_secret_client().get_secret_bundle(secret_ocid)
        base64_secret_content = response.data.secret_bundle_content.content
        base64_secret_bytes = base64_secret_content.encode("ascii")
        base64_message_bytes = base64.b64decode(base64_secret_bytes)
        secret_value = base64_message_bytes.decode("ascii")

        return secret_value

    def _get_wallet_value(self, secret_ocid):
        response = self.__get_secret_client().get_secret_bundle(secret_ocid)
        base64_secret_content = response.data.secret_bundle_content.content

        return base64.b64decode(base64.b64decode(base64_secret_content))

    def __set_vault_client(self):
        self.__vault_client = oci.vault.VaultsClient(
            config=self.__get_config(), signer=self.__get_signer()
        )

    def __get_vault_client(self):
        if self.__vault_client is None:
            self.__set_vault_client()

        return self.__vault_client

    def __set_secret_client(self):
        self.__secret_client = oci.secrets.SecretsClient(
            config=self.__get_config(), signer=self.__get_signer()
        )

    def __get_secret_client(self):
        if self.__secret_client is None:
            self.__set_secret_client()

        return self.__secret_client

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

    def _create_secret(self, secret_name, password):

        master_key = DfaVault().get_master_encryption_key()

        self.__get_vault_client().create_secret(
            create_secret_details=oci.vault.models.CreateSecretDetails(
                compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                key_id=master_key.id,
                secret_name=secret_name,
                vault_id=os.environ["DFA_VAULT_ID"],
                description="DFA User generated password by the DFA day0 deployment system",
                freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                secret_content=oci.vault.models.Base64SecretContentDetails(
                    content_type="BASE64",
                    name="dfa_user_base64",
                    stage="CURRENT",
                    content=base64.b64encode(password.encode("utf-8")).decode(),
                ),
                enable_auto_generation=False,
            )
        )

        return True


class AdwSecrets(DfaBaseSecret):
    admin_password_name = None

    def dfa_user_password_exists(self):
        self.logger.info("Verifying ADW DFA_USER password exists in the OCI vault")
        exists_flag = self._secret_exists(os.environ["DFA_ADW_DFA_USER_PASSWORD_SECRET_NAME"])
        return exists_flag

    def get_dfa_user_password(self):
        self.logger.info(
            "Pulling ADW password using secret name %s from the OCI vault",
            os.environ["DFA_ADW_DFA_USER_PASSWORD_SECRET_NAME"],
        )
        password_secret_ocid = self._get_secret_ocid(
            os.environ["DFA_ADW_DFA_USER_PASSWORD_SECRET_NAME"]
        )
        password = self._get_secret_value(password_secret_ocid)

        return password

    def save_dfa_user_password(self, password):
        self._create_secret(os.environ["DFA_ADW_DFA_USER_PASSWORD_SECRET_NAME"], password)
        return True

    def dfa_wallet_secret_exists(self):
        self.logger.info("Verifying ADW DFA_USER wallet secret exists in the OCI vault")
        exists_flag = self._secret_exists(os.environ["DFA_ADW_WALLET_SECRET_NAME"])
        return exists_flag

    def create_wallet_secret(self, wallet_content):
        self._create_secret(os.environ["DFA_ADW_WALLET_SECRET_NAME"], wallet_content)
        return True

    def dfa_wallet_pem_secret_exists(self):
        self.logger.info("Verifying ADW DFA_USER wallet PEM secret exists in the OCI vault")
        exists_flag = self._secret_exists(os.environ["DFA_ADW_EWALLET_PEM_SECRET_NAME"])
        return exists_flag

    def create_wallet_pem_secret(self, wallet_pem_content):
        self._create_secret(os.environ["DFA_ADW_EWALLET_PEM_SECRET_NAME"], wallet_pem_content)
        return True

    def dfa_wallet_password_secret_exists(self):
        self.logger.info("Verifying ADW DFA_USER wallet password secret exists in the OCI vault")
        exists_flag = self._secret_exists(os.environ["DFA_ADW_WALLET_PASSWORD_SECRET_NAME"])
        return exists_flag

    def create_wallet_password_secret(self, password):
        self._create_secret(os.environ["DFA_ADW_WALLET_PASSWORD_SECRET_NAME"], password)
        return True

    def __generate_password(self):
        uppercase = string.ascii_letters.upper()
        characters = string.ascii_letters.lower()
        numbers = string.digits

        filler1 = "".join(secrets.choice(characters) for _ in range(6))
        uppercase_letter = "".join(secrets.choice(uppercase) for _ in range(1))
        filler2 = "".join(secrets.choice(characters) for _ in range(6))
        number = "".join(secrets.choice(numbers) for _ in range(1))

        return filler1 + uppercase_letter + filler2 + number

    def set_admin_password_name(self):
        self.admin_password_name = os.environ["DFA_ADW_DFA_USER_PASSWORD_SECRET_NAME"]

    def get_admin_password_name(self):
        if self.admin_password_name is None:
            self.set_admin_password_name()
        return self.admin_password_name

    def dfa_admin_password_secret_exists(self):
        self.logger.info("Verifying ADW ADMIN password secret exists in the OCI vault")
        exists_flag = self._secret_exists(self.get_admin_password_name())
        return exists_flag

    def create_admin_password_secret(self):
        display_name = self.get_admin_password_name()
        if not self.dfa_admin_password_secret_exists():
            admin_password = self.__generate_password()
            self._create_secret(display_name, admin_password)
            self.logger.info("Successfully created secret %s", display_name)
        else:
            self.logger.info("Secret with name %s already exists in this vault", display_name)
        return True

    def get_password(self):
        self.logger.info("Pulling ADW password from the OCI vault")
        password_secret_ocid = self._get_secret_ocid(
            os.environ["DFA_ADW_DFA_USER_PASSWORD_SECRET_NAME"]
        )
        password = self._get_secret_value(password_secret_ocid)

        return password

    def get_wallet_password(self):
        self.logger.info("Pulling ADW WALLET password using from the OCI vault")
        password_secret_ocid = self._get_secret_ocid(
            os.environ["DFA_ADW_WALLET_PASSWORD_SECRET_NAME"]
        )
        password = self._get_secret_value(password_secret_ocid)

        return password

    def get_wallet(self):
        wallet_secret_ocid = self._get_secret_ocid(os.environ["DFA_ADW_WALLET_SECRET_NAME"])

        self.logger.info("Pulling ADW wallet from the OCI vault")
        return self._get_wallet_value(wallet_secret_ocid)

    def get_ewallet_pem(self):
        wallet_secret_ocid = self._get_secret_ocid(os.environ["DFA_ADW_EWALLET_PEM_SECRET_NAME"])
        self.logger.info("Pulling ADW EWALLET from the OCI vault")
        return self._get_secret_value(wallet_secret_ocid)
