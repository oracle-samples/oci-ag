# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import atexit
import base64
import os

import oci
import oracledb

from common.logger.logger import Logger


class BaseSecret:
    logger = Logger(__name__).get_logger()
    _signer_type = None
    _signer = None
    __config = None
    __vault_client = None
    __secret_client = None

    def __init__(self):
        self._check_environment()

    def __get_retry_strategy(self):
        # We can also construct our own retry strategy by using the RetryStrategyBuilder
        retry_strategy_via_constructor = oci.retry.RetryStrategyBuilder(
            max_attempts_check=True,
            max_attempts=20,
            # Don't exceed a total of 600 seconds for all service calls
            total_elapsed_time_check=True,
            total_elapsed_time_seconds=75,
            # Wait 45 seconds between attempts
            retry_max_wait_between_calls_seconds=7,
            # Use 2 seconds as the base number for doing sleep time calculations
            retry_base_sleep_time_seconds=4,
            # Retry on certain service errors:
            #
            #   - 5xx code received for the request
            #   - Any 429 (this is signified by the empty array in the retry config)
            #   - 400s where the code is QuotaExceeded or LimitExceeded
            service_error_check=False,
            service_error_retry_on_any_5xx=False,
            service_error_retry_config={
                # 400: ['QuotaExceeded', 'LimitExceeded'],
                429: []
            },
            # Use exponential backoff and retry with full jitter, but on throttles use
            # exponential backoff and retry with equal jitter
            backoff_type=oci.retry.BACKOFF_FULL_JITTER_EQUAL_ON_THROTTLE_VALUE,
        ).get_retry_strategy()

        return retry_strategy_via_constructor

    def _get_secret_ocid(self, secret_name):
        secrets = self.__get_vault_client().list_secrets(
            os.environ["DFA_COMPARTMENT_ID"],
            name=secret_name,
            vault_id=os.environ["DFA_VAULT_ID"],
            retry_strategy=self.__get_retry_strategy(),
        )
        secret_ocid = secrets.data[0].id

        return secret_ocid

    def _get_secret_value(self, secret_ocid):
        response = self.__get_secret_client().get_secret_bundle(
            secret_ocid, retry_strategy=self.__get_retry_strategy()
        )
        base64_secret_content = response.data.secret_bundle_content.content
        base64_secret_bytes = base64_secret_content.encode("ascii")
        base64_message_bytes = base64.b64decode(base64_secret_bytes)
        secret_value = base64_message_bytes.decode("ascii")

        return secret_value

    def _get_wallet_value(self, secret_ocid):
        response = self.__get_secret_client().get_secret_bundle(
            secret_ocid, retry_strategy=self.__get_retry_strategy()
        )
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
                self._signer = oci.auth.signers.SecurityTokenSigner(token, private_key)

            elif os.environ["OCI_AUTH_TYPE"] == "delegation_token_file":
                config = self.__get_config()
                token_file = config["delegation_token_file"]
                with open(token_file, "r", encoding="utf-8") as f:
                    token = f.read()
                self._signer = oci.auth.signers.InstancePrincipalsDelegationTokenSigner(delegation_token=token)

            else:
                self.logger.exception(
                "Please specify a valid OCI_AUTH_TYPE in config.ini. " \
                "Accepted values are 'security_token_file' and 'delegation_token_file'."
                )
                raise Exception("Specify a valid OCI_AUTH_TYPE in config.ini. " \
                "Accepted values are 'security_token_file' and 'delegation_token_file'.")
        else:
            self._signer = oci.auth.signers.get_resource_principals_signer()

    def __get_signer(self):
        if self._signer is None:
            self.__set_signer()

        return self._signer

    def _check_environment(self):
        pass


class AdwSecrets(BaseSecret):

    def get_dfa_user_password(self):
        self.logger.info(
            "Pulling DFA_USER ADW password using secret name %s from the OCI vault",
            os.environ["DFA_ADW_DFA_USER_PASSWORD_SECRET_NAME"],
        )
        password_secret_ocid = self._get_secret_ocid(
            os.environ["DFA_ADW_DFA_USER_PASSWORD_SECRET_NAME"]
        )
        password = self._get_secret_value(password_secret_ocid)

        return password

    def get_wallet_password(self):
        self.logger.info(
            "Pulling ADW password using secret name %s from the OCI vault",
            os.environ["DFA_ADW_WALLET_PASSWORD_SECRET_NAME"],
        )
        password_secret_ocid = self._get_secret_ocid(
            os.environ["DFA_ADW_WALLET_PASSWORD_SECRET_NAME"]
        )
        password = self._get_secret_value(password_secret_ocid)

        return password

    def save_wallet(self):
        wallet_secret_ocid = self._get_secret_ocid(os.environ["DFA_ADW_WALLET_SECRET_NAME"])

        self.logger.info(
            "Pulling ADW wallet using secret name %s from the OCI vault",
            os.environ["DFA_ADW_WALLET_SECRET_NAME"],
        )
        wallet_content = self._get_wallet_value(wallet_secret_ocid)

        with open(os.environ["DFA_ADW_WALLET_SAVE_ABSOLUTE_PATH"], "wb") as wallet:
            self.logger.info(
                "Writing wallet to %s", os.environ["DFA_ADW_WALLET_SAVE_ABSOLUTE_PATH"]
            )
            wallet.write(wallet_content)

    def get_wallet_save_directory(self):
        return os.path.dirname(os.environ["DFA_ADW_WALLET_SAVE_ABSOLUTE_PATH"]) + "/"

    def save_ewallet_pem(self):
        wallet_secret_ocid = self._get_secret_ocid(os.environ["DFA_ADW_EWALLET_PEM_SECRET_NAME"])

        self.logger.info(
            "Pulling ADW wallet using secret name %s from the OCI vault",
            os.environ["DFA_ADW_EWALLET_PEM_SECRET_NAME"],
        )
        wallet_content = self._get_secret_value(wallet_secret_ocid)

        with open(os.environ["DFA_ADW_EWALLET_PEM_SAVE_ABSOLUTE_PATH"], "w", encoding="utf-8") as wallet:
            self.logger.info("Writing wallet to %s",
                             os.environ["DFA_ADW_EWALLET_PEM_SAVE_ABSOLUTE_PATH"])
            wallet.write(wallet_content)

    def get_ewallet_pem_save_directory(self):
        return os.path.dirname(os.environ["DFA_ADW_EWALLET_PEM_SAVE_ABSOLUTE_PATH"]) + "/"

    def _check_environment(self):
        try:
            self._signer_type = os.environ["DFA_SIGNER_TYPE"]
        except KeyError:
            self.logger.exception(
                "Cannot create database objects - Environment varaible DFA_SIGNER_TYPE does not exist"
            )


class AdwConnection:
    logger = Logger(__name__).get_logger()
    __connection = None
    __cursor = None

    @classmethod
    def get_connection(cls):
        if cls.__connection is None:
            cls.logger.info("Creating new connection - pulling all secrets from OCI vault")

            secrets_mgr = AdwSecrets()
            wallet_directory = secrets_mgr.get_wallet_save_directory()
            username = os.environ["DFA_ADW_DFA_SCHEMA"]
            password = secrets_mgr.get_dfa_user_password()
            wallet_password = secrets_mgr.get_wallet_password()
            secrets_mgr.save_wallet()
            secrets_mgr.save_ewallet_pem()

            dsn = f'{os.environ["DFA_CONN_PROTOCOL"]}://{os.environ["DFA_CONN_HOST"]}\
:{os.environ["DFA_CONN_PORT"]}/{os.environ["DFA_CONN_SERVICE_NAME"]}\
?wallet_location={wallet_directory}&retry_count={os.environ["DFA_CONN_RETRY_COUNT"]}\
&retry_delay={os.environ["DFA_CONN_RETRY_DELAY"]}'

            cls.logger.info("Attempting to connect...")
            cls.__connection = oracledb.connect(
                user=username,
                password=password,
                dsn=dsn,
                wallet_password=wallet_password,
            )
            atexit.register(cls._close_all)

            cls.logger.info("Connection established successfully!!")

        return cls.__connection

    @classmethod
    def get_cursor(cls):
        if cls.__cursor is None:
            cls.__cursor = cls.get_connection().cursor()

        return cls.__cursor

    @classmethod
    def _close_all(cls):
        if cls.__cursor:
            try:
                cls.logger.info("Closing cursor(s)")
                cls.__cursor.close()
                cls.__cursor = None
            except Exception as e:
                cls.logger.info("Cannot close cursor - %s", e)
                cls.logger.info("Resetting cursor property so new one can be created")
                cls.__cursor = None
        else:
            cls.logger.info("No cursor to close - resetting connection manager's cursor tracking")
            cls.__cursor = None

        if cls.__connection:
            try:
                cls.logger.info("Closing connection(s)")
                cls.__connection.close()
                cls.__connection = None
            except Exception as e:
                cls.logger.info("Cannot close connection - %s", e)
                cls.logger.info("Resetting connection property so new one can be created")
                cls.__connection = None
        else:
            cls.logger.info("No connection to close - resetting connection manager's connection")
            cls.__connection = None

    @classmethod
    def commit(cls):
        cls.__connection.commit()

        if cls.__cursor is not None:
            cls.logger.info("Open cursor - performing commit")
            cls.__cursor.close()
        else:
            cls.logger.info("No open cursor - nothing to commit")

        cls.__cursor = None

    @classmethod
    def close(cls):
        cls._close_all()
