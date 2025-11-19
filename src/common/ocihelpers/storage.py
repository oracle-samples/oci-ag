# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import os
import time
from abc import ABC

import oci

from common.logger.logger import Logger


class BaseObjectStorage(ABC):
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
        self.__client = oci.object_storage.ObjectStorageClient(
            config=self.__get_config(), signer=self.__get_signer()
        )

    def _get_client(self):
        if self.__client is None:
            self.__set_client()

        return self.__client

    def _check_environment(self):
        self.logger.info("Performing envrionment checks for object storage client")
        try:
            self._signer_type = os.environ["DFA_SIGNER_TYPE"]
            self.logger.info(
                "Signer type for object storage manager has been set to %s", self._signer_type
            )
        except KeyError:
            self.logger.info(
                "Cannot create object storage client - Environment varaible DFA_SIGNER_TYPE does not exist"
            )

    def __get_retry_strategy(self):
        # We can also construct our own retry strategy by using the RetryStrategyBuilder
        retry_strategy_via_constructor = oci.retry.RetryStrategyBuilder(
            max_attempts_check=True,
            max_attempts=20,
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

    def download(self, events_namespace, events_bucket_name, events_object_name):
        download_start_time = time.perf_counter()
        event_object = self._get_client().get_object(
            namespace_name=events_namespace,
            bucket_name=events_bucket_name,
            object_name=events_object_name,
            retry_strategy=self.__get_retry_strategy(),
        )
        download_time = time.perf_counter() - download_start_time
        self.logger.info("Took %f seconds to download file from object storage", download_time)
        return event_object

    def upload_buffer(self, namespace, bucket_name, object_name, buffer):
        upload_start_time = time.perf_counter()
        self._get_client().put_object(
            namespace_name=namespace,
            bucket_name=bucket_name,
            object_name=object_name,
            put_object_body=buffer,
            retry_strategy=self.__get_retry_strategy(),
        )
        upload_time = time.perf_counter() - upload_start_time
        self.logger.info(
            "Took %f seconds to upload batched file to object storage bucket", upload_time
        )

        return True

    def get_objects_by_prefix_and_search_string(
        self, namespace, bucket_name, start_with, search_for
    ):
        keep_searching = True

        objects_found_by_search = []
        next_start_with = None
        while keep_searching:

            if next_start_with is None:
                objects = self._get_client().list_objects(
                    namespace_name=namespace,
                    bucket_name=bucket_name,
                    prefix=start_with,
                )
            else:
                objects = self._get_client().list_objects(
                    namespace_name=namespace,
                    bucket_name=bucket_name,
                    prefix=start_with,
                    start=next_start_with,
                )

            next_start_with = objects.data.next_start_with

            self.logger.info("Total objects returned by search: %d", len(objects.data.objects))
            for object_details in objects.data.objects:
                if search_for in object_details.name:
                    objects_found_by_search.append(object_details)

            if objects.data.next_start_with is None:
                keep_searching = False
                break

        self.logger.info("Total objects for export request: %d", len(objects_found_by_search))

        return objects_found_by_search
