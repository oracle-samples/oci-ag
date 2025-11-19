# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import base64
import json
import os

import oci

from common.logger.logger import Logger
from dfa.adw.connection import AdwConnection
from dfa.adw.query_builders.base_query_builder import StreamOffsetTrackerQueryBuilder


class BaseStream:
    logger = Logger(__name__).get_logger()
    _stream_client = None
    _signer_type = None
    _config = None
    _signer = None
    _cursor = None
    _stream_id = None
    _service_endpoint = None
    _adw_connection = None
    _group_cusor_details = None
    _transformer_name = None

    def __init__(self):
        self._check_environment()

    def _set_stream_client(self):
        self._stream_client = oci.streaming.StreamClient(
            config=self._get_config(),
            signer=self._get_signer(),
            service_endpoint=self._service_endpoint,
        )

    def _get_stream_client(self):
        if self._stream_client is None:
            self._set_stream_client()

        return self._stream_client

    def _set_config(self):
        self._config = {}

        if self._signer_type == "user":
            self._config = oci.config.from_file(
                os.environ["DFA_CONFIG_LOCATION"], os.environ["DFA_CONFIG_PROFILE"]
            )

    def _get_config(self):
        if self._config is None:
            self._set_config()

        return self._config

    def _set_signer(self):
        token = None
        if self._signer_type == "user":
            if os.environ["OCI_AUTH_TYPE"] == "security_token_file":
                config = self._get_config()
                token_file = config["security_token_file"]

                with open(token_file, "r", encoding="utf-8") as f:
                    token = f.read()

                private_key = oci.signer.load_private_key_from_file(config["key_file"])
                self._signer = oci.auth.signers.SecurityTokenSigner(token, private_key)

            elif os.environ["OCI_AUTH_TYPE"] == "delegation_token_file":
                config = self._get_config()
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

    def _get_signer(self):
        if self._signer is None:
            self._set_signer()

        return self._signer

    def _set_cursor(self, offset_type=oci.streaming.models.CreateCursorDetails.TYPE_AFTER_OFFSET):
        if offset_type == oci.streaming.models.CreateCursorDetails.TYPE_AFTER_OFFSET:
            stream_offset = 0

            query = StreamOffsetTrackerQueryBuilder().get_statement_for_latest_unfinished_stream_offset_range(
                self._transformer_name
            )
            AdwConnection.get_cursor().execute(query)

            ## current_stream_offset returned as tuple 0 - id, 1 - offset, 2 end_offset
            current_stream_offset = AdwConnection.get_cursor().fetchone()
            offset_return_index = 1

            if current_stream_offset is None:
                self.logger.info(
                    "No unfinished ranges found - getting last completed offset range for %s application",
                    self._transformer_name,
                )
                query = StreamOffsetTrackerQueryBuilder().get_statement_for_select_max_offset_for_transformer(
                    self._transformer_name
                )

                AdwConnection.get_cursor().execute(query)

                ## current_stream_offset returned as tuple 0 - end_offset
                current_stream_offset = AdwConnection.get_cursor().fetchone()
                offset_return_index = 0
            else:
                self.logger.info(
                    "Unfinished ranges found - getting last completed offset range for %s application with %s ID",
                    self._transformer_name,
                    current_stream_offset[0],
                )

            if current_stream_offset is not None:
                stream_offset = current_stream_offset[offset_return_index]

            self.logger.info("Stream offset set to %s", stream_offset)
            cursor_details = oci.streaming.models.CreateCursorDetails(
                partition="0", type=offset_type, offset=stream_offset
            )
        else:
            cursor_details = oci.streaming.models.CreateCursorDetails(
                partition="0",
                type=offset_type,
            )
        self.logger.info(cursor_details)

        self._cursor = self._get_stream_client().create_cursor(
            stream_id=self._stream_id, create_cursor_details=cursor_details
        )

        return True

    def _get_cursor(self, offset_type):
        self._set_cursor(offset_type)

        return self._cursor

    def _check_environment(self):
        try:
            self._signer_type = os.environ["DFA_SIGNER_TYPE"]
        except KeyError as e:
            self.logger.exception(
                "Cannot create database objects - Environment varaible DFA_SIGNER_TYPE does not exist"
            )
            raise e

    def _get_messages_by_offset(self):
        messages = (
            self._get_stream_client()
            .get_messages(
                stream_id=self._stream_id,
                cursor=self._get_cursor(
                    oci.streaming.models.CreateCursorDetails.TYPE_AFTER_OFFSET
                ).data.value,
                limit=int(os.environ["AUDIT_FILE_SIZE"]),
            )
            .data
        )

        return self.decode_data_feed_messages(messages)

    def _get_messages_by_trim_horizon(self):
        messages = (
            self._get_stream_client()
            .get_messages(
                stream_id=self._stream_id,
                cursor=self._get_cursor(
                    oci.streaming.models.CreateCursorDetails.TYPE_TRIM_HORIZON
                ).data.value,
                limit=int(os.environ["AUDIT_FILE_SIZE"]),
            )
            .data
        )

        return self.decode_data_feed_messages(messages)

    def decode_data_feed_messages(self, messages):
        for encoded_message in messages:
            decoded_value = base64.b64decode(
                base64.b64decode(encoded_message.value.encode()).decode()
            ).decode()
            encoded_message.value = json.loads(decoded_value)
            if "data" in encoded_message.value:
                encoded_message.value["data"] = json.loads(encoded_message.value["data"])

        return messages


class DataEnablementStream(BaseStream):
    _stream_id = None
    _service_endpoint = None
    _latest_batch_start_offset = 0
    _latest_batch_end_offset = 0
    _latest_batch = []
    _messages = []

    def __init__(self, transformer_name):
        self._transformer_name = transformer_name
        self._stream_id = os.environ["DFA_STREAM_ID"]
        self._service_endpoint = os.environ["DFA_STREAM_SERVICE_ENDPOINT"]
        super().__init__()

    def hydrate(self):
        message_threshold = 0
        try:

            self.logger.info("Pull messages using default get_messages method")
            self._messages = self._get_messages_by_offset()

            self.logger.info(
                "Successfully retrieved %d messages from stream.  Now moving on to else...",
                len(self._messages),
            )

            if len(self._messages) == 0:
                self.logger.info("No more messages to process - end processing...")

        except oci.exceptions.ServiceError as service_exception:
            self.logger.info(
                "Cannot retrieve any messages from stream. Lost track of the offset %s?",
                service_exception.message,
            )
            self.logger.info(
                "The system will need to get a time horizon cursor to reset the offset in the system."
            )
            self.logger.info("Retrieve the messages - then switch back to an after_offset cursor")
            self._messages = self._get_messages_by_trim_horizon()

            if len(self._messages) == 0:
                self.logger.info("No more messages to process - end processing...")

        finally:
            if len(self._messages) > message_threshold:
                self.logger.info("Successfully retrieved %d messages", len(self._messages))
                self._latest_batch = self._messages

                self._latest_batch_start_offset = self._messages[0].offset
                self._latest_batch_end_offset = self._messages[len(self._messages) - 1].offset
                self.logger.info(
                    "Saving start (%s) and end (%s) offset information",
                    self._latest_batch_start_offset,
                    self._latest_batch_end_offset,
                )

                query = StreamOffsetTrackerQueryBuilder().get_insert_statement_for_stream_offset(
                    self._latest_batch_start_offset,
                    self._latest_batch_end_offset,
                    self._transformer_name,
                )
                AdwConnection.get_cursor().execute(query)
                AdwConnection.commit()
            else:
                self.logger.info(
                    "Not enough messages received: %d to meet the threshold: %d. Not processing for now...",
                    len(self._messages),
                    message_threshold,
                )

        return True

    def get_unsorted_latest_events(self):
        if len(self._latest_batch) == 0:
            self.hydrate()

        return self._latest_batch

    def complete_offset_range_processing(self):
        self.logger.info("Completing open offset now...")
        offset_completion_update = (
            StreamOffsetTrackerQueryBuilder().get_statement_for_offset_range_completion(
                self._latest_batch_start_offset,
                self._latest_batch_end_offset,
                self._transformer_name,
            )
        )
        AdwConnection.get_cursor().execute(offset_completion_update)
        AdwConnection.commit()

        self._latest_batch = []

        return True

    def get_sorted_latest_events(self):
        sorted_messages = {}
        if len(self._latest_batch) == 0:
            self.hydrate()

        # Sort messages based on event object type and operation
        for message in self._latest_batch:
            if "headers" in message.value:
                if "messageType" in message.value["headers"]:
                    if message.value["headers"]["messageType"] not in sorted_messages:
                        sorted_messages[message.value["headers"]["messageType"]] = {}

                    if (
                        message.value["headers"]["operation"]
                        not in sorted_messages[message.value["headers"]["messageType"]]
                    ):
                        sorted_messages[message.value["headers"]["messageType"]][
                            message.value["headers"]["operation"]
                        ] = []

                    sorted_messages[message.value["headers"]["messageType"]][
                        message.value["headers"]["operation"]
                    ].append(message)

        return sorted_messages

    def sort_data_feed_messages(self, messages):
        sorted_messages = {}
        # Sort messages based on event object type and operation
        for message in messages:
            if "headers" in message.value:
                if "messageType" in message.value["headers"]:
                    if message.value["headers"]["messageType"] not in sorted_messages:
                        sorted_messages[message.value["headers"]["messageType"]] = {}

                    if (
                        message.value["headers"]["operation"]
                        not in sorted_messages[message.value["headers"]["messageType"]]
                    ):
                        sorted_messages[message.value["headers"]["messageType"]][
                            message.value["headers"]["operation"]
                        ] = []

                    sorted_messages[message.value["headers"]["messageType"]][
                        message.value["headers"]["operation"]
                    ].append(message)

        return sorted_messages

    @classmethod
    def sort_connector_hub_source_stream_messages(cls, messages):
        sorted_messages = {}
        # Sort messages based on event object type and operation
        for message in messages:
            if "headers" in message["value"]:
                if "messageType" in message["value"]["headers"]:
                    if message["value"]["headers"]["messageType"] not in sorted_messages:
                        sorted_messages[message["value"]["headers"]["messageType"]] = {}

                    if (
                        message["value"]["headers"]["operation"]
                        not in sorted_messages[message["value"]["headers"]["messageType"]]
                    ):
                        sorted_messages[message["value"]["headers"]["messageType"]][
                            message["value"]["headers"]["operation"]
                        ] = []

                    sorted_messages[message["value"]["headers"]["messageType"]][
                        message["value"]["headers"]["operation"]
                    ].append(message)

        return sorted_messages

    @classmethod
    def decode_connector_hub_source_stream_messages(cls, messages):
        for encoded_message in messages:
            decoded_value = base64.b64decode(
                base64.b64decode(encoded_message["value"].encode()).decode()
            ).decode()
            encoded_message["value"] = json.loads(decoded_value)
            if "data" in encoded_message["value"]:
                encoded_message["value"]["data"] = json.loads(encoded_message["value"]["data"])

        return messages

    @classmethod
    def decode_source_stream_messages(cls, messages):
        for encoded_message in messages:
            decoded_value = base64.b64decode(encoded_message["value"])
            encoded_message["value"] = json.loads(decoded_value)
            if "data" in encoded_message["value"]:
                encoded_message["value"]["data"] = json.loads(encoded_message["value"]["data"])

        return messages
