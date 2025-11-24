# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from abc import ABC, abstractmethod
from datetime import datetime

from common.logger.logger import Logger


class BaseEventTransformer(ABC):

    logger = Logger(__name__).get_logger()
    __event_object_type = None
    __operation_type = None
    _tenancy_id = None
    _service_instance_id = None
    _event_timestamp = None

    def __init__(self, event_object_type, operation_type):
        self.__event_object_type = event_object_type
        self.__operation_type = operation_type

    @abstractmethod
    def transform_raw_event(self, raw_event):
        pass

    def get_event_object_type(self):
        return self.__event_object_type

    def get_operation_type(self):
        return self.__operation_type

    def set_tenancy_id(self, tenancy_id):
        self._tenancy_id = tenancy_id

    def _get_tenancy_id(self):
        return self._tenancy_id

    def set_service_instance_id(self, service_instance_id):
        self._service_instance_id = service_instance_id

    def _get_service_instance_id(self):
        return self._service_instance_id

    def set_event_timestamp_for_message(self, event_timestamp):
        date_object = datetime.fromisoformat(event_timestamp)
        self._event_timestamp = date_object.strftime("%d-%b-%y %I:%M:%S.%f %p")

    def _get_event_timestamp(self):
        return self._event_timestamp

    def _access_message_value_data(self, message):
        ## OCI Functions processing messages directly from streams is
        ## in a different structure than OCI Functions as targets in a
        ## Connector Hub

        standardized_message_value_data = None
        if isinstance(message, dict):
            standardized_message_value_data = message["value"]["data"]
            self.set_tenancy_id(message["value"]["headers"]["tenancyId"])
            self.set_service_instance_id(message["value"]["headers"]["serviceInstanceId"])
            self.set_event_timestamp_for_message(message["value"]["headers"]["eventTime"])
        else:
            standardized_message_value_data = message.value["data"]
            self.set_tenancy_id(message.value["headers"]["tenancyId"])
            self.set_service_instance_id(message.value["headers"]["serviceInstanceId"])
            self.set_event_timestamp_for_message(message.value["headers"]["eventTime"])
        return standardized_message_value_data

    def transform_stream_message(self, message):
        return self.transform_raw_event(self._access_message_value_data(message))

    def clean_prepared_events(self, prepared_events_df):
        return prepared_events_df
