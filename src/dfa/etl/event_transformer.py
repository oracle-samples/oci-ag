# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from dfa.etl.stream_transformer import StreamTransformer


class EventTransformer(StreamTransformer):
    # Preserve the existing stream consumer and offset-tracking identifier.
    transformer_name = "dfa_audit_transformer"

    def __init__(self, is_timeseries=True):
        super().__init__(is_timeseries=is_timeseries)

    def is_valid_object_type(self, object_type):
        return object_type == "AUDIT_EVENTS" or (self.is_timeseries and object_type == "SYSTEM_EVENTS")
