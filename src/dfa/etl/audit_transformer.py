# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

from dfa.etl.stream_transformer import StreamTransformer


class AuditTransformer(StreamTransformer):
    transformer_name = "dfa_audit_transformer"
    is_timeseries = True

    def is_valid_object_type(self, object_type):
        return object_type == "AUDIT_EVENTS"
