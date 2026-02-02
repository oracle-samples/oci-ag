# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import os
from unittest.mock import MagicMock

from installer import setup

from common.ocihelpers.stream import DataEnablementStream
from dfa.bootstrap.envvars import bootstrap_local_machine_environment_variables
from dfa.etl.audit_transformer import AuditTransformer
from dfa.etl.file_transformer import FileTransformer
from dfa.etl.stream_transformer import StreamTransformer


def read_file_content(jsonl_file_path):
    messages = []
    try:
        with open(jsonl_file_path, "r") as file:
            message = {}
            message["value"] = file.read()
            messages.append(message)
            messages = DataEnablementStream.decode_source_stream_messages(messages)
            messages = DataEnablementStream.sort_connector_hub_source_stream_messages(messages)
    except Exception as e:
        pass

    return messages


def test_file_transformer(data_type: str):
    namespace = os.getenv("DFA_NAMESPACE")
    bucket_name = os.getenv("DFA_BUCKET_NAME")
    object_name_prefix = "integration-tests"
    transformer = FileTransformer(
        namespace, bucket_name, object_name=f"{object_name_prefix}/{data_type}.jsonl"
    )
    transformer.extract_data()
    transformer.transform_data()
    transformer.load_data()


def test_audit_transformer():
    messages = []
    with open("tests/dfa/etl/test_data/stream/audit_events.json", "r") as file:
        message = {}
        message["value"] = file.read()
        messages.append(message)
        messages = DataEnablementStream.decode_source_stream_messages(messages)
        messages = DataEnablementStream.sort_connector_hub_source_stream_messages(messages)

    transformer = AuditTransformer()
    transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

    transformer.transform_messages(messages)
    transformer.load_data()

def test_identity_stream_transformer():
    messages = []
    with open("tests/dfa/etl/test_data/stream/identity_unmatched_created.json", "r") as file:
        message = {}
        message["value"] = file.read()
        messages.append(message)
        messages = DataEnablementStream.decode_source_stream_messages(messages)
        messages = DataEnablementStream.sort_connector_hub_source_stream_messages(messages)

    transformer = StreamTransformer(is_timeseries=False)
    transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

    transformer.transform_messages(messages)
    transformer.load_data()

    messages = []
    with open("tests/dfa/etl/test_data/stream/target_identity_deleted.json", "r") as file:
        message = {}
        message["value"] = file.read()
        messages.append(message)
        messages = DataEnablementStream.decode_source_stream_messages(messages)
        messages = DataEnablementStream.sort_connector_hub_source_stream_messages(messages)

    transformer = StreamTransformer(is_timeseries=False)
    transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

    transformer.transform_messages(messages)
    transformer.load_data()

def main():

    # To run integration tests:
    # - add [TEST] section to config.ini
    # - change envs to point to your test targets
    # - upload test files to your bucket, under integration-tests
    bootstrap_local_machine_environment_variables("config.ini.mine", "TEST")

    # Uncomment this to re-run some setup steps
    # - add DFA_APPLICATION_OCID to config.ini under [TEST]
    # - set DFA_RECREATE_DFA_ADW_TABLES to True in order to recreate the tables
    # setup(application_ocid=os.getenv('DFA_APPLICATION_OCID')

    test_audit_transformer()
    test_identity_stream_transformer()
    test_file_transformer(data_type="accessBundle")
    test_file_transformer(data_type="accessGuardrail")
    test_file_transformer(data_type="approvalWorkflow")
    test_file_transformer(data_type="cloudGroup")
    test_file_transformer(data_type="cloudPolicy")
    test_file_transformer(data_type="globalIdentity")
    test_file_transformer(data_type="globalIdentityCollection")
    test_file_transformer(data_type="permission")
    test_file_transformer(data_type="permissionAssignment")
    test_file_transformer(data_type="policy")
    test_file_transformer(data_type="policyToResourceMapping")
    test_file_transformer(data_type="resource")
    test_file_transformer(data_type="role")


if __name__ == "__main__":
    main()
