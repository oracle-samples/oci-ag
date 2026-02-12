# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from dfa.etl.file_transformer import FileTransformer


class TestFileTransformer(unittest.TestCase):

    def setUp(self):
        self.adw_connection_patcher = patch("dfa.adw.connection.AdwConnection", autospec=True)
        self.mock_adw_connection = self.adw_connection_patcher.start()
        self.addCleanup(self.adw_connection_patcher.stop)

        self.storage_patcher = patch("dfa.etl.file_transformer.BaseObjectStorage", autospec=True)
        self.mock_storage_cls = self.storage_patcher.start()
        self.addCleanup(self.storage_patcher.stop)
        self.mock_storage = self.mock_storage_cls.return_value

        self.transformer = FileTransformer(
            "test_namespace", "test_bucket", "test_object.jsonl", False
        )

    def test_chunk_prepared_events(self):
        self.transformer._prepared_events = list(range(25))
        self.transformer.chunk_prepared_events(chunk_size=10)
        self.assertEqual(len(self.transformer._prepared_events), 3)
        self.assertEqual(self.transformer._prepared_events[0], list(range(10)))
        self.assertEqual(self.transformer._prepared_events[2], list(range(20, 25)))

    def read_file_content(self, jsonl_file_path):
        try:
            with open(jsonl_file_path, "r") as file:
                return file.read()
        except Exception as e:
            return

    def check_logs(self, logs, expected_message):
        for log in logs:
            if expected_message in log:
                return True
        return False

    def test_access_bundle(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/access_bundle.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 5)
        self.assertEqual(self.transformer._event_object_type, "ACCESS_BUNDLE")
        self.assertEqual(self.transformer._operation_type, "CREATE")
        self.assertEqual(self.transformer._event_timestamp, "2025-08-15T17:38:23.645616585Z")
        self.assertEqual(
            self.transformer._tenancy_id,
            "ocid1.tenancy.oc1..aaaaaaaazp2vvzjsn6newkqrpkwndxpdoixtqfgyhnf4y24h7d5ny2639054",
        )
        self.assertEqual(
            self.transformer._service_instance_id,
            "ocid1.agcsgovernanceinstance.oc1.iad.amaaaaaaebkbezqaadpvwolr4raumlz3uxdgczwbqkalpcoo7qcu2r639054",
        )

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 5)
        self.assertIsInstance(self.transformer._prepared_events_df, pd.DataFrame)

        self.transformer.clean_data()
        self.assertEqual(len(self.transformer._prepared_events), 5)
        self.assertTrue(isinstance(self.transformer._prepared_events_df, pd.DataFrame))

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "5 access bundle events"))

    def test_access_guardrail(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/access_guardrail.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 1)
        self.assertEqual(self.transformer._event_object_type, "ACCESS_GUARDRAIL")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertIsInstance(self.transformer._prepared_events_df, pd.DataFrame)

        self.transformer.clean_data()
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertTrue(isinstance(self.transformer._prepared_events_df, pd.DataFrame))

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "1 access guardrail events"))

    def test_cloud_group(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/cloud_group.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 2)
        self.assertEqual(self.transformer._event_object_type, "CLOUD_GROUP")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 233)
        self.assertIsInstance(self.transformer._prepared_events_df, pd.DataFrame)

        self.transformer.clean_data()
        self.assertEqual(len(self.transformer._prepared_events), 233)
        self.assertTrue(isinstance(self.transformer._prepared_events_df, pd.DataFrame))

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "233 cloud group membership events"))

    def test_cloud_policy(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/cloud_policy.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 3)
        self.assertEqual(self.transformer._event_object_type, "CLOUD_POLICY")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 4)
        self.assertIsInstance(self.transformer._prepared_events_df, pd.DataFrame)

        self.transformer.clean_data()
        self.assertEqual(len(self.transformer._prepared_events), 4)
        self.assertTrue(isinstance(self.transformer._prepared_events_df, pd.DataFrame))

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "4 cloud policy events"))

    def test_global_identity_collection(self):
        content = self.read_file_content(
            "tests/dfa/etl/test_data/file/global_identity_collection.jsonl"
        )
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 4)
        self.assertEqual(self.transformer._event_object_type, "GLOBAL_IDENTITY_COLLECTION")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 8)
        self.assertIsInstance(self.transformer._prepared_events_df, pd.DataFrame)

        self.transformer.clean_data()
        self.assertEqual(len(self.transformer._prepared_events), 8)
        self.assertTrue(isinstance(self.transformer._prepared_events_df, pd.DataFrame))

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "8 global identity collection events"))

    def test_identity(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/identity.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 1)
        self.assertEqual(self.transformer._event_object_type, "IDENTITY")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertIsInstance(self.transformer._prepared_events_df, pd.DataFrame)

        self.transformer.clean_data()
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertTrue(isinstance(self.transformer._prepared_events_df, pd.DataFrame))

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "1 identity events"))

    def test_identity_unmatched(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/identity_unmatched.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 1)
        self.assertEqual(self.transformer._event_object_type, "IDENTITY")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertIsInstance(self.transformer._prepared_events_df, pd.DataFrame)

        self.transformer.clean_data()
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertTrue(isinstance(self.transformer._prepared_events_df, pd.DataFrame))

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "1 identity events"))

    def test_permission_assignment(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/permission_assignment.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 2)
        self.assertEqual(self.transformer._event_object_type, "PERMISSION_ASSIGNMENT")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 8)
        self.assertIsInstance(self.transformer._prepared_events_df, pd.DataFrame)

        self.transformer.clean_data()
        self.assertEqual(len(self.transformer._prepared_events), 8)
        self.assertTrue(isinstance(self.transformer._prepared_events_df, pd.DataFrame))

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "8 permission assignment events"))

    def test_permission(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/permission.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 2)
        self.assertEqual(self.transformer._event_object_type, "PERMISSION")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 2)
        self.assertIsInstance(self.transformer._prepared_events_df, pd.DataFrame)

        self.transformer.clean_data()
        self.assertEqual(len(self.transformer._prepared_events), 2)
        self.assertTrue(isinstance(self.transformer._prepared_events_df, pd.DataFrame))

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "2 permission events"))

    def test_policy_to_resource_mapping(self):
        content = self.read_file_content(
            "tests/dfa/etl/test_data/file/policy_to_resource_mapping.jsonl"
        )
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 35)
        self.assertEqual(self.transformer._event_object_type, "POLICY_STATEMENT_RESOURCE_MAPPING")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 9997)
        self.assertIsInstance(self.transformer._prepared_events_df, pd.DataFrame)

        self.transformer.clean_data()
        self.assertEqual(len(self.transformer._prepared_events), 9997)
        self.assertTrue(isinstance(self.transformer._prepared_events_df, pd.DataFrame))

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(
                self.check_logs(logs.output, "9997 policy statement resource mapping events")
            )

    def test_policy(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/policy.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 3)
        self.assertEqual(self.transformer._event_object_type, "POLICY")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 3)
        self.assertIsInstance(self.transformer._prepared_events_df, pd.DataFrame)

        self.transformer.clean_data()
        self.assertEqual(len(self.transformer._prepared_events), 3)
        self.assertTrue(isinstance(self.transformer._prepared_events_df, pd.DataFrame))

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "3 policy events"))

    def test_resource(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/resource.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 2)
        self.assertEqual(self.transformer._event_object_type, "RESOURCE")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 2)
        self.assertIsInstance(self.transformer._prepared_events_df, pd.DataFrame)

        self.transformer.clean_data()
        self.assertEqual(len(self.transformer._prepared_events), 2)
        self.assertTrue(isinstance(self.transformer._prepared_events_df, pd.DataFrame))

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "2 resource events"))

    def test_role(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/role.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 2)
        self.assertEqual(self.transformer._event_object_type, "ROLE")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 3)
        self.assertIsInstance(self.transformer._prepared_events_df, pd.DataFrame)

        self.transformer.clean_data()
        self.assertEqual(len(self.transformer._prepared_events), 3)
        self.assertTrue(isinstance(self.transformer._prepared_events_df, pd.DataFrame))

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "3 role events"))

    def test_ownership_collection(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/ownership_collection.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 7)
        self.assertEqual(self.transformer._event_object_type, "OWNERSHIP_COLLECTION")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 7)
        self.assertIsInstance(self.transformer._prepared_events_df, pd.DataFrame)

        self.transformer.clean_data()
        self.assertEqual(len(self.transformer._prepared_events), 7)
        self.assertTrue(isinstance(self.transformer._prepared_events_df, pd.DataFrame))

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "7 ownership collection events"))

    def test_orchestrated_system(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/orchestrated_system.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 1)
        self.assertEqual(self.transformer._event_object_type, "ORCHESTRATED_SYSTEM")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertIsInstance(self.transformer._prepared_events_df, pd.DataFrame)

        self.transformer.clean_data()
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertTrue(isinstance(self.transformer._prepared_events_df, pd.DataFrame))

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "1 orchestrated system events"))
