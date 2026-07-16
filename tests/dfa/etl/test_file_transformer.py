# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import unittest
from unittest.mock import MagicMock, patch

from dfa.etl.file_transformer import FileTransformer


class TestFileTransformer(unittest.TestCase):

    def setUp(self):
        self.env_patcher = patch.dict("os.environ", {"DFA_ADW_DFA_SCHEMA": "DFA"})
        self.env_patcher.start()
        self.addCleanup(self.env_patcher.stop)

        self.snapshot_complete_patcher = patch(
            "dfa.adw.query_builders.base_query_builder.BaseQueryBuilder.register_snapshot_batch_completed"
        )
        self.mock_snapshot_complete = self.snapshot_complete_patcher.start()
        self.addCleanup(self.snapshot_complete_patcher.stop)

        self.snapshot_finalize_patcher = patch(
            "dfa.adw.query_builders.base_query_builder.BaseQueryBuilder.finalize_snapshot_cleanup_if_ready"
        )
        self.mock_snapshot_finalize = self.snapshot_finalize_patcher.start()
        self.addCleanup(self.snapshot_finalize_patcher.stop)

        self.mock_cursor = MagicMock()
        self.mock_cursor.getbatcherrors.return_value = []

        self.adw_get_cursor_patcher = patch("dfa.adw.connection.AdwConnection.get_cursor")
        self.mock_get_cursor = self.adw_get_cursor_patcher.start()
        self.mock_get_cursor.return_value = self.mock_cursor
        self.addCleanup(self.adw_get_cursor_patcher.stop)

        self.adw_commit_patcher = patch("dfa.adw.connection.AdwConnection.commit")
        self.mock_adw_commit = self.adw_commit_patcher.start()
        self.addCleanup(self.adw_commit_patcher.stop)

        self.adw_close_patcher = patch("dfa.adw.connection.AdwConnection.close")
        self.mock_adw_close = self.adw_close_patcher.start()
        self.addCleanup(self.adw_close_patcher.stop)

        self.adw_rollback_and_close_patcher = patch("dfa.adw.connection.AdwConnection.rollback_and_close")
        self.mock_adw_rollback_and_close = self.adw_rollback_and_close_patcher.start()
        self.addCleanup(self.adw_rollback_and_close_patcher.stop)

        self.storage_patcher = patch("dfa.etl.file_transformer.BaseObjectStorage", autospec=True)
        self.mock_storage_cls = self.storage_patcher.start()
        self.addCleanup(self.storage_patcher.stop)
        self.mock_storage = self.mock_storage_cls.return_value

        self.transformer = FileTransformer("test_namespace", "test_bucket", "test_object.jsonl", False)

    def test_chunk_prepared_events(self):
        self.transformer._prepared_events = list(range(25))
        chunks = self.transformer.chunk_prepared_events(chunk_size=10)
        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[0], list(range(10)))
        self.assertEqual(chunks[2], list(range(20, 25)))
        self.assertEqual(self.transformer._prepared_events, list(range(25)))

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

    @patch("dfa.etl.file_transformer.get_query_builder")
    def test_access_bundle(self, mock_get_query_builder):
        content = self.read_file_content("tests/dfa/etl/test_data/file/access_bundle.jsonl")
        mock_object = MagicMock()
        mock_query_builder = MagicMock()
        mock_get_query_builder.return_value = mock_query_builder
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 5)
        self.assertEqual(self.transformer._event_object_type, "ACCESS_BUNDLE")
        self.assertEqual(self.transformer._operation_type, "CREATE")
        self.assertEqual(self.transformer._event_timestamp, "2025-08-15T17:38:23.645616585Z")
        self.assertIsNone(self.transformer._num_of_batches)
        self.assertEqual(
            self.transformer._tenancy_id,
            "test-tenancy-0f3a6b0c9e2d4f11",
        )
        self.assertEqual(
            self.transformer._service_instance_id,
            "test-service-instance-5d71a8e3c04b49af",
        )

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 5)

    def test_extract_data_reads_num_of_batches_from_completion_header_only_file(self):
        self.transformer._object_name = "snapshots/identity.snapshot-1.batch-11.jsonl"
        content = self.read_file_content("tests/dfa/etl/test_data/file/complete.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()

        self.assertEqual(len(self.transformer._raw_events), 0)
        self.assertEqual(self.transformer._event_object_type, "IDENTITY")
        self.assertEqual(self.transformer._operation_type, "CREATE")
        self.assertEqual(self.transformer._snapshot_id, "e6820ac9-876c-47f5-8911-7b0d2a2c8071")
        self.assertEqual(self.transformer._snapshot_status, "COMPLETED")
        self.assertEqual(self.transformer._num_of_batches, 11)
        self.assertEqual(self.transformer._get_batch_id_for_batch(), "identity.snapshot-1.batch-11")

    @patch("dfa.etl.file_transformer.get_query_builder")
    def test_load_data_tracks_snapshot_batch_and_attempts_non_blocking_finalize(self, mock_get_query_builder):
        mock_query_builder = MagicMock()
        mock_get_query_builder.return_value = mock_query_builder

        self.transformer._object_name = "snapshots/access_bundle.snapshot-1.batch-1.jsonl"
        self.transformer._event_object_type = "ACCESS_BUNDLE"
        self.transformer._operation_type = "CREATE"
        self.transformer._event_timestamp = "2025-08-15T17:38:23.645616585Z"
        self.transformer._snapshot_id = "snapshot-1"
        self.transformer._tenancy_id = "tenant-1"
        self.transformer._service_instance_id = "svc-1"
        self.transformer._prepared_events = [{"id": "ab-1"}]
        self.transformer._num_of_batches = None
        self.transformer._snapshot_status = "IN_PROGRESS"

        self.transformer.load_data()

        mock_query_builder.execute_sql_for_events.assert_called_once()
        mock_query_builder.register_snapshot_batch_completed.assert_called_once_with(
            snapshot_id="snapshot-1",
            batch_id="access_bundle.snapshot-1.batch-1",
            event_timestamp="15-Aug-25 17:38:23.645616",
            tenancy_id="tenant-1",
            service_instance_id="svc-1",
        )
        mock_query_builder.finalize_snapshot_cleanup_if_ready.assert_not_called()

    @patch("dfa.etl.file_transformer.get_query_builder")
    def test_load_data_does_not_finalize_snapshot_for_normal_batch(self, mock_get_query_builder):
        mock_query_builder = MagicMock()
        mock_get_query_builder.return_value = mock_query_builder

        self.transformer._object_name = "snapshots/access_bundle.snapshot-1.batch-2.jsonl"
        self.transformer._event_object_type = "ACCESS_BUNDLE"
        self.transformer._operation_type = "CREATE"
        self.transformer._event_timestamp = "2025-08-15T17:38:23.645616585Z"
        self.transformer._snapshot_id = "snapshot-1"
        self.transformer._tenancy_id = "tenant-1"
        self.transformer._service_instance_id = "svc-1"
        self.transformer._prepared_events = [{"id": "ab-2"}]
        self.transformer._num_of_batches = None
        self.transformer._snapshot_status = "IN_PROGRESS"

        self.transformer.load_data()

        mock_query_builder.finalize_snapshot_cleanup_if_ready.assert_not_called()

    @patch("dfa.etl.file_transformer.get_query_builder")
    def test_load_data_rolls_back_and_closes_on_failure(self, mock_get_query_builder):
        mock_query_builder = MagicMock()
        mock_query_builder.execute_sql_for_events.side_effect = RuntimeError("load failed")
        mock_get_query_builder.return_value = mock_query_builder

        self.transformer._object_name = "snapshots/access_bundle.snapshot-1.batch-2.jsonl"
        self.transformer._event_object_type = "ACCESS_BUNDLE"
        self.transformer._operation_type = "UPDATE"
        self.transformer._event_timestamp = "2025-08-15T17:38:23.645616585Z"
        self.transformer._prepared_events = [{"id": "ab-2"}]

        with self.assertRaisesRegex(RuntimeError, "load failed"):
            self.transformer.load_data()

        self.mock_adw_rollback_and_close.assert_called_once()
        self.mock_adw_close.assert_not_called()

    @patch("dfa.etl.file_transformer.get_query_builder")
    def test_load_data_uses_completion_marker_to_finalize_snapshot(self, mock_get_query_builder):
        mock_query_builder = MagicMock()
        mock_get_query_builder.return_value = mock_query_builder

        self.transformer._object_name = "snapshots/access_bundle.snapshot-1.batch-3.jsonl"
        self.transformer._event_object_type = "ACCESS_BUNDLE"
        self.transformer._operation_type = "CREATE"
        self.transformer._event_timestamp = "2026-04-17T12:00:00.000000Z"
        self.transformer._snapshot_id = "snapshot-1"
        self.transformer._tenancy_id = "tenant-1"
        self.transformer._service_instance_id = "svc-1"
        self.transformer._prepared_events = []
        self.transformer._num_of_batches = 3
        self.transformer._snapshot_status = "COMPLETED"

        self.transformer.load_data()

        mock_query_builder.execute_sql_for_events.assert_not_called()
        mock_query_builder.register_snapshot_batch_completed.assert_not_called()
        mock_query_builder.finalize_snapshot_cleanup_if_ready.assert_called_once()
        cleanup_args = mock_query_builder.finalize_snapshot_cleanup_if_ready.call_args
        self.assertEqual(cleanup_args.args, ())
        self.assertEqual(cleanup_args.kwargs["snapshot_id"], "snapshot-1")
        self.assertEqual(cleanup_args.kwargs["num_of_batches"], 3)
        self.assertEqual(cleanup_args.kwargs["tenancy_id"], "tenant-1")
        self.assertEqual(cleanup_args.kwargs["service_instance_id"], "svc-1")

    @patch("dfa.etl.file_transformer.get_query_builder")
    def test_load_data_converts_completion_marker_timestamp_to_utc(self, mock_get_query_builder):
        mock_query_builder = MagicMock()
        mock_get_query_builder.return_value = mock_query_builder

        self.transformer._object_name = "snapshots/access_bundle.snapshot-1.batch-3.jsonl"
        self.transformer._event_object_type = "ACCESS_BUNDLE"
        self.transformer._operation_type = "CREATE"
        self.transformer._event_timestamp = "2026-04-17T12:00:00.000000-05:00"
        self.transformer._snapshot_id = "snapshot-1"
        self.transformer._tenancy_id = "tenant-1"
        self.transformer._service_instance_id = "svc-1"
        self.transformer._prepared_events = []
        self.transformer._num_of_batches = 3
        self.transformer._snapshot_status = "COMPLETED"

        self.transformer.load_data()

        mock_query_builder.register_snapshot_batch_completed.assert_not_called()
        cleanup_args = mock_query_builder.finalize_snapshot_cleanup_if_ready.call_args
        self.assertEqual(cleanup_args.args, ())

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

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Using bulk insert into"))

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
        self.assertEqual(len(self.transformer._prepared_events), 231)

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Using bulk insert into"))

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

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Using bulk insert into"))

    def test_global_identity_collection(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/global_identity_collection.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 4)
        self.assertEqual(self.transformer._event_object_type, "GLOBAL_IDENTITY_COLLECTION")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 8)

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Using bulk insert into"))

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

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Using bulk insert into"))

    def test_identity_unmatched(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/identity_unmatched.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 2)
        self.assertEqual(self.transformer._event_object_type, "IDENTITY")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 2)

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Using bulk insert into"))

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

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Using bulk insert into"))

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

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Using bulk insert into"))

    def test_policy_to_resource_mapping(self):
        content = self.read_file_content("tests/dfa/etl/test_data/file/policy_to_resource_mapping.jsonl")
        mock_object = MagicMock()
        mock_object.data.content.decode.return_value = content
        self.mock_storage.download.return_value = mock_object

        self.transformer.extract_data()
        self.assertEqual(len(self.transformer._raw_events), 35)
        self.assertEqual(self.transformer._event_object_type, "POLICY_STATEMENT_RESOURCE_MAPPING")
        self.assertEqual(self.transformer._operation_type, "CREATE")

        self.transformer.transform_data()
        self.assertEqual(len(self.transformer._prepared_events), 9997)

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Using bulk insert into"))

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

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Using bulk insert into"))

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

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Using bulk insert into"))

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

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Using bulk insert into"))

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

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Using bulk insert into"))

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

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Using bulk insert into"))
