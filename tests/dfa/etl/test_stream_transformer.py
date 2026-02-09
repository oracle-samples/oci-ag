# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import unittest
from unittest.mock import MagicMock, patch

from common.ocihelpers.stream import DataEnablementStream
from dfa.etl.stream_transformer import StreamTransformer


class TestStreamTransformer(unittest.TestCase):

    def setUp(self):
        self.adw_patcher = patch("dfa.adw.connection.AdwConnection", autospec=True)
        self.mock_adw_manager = self.adw_patcher.start()
        self.addCleanup(self.adw_patcher.stop)

        self.patcher_stream = patch(
            "dfa.etl.stream_transformer.DataEnablementStream", autospec=True
        )
        self.mock_stream = self.patcher_stream.start()
        self.addCleanup(self.patcher_stream.stop)

        self.transformer = StreamTransformer(is_timeseries=False)

    def test_append_prepared_event_empty(self):
        self.transformer._prepared_events = []
        self.transformer._append_prepared_event([])
        self.assertTrue(self.transformer._prepared_events == [])
        self.transformer._append_prepared_event(None)
        self.assertTrue(self.transformer._prepared_events == [])

    def test_append_prepared_event_single(self):
        self.transformer._prepared_events = []
        event = {"event_object_type": "TYPE1", "operation_type": "INSERT"}
        self.transformer._append_prepared_event(event)
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(self.transformer._prepared_events[0]["event_object_type"], "TYPE1")

    def test_append_prepared_event_merge(self):
        self.transformer._prepared_events = []
        event1 = {"event_object_type": "TYPE1", "operation_type": "INSERT"}
        event2 = {"event_object_type": "TYPE1", "operation_type": "INSERT"}
        self.transformer._append_prepared_event(event1)
        self.transformer._append_prepared_event(event2)
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(len(self.transformer._prepared_events[0]["data"]), 2)

    def test_transform_messages_calls(self):
        messages = {"TYPE1": {"INSERT": [{}]}}
        self.transformer._start_processing_timer = MagicMock()
        self.transformer._set_raw_event_data = MagicMock()
        self.transformer.transform_data = MagicMock()
        self.transformer.transform_messages(messages)
        self.transformer._start_processing_timer.assert_called_once()
        self.transformer._set_raw_event_data.assert_called_once_with(messages)
        self.transformer.transform_data.assert_called_once()

    def read_file_content(self, jsonl_file_path):
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

    def check_logs(self, logs, expected_message):
        for log in logs:
            if expected_message in log:
                return True
        return False

    def test_access_bundle_changed(self):
        messages = self.read_file_content(
            "tests/dfa/etl/test_data/stream/access_bundle_changed.json"
        )
        self.transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

        self.transformer.transform_messages(messages)
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(self.transformer._prepared_events[0]["event_object_type"], "ACCESS_BUNDLE")
        self.assertEqual(self.transformer._prepared_events[0]["operation_type"], "UPDATE")
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["event_timestamp"],
            "18-Aug-25 06:15:19.032641 PM",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["tenancy_id"],
            "ocid1.tenancy.oc1..aaaaaaaazp2vvzjsn6newkqrpkwndxpdoixtqfgyhnf4y24h7d5ny2639054",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["service_instance_id"],
            "ocid1.agcsgovernanceinstance.oc1.iad.amaaaaaaebkbezqaadpvwolr4raumlz3uxdgczwbqkalpcoo7qcu2r639054",
        )

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "1 access bundle events"))

    def test_approval_workflow_changed(self):
        messages = self.read_file_content(
            "tests/dfa/etl/test_data/stream/approval_workflow_changed.json"
        )
        self.transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

        self.transformer.transform_messages(messages)
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(
            self.transformer._prepared_events[0]["event_object_type"], "APPROVAL_WORKFLOW"
        )
        self.assertEqual(self.transformer._prepared_events[0]["operation_type"], "UPDATE")
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["summary"],
            "{[beneficiary-manager]}{[beneficiary][owner]}{[manager-chain]}{[group,f64ba5a1-cab3-4dd0-b354-f37dde47a7cd,Test IC Managed (New)]}{[custom-user,globalId.ICF.f0f364e5-2804-4790-ab72-7a5caf405059.0d80c7ccdea4d336bd329ce8dc9ccd3b,Zulma Milner]}",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["event_timestamp"],
            "25-Aug-25 08:47:33.535442 PM",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["tenancy_id"],
            "ocid.tenancy.oc1..tenant_test_1_p2",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["service_instance_id"],
            "ocid1.instance.oc1..cc73a..instance_test_1_p2",
        )

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "1 approval workflow events"))

    def test_identity_delete(self):
        messages = self.read_file_content("tests/dfa/etl/test_data/stream/identity_deleted.json")
        self.transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

        self.transformer.transform_messages(messages)
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(self.transformer._prepared_events[0]["event_object_type"], "IDENTITY")
        self.assertEqual(self.transformer._prepared_events[0]["operation_type"], "DELETE")
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["tenancy_id"],
            "ocid1.tenancy.oc1..aaaaaaaazp2vvzjsn6newkqrpkwndxpdoixtqfgyhnf4y24h7d5ny2639054",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["service_instance_id"],
            "ocid1.agcsgovernanceinstance.oc1.iad.amaaaaaaebkbezqaadpvwolr4raumlz3uxdgczwbqkalpcoo7qcu2r639054",
        )

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Row delete for identity delete request"))

    def test_target_identity_delete(self):
        messages = self.read_file_content(
            "tests/dfa/etl/test_data/stream/target_identity_deleted.json"
        )
        self.transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

        self.transformer.transform_messages(messages)
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(self.transformer._prepared_events[0]["event_object_type"], "IDENTITY")
        self.assertEqual(self.transformer._prepared_events[0]["operation_type"], "DELETE")
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["tenancy_id"],
            "ocid1.tenancy.oc1..aaaaaaaazp2vvzjsn6newkqrpkwndxpdoixtqfgyhnf4y24h7d5ny2639054",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["service_instance_id"],
            "ocid1.agcsgovernanceinstance.oc1.iad.amaaaaaaebkbezqaadpvwolr4raumlz3uxdgczwbqkalpcoo7qcu2r639054",
        )

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "Row delete for identity delete request"))

    def test_ownership_collection_delete(self):
        messages = self.read_file_content(
            "tests/dfa/etl/test_data/stream/ownership_collection_delete.json"
        )
        self.transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

        self.transformer.transform_messages(messages)
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(
            self.transformer._prepared_events[0]["event_object_type"], "OWNERSHIP_COLLECTION"
        )
        self.assertEqual(self.transformer._prepared_events[0]["operation_type"], "DELETE")
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["id"],
            "b00c385f-6e64-433e-b9a7-66c9a42fdca9",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["tenancy_id"],
            "ocid1.tenancy.oc1..aaaaaaaazp2vvzjsn6newkqrpkwndxpdoixtqfgyhnf4y24h7d5ny2639054",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["service_instance_id"],
            "ocid1.agcsgovernanceinstance.oc1.iad.amaaaaaaebkbezqaadpvwolr4raumlz3uxdgczwbqkalpcoo7qcu2r639054",
        )

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(
                self.check_logs(logs.output, "Row delete for ownership collection delete request")
            )

    def test_identity_unmatched_created(self):
        messages = self.read_file_content(
            "tests/dfa/etl/test_data/stream/identity_unmatched_created.json"
        )
        self.transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

        self.transformer.transform_messages(messages)
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(self.transformer._prepared_events[0]["event_object_type"], "IDENTITY")
        self.assertEqual(self.transformer._prepared_events[0]["operation_type"], "CREATE")
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["event_timestamp"],
            "29-Jan-26 07:00:02.145097 PM",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["tenancy_id"],
            "ocid1.tenancy.oc1..aaaaaaaazp2vvzjsn6newkqrpkwndxpdoixtqfgyhnf4y24h7d5ny27h6f3q",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["data"][0]["service_instance_id"],
            "ocid1.agcsgovernanceinstance.oc1.iad.amaaaaaaebkbezqaadpvwolr4raumlz3uxdgczwbqkalpcoo7qcu2r53rrha",
        )

        with self.assertLogs("dfa.adw.query_builders.base_query_builder", level="INFO") as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, "1 identity events"))
