# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import base64
import json
import unittest
from unittest.mock import MagicMock, patch

from common.ocihelpers.stream import DataEnablementStream
from dfa.etl.stream_transformer import StreamTransformer


class TestStreamTransformer(unittest.TestCase):

    def setUp(self):
        self.env_patcher = patch.dict("os.environ", {"DFA_ADW_DFA_SCHEMA": "DFA"})
        self.env_patcher.start()
        self.addCleanup(self.env_patcher.stop)

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

        self.patcher_stream = patch("dfa.etl.stream_transformer.DataEnablementStream", autospec=True)
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
        self.assertEqual(len(self.transformer._prepared_events), 2)

    def test_transform_messages_calls(self):
        messages = {"TYPE1": {"INSERT": [{}]}}
        self.transformer._set_raw_event_data = MagicMock()
        self.transformer.transform_data = MagicMock()
        self.transformer.transform_messages(messages)
        self.transformer._set_raw_event_data.assert_called_once_with(messages)
        self.transformer.transform_data.assert_called_once()

    def test_non_permission_assignment_v2_message_is_skipped(self):
        messages = {
            "IDENTITY": {
                "CREATE": [
                    {
                        "value": {
                            "headers": {
                                "messageType": "IDENTITY",
                                "operation": "CREATE",
                                "eventTypeVersion": "2.0",
                                "eventTime": "2026-08-03T18:00:00Z",
                                "tenancyId": "tenant-1",
                                "serviceInstanceId": "service-1",
                            },
                            "data": {"id": "identity-1"},
                        }
                    }
                ]
            }
        }

        self.transformer.transform_messages(messages)

        self.assertEqual(self.transformer._prepared_events, [])

    def test_permission_assignment_v1_add_remove_is_transformed(self):
        messages = {
            "PERMISSION_ASSIGNMENT": {
                "UPDATE": [
                    {
                        "value": {
                            "headers": {
                                "messageType": "PERMISSION_ASSIGNMENT",
                                "operation": "UPDATE",
                                "eventTypeVersion": "1.0",
                                "eventTime": "2026-08-07T18:00:00Z",
                                "tenancyId": "tenant-1",
                                "serviceInstanceId": "service-1",
                            },
                            "data": {
                                "targetIdentityId": "target-identity-001",
                                "globalIdentityId": "global-identity-001",
                                "add": [
                                    {
                                        "id": "assignment-001",
                                        "targetIdentityId": None,
                                        "globalIdentityId": None,
                                        "permissionId": "permission-001",
                                        "customAttributes": {"grantDate": 1786100400000},
                                    }
                                ],
                                "remove": [
                                    {
                                        "id": "assignment-002",
                                        "permissionId": "permission-002",
                                        "customAttributes": {"grantDate": 1786100500000},
                                    }
                                ],
                            },
                        }
                    }
                ]
            }
        }

        self.transformer.transform_messages(messages)

        self.assertEqual(len(self.transformer._prepared_events), 2)
        self.assertEqual(self.transformer._prepared_events[0]["assignment_id"], "assignment-001")
        self.assertEqual(
            self.transformer._prepared_events[0]["target_identity_id"],
            "target-identity-001",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["global_identity_id"],
            "global-identity-001",
        )
        self.assertEqual(self.transformer._prepared_events[0]["identity_operation_type"], "add")
        self.assertIsNone(self.transformer._prepared_events[0]["created_on"])
        self.assertEqual(
            json.loads(self.transformer._prepared_events[0]["assignment_attributes"]),
            {"grantDate": 1786100400000},
        )
        self.assertEqual(self.transformer._prepared_events[1]["assignment_id"], "assignment-002")
        self.assertEqual(self.transformer._prepared_events[1]["identity_operation_type"], "remove")
        self.assertIsNone(self.transformer._prepared_events[1]["created_on"])
        self.assertEqual(
            json.loads(self.transformer._prepared_events[1]["assignment_attributes"]),
            {"grantDate": 1786100500000},
        )

    def test_permission_assignment_v1_rows_without_id_are_skipped(self):
        messages = {
            "PERMISSION_ASSIGNMENT": {
                "CREATE": [
                    {
                        "value": {
                            "headers": {
                                "eventTypeVersion": "1.0",
                                "eventTime": "2026-08-07T18:00:00Z",
                                "tenancyId": "tenant-1",
                                "serviceInstanceId": "service-1",
                            },
                            "data": {
                                "add": [
                                    {"permissionId": "permission-without-id"},
                                    {"id": None, "permissionId": "permission-null-id"},
                                    {"id": "assignment-001", "permissionId": "permission-001"},
                                ]
                            },
                        }
                    }
                ]
            }
        }

        self.transformer.transform_messages(messages)

        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(self.transformer._prepared_events[0]["assignment_id"], "assignment-001")

    def test_permission_assignment_v2_rows_without_id_are_skipped(self):
        messages = {
            "PERMISSION_ASSIGNMENT": {
                "CREATE": [
                    {
                        "value": {
                            "headers": {
                                "eventTypeVersion": "2.0",
                                "eventTime": "2026-08-07T18:00:00Z",
                                "tenancyId": "tenant-1",
                                "serviceInstanceId": "service-1",
                            },
                            "data": [
                                {"permissionId": "permission-without-id"},
                                {"id": None, "permissionId": "permission-null-id"},
                                {"id": "assignment-001", "permissionId": "permission-001"},
                            ],
                        }
                    }
                ]
            }
        }

        self.transformer.transform_messages(messages)

        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(self.transformer._prepared_events[0]["assignment_id"], "assignment-001")

    def test_permission_assignment_v1_timeseries_rows_without_id_are_retained(self):
        transformer = StreamTransformer(is_timeseries=True)
        messages = {
            "PERMISSION_ASSIGNMENT": {
                "UPDATE": [
                    {
                        "value": {
                            "headers": {
                                "eventTypeVersion": "1.0",
                                "eventTime": "2026-08-07T18:00:00Z",
                                "tenancyId": "tenant-1",
                                "serviceInstanceId": "service-1",
                            },
                            "data": {
                                "targetIdentityId": "target-identity-001",
                                "globalIdentityId": "global-identity-001",
                                "add": [
                                    {
                                        "id": None,
                                        "targetIdentityId": None,
                                        "globalIdentityId": None,
                                        "permissionId": "permission-without-id",
                                    }
                                ],
                                "remove": [{"id": None, "permissionId": "permission-null-id"}],
                            },
                        }
                    }
                ]
            }
        }

        transformer.transform_messages(messages)

        self.assertEqual(len(transformer._prepared_events), 2)
        self.assertIsNone(transformer._prepared_events[0]["assignment_id"])
        self.assertIsNone(transformer._prepared_events[1]["assignment_id"])
        for assignment in transformer._prepared_events:
            self.assertEqual(assignment["target_identity_id"], "target-identity-001")
            self.assertEqual(assignment["global_identity_id"], "global-identity-001")

    def test_permission_assignment_v2_timeseries_rows_without_id_are_retained(self):
        transformer = StreamTransformer(is_timeseries=True)
        messages = {
            "PERMISSION_ASSIGNMENT": {
                "UPDATE": [
                    {
                        "value": {
                            "headers": {
                                "eventTypeVersion": "2.0",
                                "eventTime": "2026-08-07T18:00:00Z",
                                "tenancyId": "tenant-1",
                                "serviceInstanceId": "service-1",
                            },
                            "data": [
                                {"permissionId": "permission-without-id"},
                                {"id": None, "permissionId": "permission-null-id"},
                            ],
                        }
                    }
                ]
            }
        }

        transformer.transform_messages(messages)

        self.assertEqual(len(transformer._prepared_events), 2)

    def test_permission_assignment_v2_flat_list_is_transformed(self):
        messages = {
            "PERMISSION_ASSIGNMENT": {
                "CREATE": [
                    {
                        "value": {
                            "headers": {
                                "messageType": "PERMISSION_ASSIGNMENT",
                                "operation": "CREATE",
                                "eventTypeVersion": "2.0",
                                "eventTime": "2026-08-03T18:00:00Z",
                                "tenancyId": "tenant-1",
                                "serviceInstanceId": "service-1",
                            },
                            "data": [
                                {
                                    "id": "id-1",
                                    "targetIdentityId": "identity-1",
                                    "permissionId": "permission-1",
                                },
                                {
                                    "id": "id-2",
                                    "targetIdentityId": "identity-2",
                                    "permissionId": "permission-2",
                                },
                            ],
                        }
                    }
                ]
            }
        }

        self.transformer.transform_messages(messages)

        self.assertEqual(len(self.transformer._prepared_events), 2)
        self.assertEqual(self.transformer._prepared_events[0]["assignment_id"], "id-1")
        self.assertEqual(self.transformer._prepared_events[0]["target_identity_id"], "identity-1")
        self.assertEqual(self.transformer._prepared_events[1]["assignment_id"], "id-2")

    def test_permission_assignment_v2_json_encoded_flat_list_is_transformed(self):
        assignments = [
            {
                "id": "permission-assignment-001",
                "targetIdentityId": "target-identity-001",
                "globalIdentityId": "global-identity-001",
                "externalId": "jsmith",
                "status": "PROVISIONED",
                "accountStatus": "ACTIVE",
                "targetId": "target-001",
                "targetType": "OCI",
                "granttype": "POLICY",
                "permissionType": "ACCESS_BUNDLE",
                "permissionId": "permission-001",
                "permissionName": "Application Administrator",
                "accessBundleId": "access-bundle-001",
                "accessBundleName": "Application Administration",
                "roleId": "role-001",
                "roleName": "Application Administrator",
                "identityGroupId": "identity-group-001",
                "identityGroupName": "Application Administrators",
                "resourceId": "resource-001",
                "resourceDisplayName": "Production Application",
                "policyId": "policy-001",
                "policyName": "Application Administrator Access",
                "policyRuleId": "policy-rule-001",
                "userLogin": "jsmith",
                "validFrom": 1785758400000,
                "validTo": 1817294400000,
                "created": 1785758400000,
                "lastModified": 1785844800000,
                "customAttributes": {"source": "AG"},
            },
            {
                "id": "permission-assignment-002",
                "targetIdentityId": "target-identity-002",
                "globalIdentityId": "global-identity-002",
                "permissionId": "permission-002",
            },
        ]
        messages = {
            "PERMISSION_ASSIGNMENT": {
                "CREATE": [
                    {
                        "value": {
                            "headers": {
                                "messageType": "PERMISSION_ASSIGNMENT",
                                "operation": "CREATE",
                                "eventTypeVersion": "2.0",
                                "eventTime": "2026-08-03T18:00:00Z",
                                "tenancyId": "tenant-1",
                                "serviceInstanceId": "service-1",
                            },
                            "data": json.dumps(assignments),
                        }
                    }
                ]
            }
        }

        self.transformer.transform_messages(messages)

        self.assertEqual(len(self.transformer._prepared_events), 2)
        first_assignment = self.transformer._prepared_events[0]
        self.assertEqual(first_assignment["assignment_id"], "permission-assignment-001")
        self.assertEqual(first_assignment["target_identity_id"], "target-identity-001")
        self.assertEqual(first_assignment["global_identity_id"], "global-identity-001")
        self.assertEqual(first_assignment["status"], "PROVISIONED")
        self.assertEqual(first_assignment["account_status"], "ACTIVE")
        self.assertEqual(first_assignment["grant_type"], "POLICY")
        self.assertEqual(first_assignment["valid_from"], 1785758400000)
        self.assertEqual(first_assignment["created_on"], 1785758400000)
        self.assertEqual(first_assignment["updated_on"], 1785844800000)
        self.assertEqual(
            json.loads(first_assignment["assignment_attributes"]),
            {"source": "AG"},
        )
        self.assertEqual(
            self.transformer._prepared_events[1]["assignment_id"],
            "permission-assignment-002",
        )

    def test_permission_assignment_v2_created_fixture_timestamps(self):
        messages = self.read_file_content("tests/dfa/etl/test_data/stream/permission_assignment_created_2.0.json")

        self.transformer.transform_messages(messages)

        self.assertEqual(len(self.transformer._prepared_events), 2)
        first_assignment = self.transformer._prepared_events[0]
        self.assertEqual(first_assignment["created_on"], 1785758400000)
        self.assertEqual(first_assignment["updated_on"], 1785758400000)
        self.assertEqual(json.loads(first_assignment["assignment_attributes"]), {})
        second_assignment = self.transformer._prepared_events[1]
        self.assertEqual(second_assignment["created_on"], 1785844800000)
        self.assertEqual(second_assignment["updated_on"], 1785844800000)
        self.assertEqual(json.loads(second_assignment["assignment_attributes"]), {})

    def test_permission_assignment_v2_deleted_fixture_ids(self):
        messages = self.read_file_content("tests/dfa/etl/test_data/stream/permission_assignment_deleted_2.0.json")

        self.transformer.transform_messages(messages)

        self.assertEqual(len(self.transformer._prepared_events), 2)
        self.assertEqual(
            self.transformer._prepared_events[0]["assignment_id"],
            "permission-assignment-001",
        )
        self.assertEqual(
            self.transformer._prepared_events[1]["assignment_id"],
            "permission-assignment-002",
        )
        for assignment in self.transformer._prepared_events:
            self.assertEqual(assignment["operation_type"], "DELETE")
            self.assertEqual(assignment["identity_operation_type"], "remove")

    def test_permission_assignment_v1_deleted_fixture_identity_ids(self):
        messages = self.read_file_content("tests/dfa/etl/test_data/stream/permission_assignment_deleted.json")

        self.transformer.transform_messages(messages)

        self.assertEqual(len(self.transformer._prepared_events), 1)
        assignment = self.transformer._prepared_events[0]
        self.assertEqual(
            assignment["permission_id"],
            "29dbccf64e35.c5be56c42b7e689693e483e34db7ba7d",
        )
        self.assertEqual(
            assignment["target_identity_id"],
            "b00c385f-6e64-433e-b9a7-66c9a42fdca9",
        )
        self.assertEqual(assignment["global_identity_id"], "global-identity-001")
        self.assertEqual(assignment["operation_type"], "DELETE")

    @patch("dfa.etl.stream_transformer.get_query_builder")
    def test_load_data_builds_query_builder_for_prepared_events(self, mock_get_query_builder):
        mock_query_builder = MagicMock()
        mock_get_query_builder.return_value = mock_query_builder
        self.transformer._prepared_events = [
            {
                "event_object_type": "IDENTITY",
                "operation_type": "UPDATE",
                "id": "identity-1",
            }
        ]

        self.transformer.load_data()

        mock_get_query_builder.assert_called_once_with(
            "IDENTITY",
            "UPDATE",
            [
                {
                    "event_object_type": "IDENTITY",
                    "operation_type": "UPDATE",
                    "id": "identity-1",
                }
            ],
            False,
        )
        mock_query_builder.execute_sql_for_events.assert_called_once()
        self.mock_adw_close.assert_not_called()

    @patch("dfa.etl.stream_transformer.get_query_builder")
    def test_load_data_preserves_delete_operation_type(self, mock_get_query_builder):
        mock_query_builder = MagicMock()
        mock_get_query_builder.return_value = mock_query_builder
        self.transformer._prepared_events = [
            {
                "event_object_type": "PERMISSION_ASSIGNMENT",
                "operation_type": "DELETE",
                "assignment_id": "assignment-1",
            }
        ]

        self.transformer.load_data()

        loaded_events = mock_get_query_builder.call_args.args[2]
        self.assertEqual(loaded_events[0]["operation_type"], "DELETE")
        self.assertEqual(loaded_events[0]["event_object_type"], "PERMISSION_ASSIGNMENT")
        mock_query_builder.execute_sql_for_events.assert_called_once()

    @patch("dfa.etl.stream_transformer.get_query_builder")
    def test_load_data_rolls_back_and_closes_on_failure(self, mock_get_query_builder):
        mock_query_builder = MagicMock()
        mock_query_builder.execute_sql_for_events.side_effect = RuntimeError("load failed")
        mock_get_query_builder.return_value = mock_query_builder
        self.transformer._prepared_events = [
            {
                "event_object_type": "IDENTITY",
                "operation_type": "UPDATE",
                "id": "identity-1",
            }
        ]

        with self.assertRaisesRegex(RuntimeError, "load failed"):
            self.transformer.load_data()

        self.mock_adw_rollback_and_close.assert_called_once()
        self.mock_adw_close.assert_not_called()

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

    def _encode_without_padding(self, value):
        return base64.b64encode(value).decode().rstrip("=")

    def test_decode_source_stream_messages_accepts_unpadded_base64(self):
        payload = {"headers": {"messageType": "IDENTITY"}, "data": json.dumps({"id": "identity-1"})}
        messages = [{"value": self._encode_without_padding(json.dumps(payload).encode())}]

        decoded_messages = DataEnablementStream.decode_source_stream_messages(messages)

        self.assertEqual(decoded_messages[0]["value"]["headers"]["messageType"], "IDENTITY")
        self.assertEqual(decoded_messages[0]["value"]["data"]["id"], "identity-1")

    def test_decode_connector_hub_source_stream_messages_accepts_unpadded_base64_layers(self):
        payload = {"headers": {"messageType": "IDENTITY"}, "data": json.dumps({"id": "identity-1"})}
        inner_encoded = self._encode_without_padding(json.dumps(payload).encode()).encode()
        messages = [{"value": self._encode_without_padding(inner_encoded)}]

        decoded_messages = DataEnablementStream.decode_connector_hub_source_stream_messages(messages)

        self.assertEqual(decoded_messages[0]["value"]["headers"]["messageType"], "IDENTITY")
        self.assertEqual(decoded_messages[0]["value"]["data"]["id"], "identity-1")

    def test_access_bundle_changed(self):
        messages = self.read_file_content("tests/dfa/etl/test_data/stream/access_bundle_changed.json")
        self.transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

        self.transformer.transform_messages(messages)
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(self.transformer._prepared_events[0]["event_object_type"], "ACCESS_BUNDLE")
        self.assertEqual(self.transformer._prepared_events[0]["operation_type"], "UPDATE")
        self.assertEqual(
            self.transformer._prepared_events[0]["event_timestamp"],
            "18-Aug-25 06:15:19.032641 PM",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["tenancy_id"],
            "test-tenancy-0f3a6b0c9e2d4f11",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["service_instance_id"],
            "test-service-instance-5d71a8e3c04b49af",
        )

        self.transformer.load_data()
        self.mock_cursor.executemany.assert_called_once()

    def test_approval_workflow_changed(self):
        messages = self.read_file_content("tests/dfa/etl/test_data/stream/approval_workflow_changed.json")
        self.transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

        self.transformer.transform_messages(messages)
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(self.transformer._prepared_events[0]["event_object_type"], "APPROVAL_WORKFLOW")
        self.assertEqual(self.transformer._prepared_events[0]["operation_type"], "UPDATE")
        self.assertEqual(
            self.transformer._prepared_events[0]["summary"],
            "{[beneficiary-manager]}{[beneficiary][owner]}{[manager-chain]}{[group,f64ba5a1-cab3-4dd0-b354-f37dde47a7cd,Test IC Managed (New)]}{[custom-user,globalId.ICF.f0f364e5-2804-4790-ab72-7a5caf405059.0d80c7ccdea4d336bd329ce8dc9ccd3b,Zulma Milner]}",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["event_timestamp"],
            "25-Aug-25 08:47:33.535442 PM",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["tenancy_id"],
            "test-tenancy-a6d2b4e9087f41c3",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["service_instance_id"],
            "test-service-instance-c92f0d17aab34e65",
        )

        self.transformer.load_data()
        self.mock_cursor.executemany.assert_called_once()

    def test_identity_delete(self):
        messages = self.read_file_content("tests/dfa/etl/test_data/stream/identity_deleted.json")
        self.transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

        self.transformer.transform_messages(messages)
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(self.transformer._prepared_events[0]["event_object_type"], "IDENTITY")
        self.assertEqual(self.transformer._prepared_events[0]["operation_type"], "DELETE")
        self.assertEqual(
            self.transformer._prepared_events[0]["tenancy_id"],
            "test-tenancy-0f3a6b0c9e2d4f11",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["service_instance_id"],
            "test-service-instance-5d71a8e3c04b49af",
        )

        self.transformer.load_data()
        self.mock_cursor.executemany.assert_called_once()

    def test_target_identity_delete(self):
        messages = self.read_file_content("tests/dfa/etl/test_data/stream/target_identity_deleted.json")
        self.transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

        self.transformer.transform_messages(messages)
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(self.transformer._prepared_events[0]["event_object_type"], "IDENTITY")
        self.assertEqual(self.transformer._prepared_events[0]["operation_type"], "DELETE")
        self.assertEqual(
            self.transformer._prepared_events[0]["tenancy_id"],
            "test-tenancy-0f3a6b0c9e2d4f11",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["service_instance_id"],
            "test-service-instance-5d71a8e3c04b49af",
        )

        self.transformer.load_data()
        self.mock_cursor.executemany.assert_called_once()

    def test_ownership_collection_delete(self):
        messages = self.read_file_content("tests/dfa/etl/test_data/stream/ownership_collection_delete.json")
        self.transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

        self.transformer.transform_messages(messages)
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(self.transformer._prepared_events[0]["event_object_type"], "OWNERSHIP_COLLECTION")
        self.assertEqual(self.transformer._prepared_events[0]["operation_type"], "DELETE")
        self.assertEqual(
            self.transformer._prepared_events[0]["id"],
            "b00c385f-6e64-433e-b9a7-66c9a42fdca9",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["tenancy_id"],
            "test-tenancy-0f3a6b0c9e2d4f11",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["service_instance_id"],
            "test-service-instance-5d71a8e3c04b49af",
        )

        self.transformer.load_data()
        self.mock_cursor.executemany.assert_called_once()

    def test_identity_unmatched_created(self):
        messages = self.read_file_content("tests/dfa/etl/test_data/stream/identity_unmatched_created.json")
        self.transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

        self.transformer.transform_messages(messages)
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(self.transformer._prepared_events[0]["event_object_type"], "IDENTITY")
        self.assertEqual(self.transformer._prepared_events[0]["operation_type"], "CREATE")
        self.assertEqual(
            self.transformer._prepared_events[0]["event_timestamp"],
            "29-Jan-26 07:00:02.145097 PM",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["tenancy_id"],
            "test-tenancy-78cfe291d0ab43b6",
        )
        self.assertEqual(
            self.transformer._prepared_events[0]["service_instance_id"],
            "test-service-instance-b8a42fd0936e47ac",
        )

        self.transformer.load_data()
        self.mock_cursor.executemany.assert_called_once()

    def test_policy_statement_resource_mapping_created(self):
        messages = self.read_file_content(
            "tests/dfa/etl/test_data/stream/policy_statement_resource_mapping_created.json"
        )
        self.transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

        self.transformer.transform_messages(messages)
        self.assertEqual(len(self.transformer._prepared_events), 4)
        self.assertEqual(self.transformer._prepared_events[3]["event_object_type"], "POLICY_STATEMENT_RESOURCE_MAPPING")
        self.assertEqual(self.transformer._prepared_events[3]["operation_type"], "CREATE")
        self.assertEqual(
            self.transformer._prepared_events[3]["event_timestamp"],
            "29-Jan-26 07:00:02.145097 PM",
        )
        self.assertEqual(
            self.transformer._prepared_events[3]["tenancy_id"],
            "test-tenancy-78cfe291d0ab43b6",
        )
        self.assertEqual(
            self.transformer._prepared_events[3]["service_instance_id"],
            "test-service-instance-b8a42fd0936e47ac",
        )

        self.transformer.load_data()
        self.mock_cursor.executemany.assert_called_once()
