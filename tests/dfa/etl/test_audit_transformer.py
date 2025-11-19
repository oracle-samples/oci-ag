# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import unittest
from unittest.mock import MagicMock, patch

from common.ocihelpers.stream import DataEnablementStream

from dfa.etl.audit_transformer import AuditTransformer

class TestAuditTransformer(unittest.TestCase):

    def setUp(self):
        self.adw_patcher = patch('dfa.adw.connection.AdwConnection', autospec=True)
        self.mock_adw_manager = self.adw_patcher.start()
        self.addCleanup(self.adw_patcher.stop)

        self.patcher_stream = patch('dfa.etl.stream_transformer.DataEnablementStream', autospec=True)
        self.mock_stream = self.patcher_stream.start()
        self.addCleanup(self.patcher_stream.stop)

        self.transformer = AuditTransformer()

    def test_is_valid_object_type(self):
        self.assertTrue(self.transformer.is_valid_object_type('AUDIT_EVENTS'))
        self.assertFalse(self.transformer.is_valid_object_type('OTHER_TYPE'))

    def read_file_content(self, jsonl_file_path):
        messages = []
        try:
            with open(jsonl_file_path, 'r') as file:
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
    

    def test_audit_events(self):
        messages = self.read_file_content('tests/dfa/etl/test_data/stream/audit_events.json')
        self.transformer._stream_manager.get_sorted_latest_events = MagicMock(return_value=messages)

        self.transformer.transform_messages(messages)
        self.assertEqual(len(self.transformer._prepared_events), 1)
        self.assertEqual(self.transformer._prepared_events[0]['event_object_type'], 'AUDIT_EVENTS')
        self.assertEqual(self.transformer._prepared_events[0]['operation_type'], 'CREATE')
        self.assertEqual(self.transformer._prepared_events[0]['data'][0]['event_timestamp'], '18-Aug-25 06:15:18.820311 PM')
        self.assertEqual(self.transformer._prepared_events[0]['data'][0]['tenancy_id'], 'ocid1.tenancy.oc1..aaaaaaaazp2vvzjsn6newkqrpkwndxpdoixtqfgyhnf4y24h7d5ny2639054')
        self.assertEqual(self.transformer._prepared_events[0]['data'][0]['service_instance_id'], 'ocid1.agcsgovernanceinstance.oc1.iad.amaaaaaaebkbezqaadpvwolr4raumlz3uxdgczwbqkalpcoo7qcu2r639054')

        with self.assertLogs('dfa.adw.query_builders.base_query_builder', level='INFO') as logs:
            self.transformer.load_data()
            self.assertTrue(self.check_logs(logs.output, '1 audit events'))
