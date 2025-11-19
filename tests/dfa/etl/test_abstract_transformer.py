# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import unittest
from unittest.mock import MagicMock, patch
from dfa.etl.abstract_transformer import AbstractTransformer

class DummyTransformer(AbstractTransformer):
    def _set_raw_event_data(self, obj):
        self._raw_events.append(obj)

    def extract_data(self):
        return "extracted"

    def transform_data(self):
        return "transformed"

    def clean_data(self):
        return "cleaned"

    def load_data(self):
        return "loaded"

class TestAbstractTransformer(unittest.TestCase):

    def setUp(self):
        self.transformer = DummyTransformer()
        self.transformer._event_object_type = "IDENTITY"
        self.transformer._operation_type = "CREATE"

    def test_get_event_object_type(self):
        self.assertEqual(self.transformer.get_event_object_type(), "IDENTITY")

    def test_get_operation_type(self):
        self.assertEqual(self.transformer.get_operation_type(), "CREATE")

    def test_is_valid_object_type_true(self):
        self.assertTrue(self.transformer.is_valid_object_type("IDENTITY"))

    def test_is_valid_object_type_false(self):
        self.assertFalse(self.transformer.is_valid_object_type("INVALID_TYPE"))

    def test_transformer_factory(self):
        self.transformer._event_object_type = "IDENTITY"
        self.transformer._operation_type = "CREATE"
        result = self.transformer.transformer_factory()
        self.assertEqual(result.get_event_object_type(), "IDENTITY")
        self.assertEqual(result.get_operation_type(), "CREATE")

        self.transformer._event_object_type = "CLOUD_POLICY"
        self.transformer._operation_type = "UPDATE"
        result = self.transformer.transformer_factory()
        self.assertEqual(result.get_event_object_type(), "CLOUD_POLICY")
        self.assertEqual(result.get_operation_type(), "UPDATE")
