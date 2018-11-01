# coding: utf-8
"""
Tests the validators
"""
import unittest
from maggma.validator import JSONSchemaValidator, msonable_schema
from monty.json import MSONable

class ValidatorTests(unittest.TestCase):
    """
    Tests for Validators.
    """

    def test_jsonschemevalidator(self):
        """
        Test the JSONSchemaValidator class.
        """

        class LatticeMock(MSONable):
            """
            A sample MSONable object, just for testing.
            """
            def __init__(self, a):
                self.a = a

        test_schema = {
            "type": "object",
            "properties":
                {
                    "task_id": {"type": "string"},
                    "successful": {"type": "boolean"},
                    "lattice": msonable_schema(LatticeMock)
                },
            "required": ["task_id", "successful"]
        }

        validator = JSONSchemaValidator(schema=test_schema)

        lattice = LatticeMock(5)

        valid_doc = {
            'task_id': 'mp-test',
            'successful': True,
            'lattice': lattice.as_dict()
        }

        invalid_doc_msonable = {
            'task_id': 'mp-test',
            'successful': True,
            'lattice': ['I am not a lattice!']
        }

        invalid_doc_missing_key = {
            'task_id': 'mp-test',
            'lattice': lattice.as_dict()
        }

        invalid_doc_wrong_type = {
            'task_id': 'mp-test',
            'successful': 'true',
            'lattice': lattice.as_dict()
        }

        self.assertTrue(validator.is_valid(valid_doc))
        self.assertFalse(validator.is_valid(invalid_doc_msonable))
        self.assertFalse(validator.is_valid(invalid_doc_missing_key))
        self.assertFalse(validator.is_valid(invalid_doc_wrong_type))

        self.assertListEqual(validator.validation_errors(invalid_doc_msonable),
                             ["lattice: ['I am not a lattice!'] is not of type 'object'"])

        self.assertListEqual(validator.validation_errors(invalid_doc_missing_key),
                             [": 'successful' is a required property"])

        self.assertListEqual(validator.validation_errors(invalid_doc_wrong_type),
                             ["successful: 'true' is not of type 'boolean'"])
