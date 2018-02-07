import unittest
from maggma.validator import StandardValidator
from monty.json import MSONable

class ValidatorTests(unittest.TestCase):

    def test_standardvalidator(self):

        class LatticeMock(MSONable):
            def __init__(self, a):
                self.a = a

        class SampleValidator(StandardValidator):

            @property
            def schema(self):
                return {
                    "type": "object",
                    "properties":
                        {
                            "task_id": {"type": "string"},
                            "successful": {"type": "boolean"}
                        },
                    "required": ["task_id", "successful"]
                }

            @property
            def msonable_keypaths(self):
                return {"lattice": LatticeMock}

        validator = SampleValidator()

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