# coding: utf-8
"""
Tests the validators
"""
import pytest
from monty.json import MSONable

from maggma.validators import JSONSchemaValidator, ValidationError, msonable_schema


class LatticeMock(MSONable):
    """
    A sample MSONable object, just for testing.
    """

    def __init__(self, a):
        self.a = a


@pytest.fixture
def test_schema():
    return {
        "type": "object",
        "properties": {
            "task_id": {"type": "string"},
            "successful": {"type": "boolean"},
            "lattice": msonable_schema(LatticeMock),
        },
        "required": ["task_id", "successful"],
    }


def test_jsonschemevalidator(test_schema):
    """
    Test the JSONSchemaValidator class.
    """

    validator = JSONSchemaValidator(schema=test_schema)
    strict_validator = JSONSchemaValidator(schema=test_schema, strict=True)

    lattice = LatticeMock(5)

    valid_doc = {"task_id": "mp-test", "successful": True, "lattice": lattice.as_dict()}

    invalid_doc_msonable = {
        "task_id": "mp-test",
        "successful": True,
        "lattice": ["I am not a lattice!"],
    }

    invalid_doc_missing_key = {"task_id": "mp-test", "lattice": lattice.as_dict()}

    invalid_doc_wrong_type = {
        "task_id": "mp-test",
        "successful": "true",
        "lattice": lattice.as_dict(),
    }

    assert validator.is_valid(valid_doc)
    assert not validator.is_valid(invalid_doc_msonable)
    assert not validator.is_valid(invalid_doc_missing_key)
    assert not validator.is_valid(invalid_doc_wrong_type)

    with pytest.raises(ValidationError):
        strict_validator.is_valid(invalid_doc_msonable)

    assert validator.validation_errors(valid_doc) == []
    assert validator.validation_errors(invalid_doc_msonable) == [
        "lattice: ['I am not a lattice!'] is not of type 'object'"
    ]

    assert validator.validation_errors(invalid_doc_missing_key) == [
        ": 'successful' is a required property"
    ]

    assert validator.validation_errors(invalid_doc_wrong_type) == [
        "successful: 'true' is not of type 'boolean'"
    ]
