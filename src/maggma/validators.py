"""
Validator class for document-level validation on Stores. Attach an instance
of a Validator subclass to a Store .schema variable to enable validation on
that Store.
"""

from typing import Dict, List

from jsonschema import ValidationError, validate
from jsonschema.validators import validator_for

from maggma.core import Validator


class JSONSchemaValidator(Validator):
    """
    A validator that allows document validation against a
    provided JSON schema.

    For convenience, the helper method in this module
    `msonable_schema` can be used to create a schema for a
    specific MSONable object, which can be embedded in your
    JSON schema. See the tests for an example of this.
    """

    def __init__(self, schema: Dict, strict: bool = False):
        """
        Args:
            strict: Informs Store how to treat Validator: if
            True, will cause build to fail if invalid document
            is found and raise a ValueError, if False will continue
            build but log an error message. In both cases, invalid
            documents will not be stored.
            schema: A Python dict representation of a JSON
        """
        self._schema = schema
        self._strict = strict

    @property
    def strict(self) -> bool:
        """
        Whether is_valid() should raise a ValidationError or
        simply return False if a document fails validation.
        """
        return self._strict

    @property
    def schema(self) -> Dict:
        """
        Defines a JSON schema for your document,
        which is used by the default `validate_doc()` method.

        This is a standard, with many validators existing, including
        MongoDB's own JSON schema validator (3.6+). Implementing this
        property allows both the document to be validated by a Builder,
        and also makes it possible to enable document-level validation
        inside MongoDB for Mongo-backed Stores.
        """
        return self._schema

    def is_valid(self, doc: Dict) -> bool:
        """
        Returns True or False if validator initialized with
        strict=False, or returns True or raises ValidationError
        if strict=True.

        Args:
            doc (dict): a single document
        """
        try:
            validate(doc, schema=self.schema)
            return True
        except ValidationError:
            if self.strict:
                raise
            return False

    def validation_errors(self, doc: Dict) -> List[str]:
        """
        If document is not valid, provides a list of
        strings to display for why validation has failed

        Returns empty list if the document is valid

        Args:
            doc - document to check
        """

        if self.is_valid(doc):
            return []

        validator = validator_for(self.schema)(self.schema)
        return ["{}: {}".format(".".join(error.absolute_path), error.message) for error in validator.iter_errors(doc)]


def msonable_schema(cls):
    """
    Convenience function to return a JSON Schema for any MSONable class.
    """
    return {
        "type": "object",
        "required": ["@class", "@module"],
        "properties": {
            "@class": {"const": cls.__name__},
            "@module": {"const": cls.__module__},
        },
    }
