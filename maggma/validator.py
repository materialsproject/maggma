# coding: utf-8
"""
Validator class for document-level validation on Stores. Attach an instance
of a Validator subclass to a Store .schema variable to enable validation on
that Store.
"""

from abc import ABC, abstractmethod
from jsonschema import validate, ValidationError
from jsonschema.validators import validator_for
import pydash


class Validator(ABC):
    """
    A generic class to perform document-level validation on Stores.
    Attach a Validator to a Store during initialization, any all documents
    added to the Store will call .validate_doc() before being added.
    """

    @abstractmethod
    def is_valid(self, doc):
        """
        Returns (bool): True if document valid, False if document
        invalid
        """
        return NotImplementedError

    @abstractmethod
    def validation_errors(self, doc):
        """
        Returns (bool): if document is not valid, provide a list of
        strings to display for why validation has failed
        """
        return NotImplementedError


class JSONSchemaValidator(Validator):
    """
    A validator that allows document validation against a
    provided JSON schema.

    For convenience, the helper method in this module
    `msonable_schema` can be used to create a schema for a
    specific MSONable object, which can be embedded in your
    JSON schema. See the tests for an example of this.
    """

    def __init__(self, schema, strict=False):
        """
        Args:
            strict (bool): Informs Store how to treat Validator: if
            True, will cause build to fail if invalid document
            is found and raise a ValueError, if False will continue
            build but log an error message. In both cases, invalid
            documents will not be stored.
            schema (dict): A Python dict representation of a JSON
        """
        self._schema = schema
        self._strict = strict

    @property
    def strict(self):
        """
        Whether is_valid() should raise a ValidationError or
        simply return False if a document fails validation.
        :return (bool):
        """
        return self._strict

    @property
    def schema(self):
        """
        Defines a JSON schema for your document,
        which is used by the default `validate_doc()` method.

        This is a standard, with many validators existing, including
        MongoDB's own JSON schema validator (3.6+). Implementing this
        property allows both the document to be validated by a Builder,
        and also makes it possible to enable document-level validation
        inside MongoDB for Mongo-backed Stores.

        Returns (dict): a JSON schema as a Python dict
        """
        return self._schema

    def is_valid(self, doc):
        """
        Returns True or False if validator initialized with
        strict=False, or returns True or raises ValidationError
        if strict=True.

        :param doc (dict): a single document
        :return: True, False or ValidationError
        """
        try:
            validate(doc, schema=self.schema)
            return True
        except ValidationError:
            if self.strict:
                raise
            else:
                return False

    def validation_errors(self, doc):

        if self.is_valid(doc):
            return []

        validator = validator_for(self.schema)(self.schema)
        errors = ["{}: {}".format(".".join(error.absolute_path),
                                  error.message)
                  for error in validator.iter_errors(doc)]

        return errors



def msonable_schema(cls):
    """
    Convenience function to return a JSON Schema for any MSONable class.
    """
    return {
        "type": "object",
        "required": ["@class", "@module"],
        "properties": {
            "@class": {"const": cls.__name__},
            "@module": {"const": cls.__module__}
        }
    }
