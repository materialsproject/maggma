# coding: utf-8
"""
Validator class for document-level validation on Stores. Attach an instance
of a Validator subclass to a Store .schema variable to enable validation on
that Store.
"""

from abc import ABC, abstractmethod
from jsonschema import validate, ValidationError
import pydash

class Validator(ABC):
    """
    A generic class to perform document-level validation on Stores.
    Attach a Validator to a Store during initialization, any all documents
    added to the Store will call .validate_doc() before being added.
    """

    def init(self, strict=False):
        """
        Args:
            strict (bool): Informs Store how to treat Validator: if
            True, will cause build to fail if invalid document
            is found and raise a ValueError, if False will continue
            build but log an error message. In both cases, invalid
            documents will not be stored.
        """
        self.strict = strict

    @abstractmethod
    def is_valid(self, doc):
        """
        Returns (bool): True if document valid, False if document
        invalid
        """
        return NotImplementedError


class StandardValidator(Validator):
    """
    A standard Validator, which allows document validation against a
    provided JSON schema, and also can check that specified keys, if present,
    are MSONable (that is, a Python object can be reconstructed).

    To use, subclass StandardSchema and with your own `schema`
    and `msonable_keypaths` keys.
    """

    @property
    @abstractmethod
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
        return {}

    @property
    @abstractmethod
    def msonable_keypaths(self):
        """
        Optional. Used by the default `validate_doc()` method. Define a
        list of keypaths

        Returns (dict): keys should be the keypath to the relevant value
        in your document, and values should be the relevant class
        """
        return {}

    def is_valid(self, doc):

        # JSON validation
        try:
            validate(doc, schema=self.schema)
            valid_schema = True
        except ValidationError:
            valid_schema = False

        # MSONable validation
        def _validate_doc_msonable(doc):
            """
            For every keypath, will return True if either
            keypath does not exist, or keypath does exist
            and a Python object can be reconstructed.
            Otherwise will return False.
            """

            for keypath, obj in self.msonable_keypaths.items():

                dict_to_check = pydash.get(doc, keypath, None)

                if dict_to_check:
                    try:
                        obj.from_dict(dict_to_check)
                    except:
                        return False

            return True

        valid_msonable = _validate_doc_msonable(doc)

        return valid_schema and valid_msonable

