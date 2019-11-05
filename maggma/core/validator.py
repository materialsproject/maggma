# coding: utf-8
"""
Validator class for document-level validation on Stores. Attach an instance
of a Validator subclass to a Store .schema variable to enable validation on
that Store.
"""

from abc import ABCMeta, abstractmethod
from monty.json import MSONable
from typing import Dict


class Validator(MSONable, metaclass=ABCMeta):
    """
    A generic class to perform document-level validation on Stores.
    Attach a Validator to a Store during initialization, any all documents
    added to the Store will call .validate_doc() before being added.
    """

    @abstractmethod
    def is_valid(self, doc: Dict) -> bool:
        """
        Returns (bool): True if document valid, False if document
        invalid
        """
        return NotImplementedError

    @abstractmethod
    def validation_errors(self, doc: Dict) -> bool:
        """
        Returns (bool): if document is not valid, provide a list of
        strings to display for why validation has failed
        """
        return NotImplementedError
