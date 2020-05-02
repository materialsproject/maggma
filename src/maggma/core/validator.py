# coding: utf-8
"""
Validator class for document-level validation on Stores. Attach an instance
of a Validator subclass to a Store .schema variable to enable validation on
that Store.
"""

from abc import ABCMeta, abstractmethod
from typing import Dict, List

from monty.json import MSONable


class Validator(MSONable, metaclass=ABCMeta):
    """
    A generic class to perform document-level validation on Stores.
    Attach a Validator to a Store during initialization, any all documents
    added to the Store will call .validate_doc() before being added.
    """

    @abstractmethod
    def is_valid(self, doc: Dict) -> bool:
        """
        Determines if the document is valid

        Args:
            doc: document to check
        """

    @abstractmethod
    def validation_errors(self, doc: Dict) -> List[str]:
        """
        If document is not valid, provides a list of
        strings to display for why validation has failed

        Returns empty list if the document is valid

        Args:
            doc:  document to check
        """
