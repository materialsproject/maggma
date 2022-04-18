from abc import ABCMeta, abstractmethod
from typing import Dict, List

from monty.json import MSONable

from maggma.api.utils import STORE_PARAMS


class QueryOperator(MSONable, metaclass=ABCMeta):
    """
    Base Query Operator class for defining powerfull query language
    in the Materials API
    """

    @abstractmethod
    def query(self) -> STORE_PARAMS:
        """
        The query function that does the work for this query operator
        """

    def meta(self) -> Dict:
        """
        Returns meta data to return with the Response

        Args:
            store: the Maggma Store that the resource uses
            query: the query being executed in this API call
        """
        return {}

    def post_process(self, docs: List[Dict], query: Dict) -> List[Dict]:
        """
        An optional post-processing function for the data

        Args:
            docs: the document results to post-process
            query: the store query dict to use in post-processing
        """
        return docs
