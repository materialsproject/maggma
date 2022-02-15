from typing import Callable, Dict, List

from fastapi import Depends, Request, Response

from maggma.api.query_operator import QueryOperator
from maggma.api.utils import STORE_PARAMS, attach_signature


def attach_query_ops(
    function: Callable[[List[STORE_PARAMS]], Dict], query_ops: List[QueryOperator]
) -> Callable[[List[STORE_PARAMS]], Dict]:
    """
    Attach query operators to API compliant function
    The function has to take a list of STORE_PARAMs as the only argument

    Args:
        function: the function to decorate
    """
    attach_signature(
        function,
        annotations={
            **{f"dep{i}": STORE_PARAMS for i, _ in enumerate(query_ops)},
            "request": Request,
            "temp_response": Response,
        },
        defaults={f"dep{i}": Depends(dep.query) for i, dep in enumerate(query_ops)},
    )
    return function
