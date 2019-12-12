from typing import List
from maggma.core import Builder


def get_build_order(builders: List[Builder]) -> List[Builder]:
    """
    Returns a list of builders in the order they should run to satisfy
    dependencies

    TODO: For now just do dumb in order since builders should be 
    written to just run over and over again
    """
    return builders
