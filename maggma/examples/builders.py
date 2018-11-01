"""
Example builders for testing and general use.
"""
import traceback
from abc import ABCMeta, abstractmethod
from datetime import datetime

from maggma.builders import Builder



def check_indicies(source, target, query=None, incremental=True, logger=None):
    """Return keys to pass to `source.query` to get items."""
    index_checks = [confirm_field_index(target, target.key)]
    if incremental:
        # Ensure [(lu_field, -1), (key, 1)] index on both source and target
        for store in (source, target):
            info = store.collection.index_information().values()
            index_checks.append(
                any(spec == [(store.lu_field, -1), (store.key, 1)] for spec in (index['key'] for index in info)))
    if not all(index_checks):
        index_warning = ("Missing one or more important indices on stores. "
                         "Performance for large stores may be severely degraded. "
                         "Ensure indices on target.key and "
                         "[(store.lu_field, -1), (store.key, 1)] "
                         "for each of source and target.")
        if logger:
            logger.warning(index_warning)

    keys_updated = source_keys_updated(source, target, query)

    return keys_updated



