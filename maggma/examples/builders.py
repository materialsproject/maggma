"""
Example builders for testing and general use.
"""

from datetime import datetime

from pydash import identity

from maggma.builder import Builder
from maggma.utils import confirm_field_index, total_size


def source_keys_updated(source, target):
    """
    Utility for incremental building. Gets a list of source.key values.

    Get source.key values for documents that have been updated on target.
    Ensure [(lu_field, -1),(key, 1)] compound index on both source and target.
    """
    keys_updated = []
    cursor_source = source.query(
        properties=[source.key, source.lu_field],
        sort=[(source.lu_field, -1), (source.key, 1)])
    cursor_target = target.query(
        properties=[target.key, target.lu_field],
        sort=[(target.lu_field, -1), (target.key, 1)])
    tdoc = next(cursor_target, None)
    for sdoc in cursor_source:
        if tdoc is None:
            keys_updated.append(sdoc[source.key])
            continue

        if tdoc[target.key] == sdoc[source.key]:
            if tdoc[target.lu_field] < source.lu_func[0](sdoc[source.lu_field]):
                keys_updated.append(sdoc[source.key])
            tdoc = next(cursor_target, None)
        else:
            keys_updated.append(sdoc[source.key])
    return keys_updated


class MapBuilder(Builder):
    """
    Apply a unary function to yield a target document for each source document.

    Supports incremental building, where a source document gets built only if it
    has newer (by lu_field) data than the corresponding (by key) target
    document.

    """
    def __init__(self, source, target, ufn_as_dict=None,
                 query=None, incremental=True, **kwargs):
        """
        Apply a unary function to each source document.

        Args:
            source (Store): source store
            target (Store): target store
            ufn_as_dict (dict): {@module,@name} of unary function for import, or
                None if subclassing MapBuilder and setting attribute before
                calling super().__init__.
            query (dict): optional query to filter source store
            incremental (bool): whether to use lu_field of source and target
                to get only new/updated documents.
        """
        self.source = source
        self.target = target
        self.ufn_as_dict = ufn_as_dict
        self.incremental = incremental
        self.query = query
        super().__init__(sources=[source], targets=[target], **kwargs)
        self.kwargs = kwargs.copy()
        self.kwargs.update(
            ufn_as_dict=ufn_as_dict, query=query, incremental=incremental)
        if not hasattr(self, "ufn"):
            self.ufn = None
        if self.ufn_as_dict:
            modname, fname = ufn_as_dict["@module"], ufn_as_dict["@name"]
            mod = __import__(modname, globals(), locals(), [fname], 0)
            self.ufn = getattr(mod, fname)

    def get_items(self):
        source, target = self.source, self.target

        index_checks = [confirm_field_index(target, target.key)]
        if self.incremental:
            # Ensure [(lu_field, -1), (key, 1)] index on both source and target
            for store in (source, target):
                info = store.collection.index_information().values()
                index_checks.append(
                    any(spec == [(store.lu_field, -1), (store.key, 1)]
                        for spec in (index['key'] for index in info)))
        if not all(index_checks):
            index_warning = (
                "Missing one or more important indices on stores. "
                "Performance for large stores may be severely degraded. "
                "Ensure indices on target.key and "
                "[(store.lu_field, -1), (store.key, 1)] "
                "for each of source and target."
            )
            self.logger.warning(index_warning)

        criteria = {}
        if self.query:
            criteria.update(self.query)
        if self.incremental:
            self.logger.info(
                "incremental mode: finding new/updated source keys")
            keys_updated = source_keys_updated(source, target)
            # Merge existing criteria and {source.key: {"$in": keys_updated}}.
            if "$and" in criteria:
                criteria["$and"].append({source.key: {"$in": keys_updated}})
            elif source.key in criteria:
                # XXX could go deeper and check for $in, but this is fine.
                criteria["$and"] = [
                    {source.key: criteria[source.key].copy()},
                    {source.key: {"$in": keys_updated}},
                ]
                del criteria[source.key]
            else:
                criteria.update({source.key: {"$in": keys_updated}})
        # Check ratio of criteria size to 16 MB MongoDB document size limit.
        # Overestimates ratio via 1000 * 1000 instead of 1024 * 1024.
        # If criteria is > 16MB, even cursor.count() will fail with a
        # "DocumentTooLarge: "command document too large" error.
        if (total_size(criteria) / (16 * 1000 * 1000)) >= 1:
            raise RuntimeError(
                "`get_items` query criteria too large. This can happen if "
                "trying to run incremental=True for the initial build of a "
                "very large source store, or if `query` is too large. You "
                "can use maggma.utils.total_size to ensure `query` is smaller "
                "than 16,000,000 bytes.")
        return source.query(criteria=criteria)

    def process_item(self, item):
        try:
            processed = self.ufn.__call__(item)
        except Exception as e:
            processed = {"error": str(e)}
        key, lu_field = self.source.key, self.source.lu_field
        out = {key: item[key], lu_field: item[lu_field]}
        out.update(processed)
        return out

    def update_targets(self, items):
        source, target = self.source, self.target
        for item in items:
            # Use source last-updated value, ensuring `datetime` type.
            item[target.lu_field] = source.lu_func[0](item[source.lu_field])
            if source.lu_field != target.lu_field:
                del item[source.lu_field]
            item["_bt"] = datetime.utcnow()
            if "_id" in item:
                del item["_id"]
        target.update(items, update_lu=False)


class CopyBuilder(MapBuilder):
    """Sync a source store with a target store."""
    def __init__(self, source, target, **kwargs):
        self.ufn = identity
        super().__init__(source=source, target=target, **kwargs)
