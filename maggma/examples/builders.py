from datetime import datetime

from maggma.builder import Builder
from maggma.utils import confirm_field_index


def source_keys_updated(source, target):
    """
    Get a list of keys for source documents that have been updated.

    Requires [(lu_field, -1),(key, 1)] compound index on both source and target.
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
    def __init__(self, source, target, process_item=None,
                 query=None, incremental=True, **kwargs):
        """
        Apply a `process_item` function to each source document.

        Args:
            source (Store): source store
            target (Store): target store
            process_item (dict): {@module,@name} of unary function for import
            query (dict): optional query to filter source store
            incremental (bool): whether to use lu_field of source and target
                to get only new/updated documents.
        """
        self.source = source
        self.target = target
        self.process_item = process_item
        self.incremental = incremental
        self.query = query
        super().__init__(sources=[source], targets=[target], **kwargs)

        modname, fname = process_item["@module"], process_item["@name"]
        mod = __import__(modname, globals(), locals(), [fname], 0)
        self.ufn = getattr(mod, fname)

        checks = [confirm_field_index(target, target.key)]
        if self.incremental:
            # Ensure [(lu_field, -1), (key, 1)] index on both source and target
            for store in (source, target):
                info = store.collection.index_information().values()
                checks.append(
                    any(spec == [('last_updated', -1), ('task_id', 1)]
                        for spec in (index['key'] for index in info)))
        if not all(checks):
            raise Exception("Missing required index on store(s).")

    def get_items(self):
        source, target = self.source, self.target
        criteria = {}
        if self.query:
            criteria.update(self.query)
        if self.incremental:
            keys_updated = source_keys_updated(source, target)
            criteria.update({source.key: {"$in": keys_updated}})
        return source.query(criteria=criteria)

    def process_item(self, item):
        return self.ufn.__call__(item)

    def update_targets(self, items):
        source, target = self.source, self.target
        for item in items:
            # Use source last-updated value, ensuring `datetime` type.
            item[target.lu_field] = source.lu_func[0](item[source.lu_field])
            if source.lu_field != target.lu_field:
                del item[source.lu_field]
            item["_bt"] = datetime.utcnow()
            del item["_id"]
        target.update(items, update_lu=False)


class CopyBuilder(MapBuilder):
    def __init__(self, source, target, query=None, incremental=True, **kwargs):
        """Sync a source with a target."""
        self.source = source
        self.target = target
        self.incremental = incremental
        self.query = query if query else {}
        process_item = {"@module": "pydash", "@name": "identity"}
        super().__init__(
            source=source, target=target, process_item=process_item,
            query=query, incremental=incremental, **kwargs)
