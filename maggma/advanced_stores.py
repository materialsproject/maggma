from maggma.stores import Store
from pydash.objects import set_, get, has
from pydash.utilities import to_path
import pydash.objects


class AliasingStore(Store):
    """
    Special Store that aliases for the primary accessors
    """

    def __init__(self, store, aliases, **kwargs):
        """
        store (Store): the store to wrap around
        aliases (dict): dict of aliases of the form external key: internal key
        """
        self.store = store
        self.aliases = aliases
        self.reverse_aliases = {v: k for k, v in aliases.items()}
        self.kwargs = kwargs

        kwargs.update({"lu_field": store.lu_field, "lu_key": store.lu_key})
        super(AliasingStore, self).__init__(**kwargs)

    def query(self, properties=None, criteria=None, **kwargs):

        if isinstance(properties, list):
            properties = {p: 1 for p in properties}

        criteria = criteria if criteria else {}
        substitute(properties, self.reverse_aliases)
        lazy_substitute(criteria, self.reverse_aliases)
        for d in self.store.query(properties, criteria, **kwargs):
            substitute(d, self.aliases)
            yield d

    def distinct(self, key, criteria=None, **kwargs):
        if key in self.aliases:
            key = self.aliases[key]
        criteria = criteria if criteria else {}
        lazy_substitute(criteria, self.aliases)
        return self.store.distinct(key, criteria, **kwargs)

    def update(self, docs, update_lu=True, key=None):
        key = key if key else self.key

        for d in docs:
            substitute(d, self.reverse_aliases)

        if key in self.aliases:
            key = self.aliases[key]

        self.store.update(docs, update_lu=update_lu, key=key)

    def ensure_index(self, key, unique=False):
        if key in self.aliases:
            key = self.aliases
        return self.store.ensure_index(key, unique)

    def close(self):
        self.store.close()

    @property
    def collection(self):
        return self.store.collection

    def connect(self):
        self.store.connect()


def lazy_substitute(d, aliases):
    for alias, key in aliases.items():
        if key in d:
            d[alias] = d[key]
            del d[key]


def substitute(d, aliases):
    for alias, key in aliases.items():
        if has(d, key):
            set_(d, alias, get(d, key))
            unset(d, key)


def unset(d, key):
    pydash.objects.unset(d, key)
    path = to_path(key)
    for i in reversed(range(1, len(path))):
        if len(get(d, path[:i])) == 0:
            unset(d, path[:i])
