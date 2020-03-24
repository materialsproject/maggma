# Advanced Builder Concepts

There are a number of features in `maggma` designed to assist with advanced features:

## Logging

`maggma` builders have a python `logger` object that is already setup to output to the correct level. You can directly use it to output `info`, `debug`, and `error` messages.

``` python
    def get_items(self) -> Iterable:
        ...
        self.logger.info(f"Got {len(to_process_ids)} to process")
        ...
```

## Querying for Updated Documents

One of the most important features in a builder is incremental building which allows the builder to just process new documents. One of the parameters for a maggma store is the `last_updated_field` and the `last_updated_type` which tell `maggma` how to deal with dates in the source and target documents. This allows us to get the `id` of any documents that are newer in the target than the newest document in the source:

``` python

        new_ids = self.target.newer_in(self.source)
```

## Speeding up Data Transfers

Since `maggma` is designed around Mongo style data sources and sinks, building indexes or in-memory copies of fields you want to search on is critical to get the fastest possible data input/output (IO). Since this is very builder and document style dependent, `maggma` provides a direct interface to `ensure_indexes` on a Store. A common paradigm is to do this in the beginning of `get_items`:

``` python
    def ensure_indexes(self):
        self.source.ensure_index("some_search_fields")
        self.target.ensure_index(self.target.key)

    def get_items(self) -> Iterable:
        self.ensure_indexes()
        ...
```


## Built in Templates for Advanced Builders

`maggma` implements templates for builders that have many of these advanced features listed above:

- [MapBuilder](map_builder.md) Creates one-to-one document mapping of items in the source Store to the transformed documents in the target Store.
- [GroupBuilder](group_builder.md) Creates many-to-one document mapping of items in the source Store to transformed documents in the traget Store
