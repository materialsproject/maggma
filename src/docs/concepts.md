
## MSONable

Maggma objects implement the `MSONable` pattern which enables these objects to serialize and deserialize to python dictionaries or even JSON. The MSONable encoder injects in `@module` and `@class` info so that the object can be deserialized without the manual. This enables much of Maggma to operate like a plugin system.

## Store

Stores are document-based data sources and data sinks. They are modeled around the MongoDB collection although they can represent more complex data sources as well. Stores implement methods to `connect`, `query`, find `distinct` values, `groupby` fields, `update` documents, and `remove` documents. Stores also implement a number of critical fields for Maggma: the `key` and the `last_updated_field`. `key` is the field that is used to index the underlying data source. `last_updated_field` is the timestamp of when that document.

## Builder

Builders represent a data transformation step. Builders break down each transformation into 3 key steps: `get_items`, `process_item`, and `update_targets`. Both `get_items` and `update_targets` can perform IO to the data stores. `process_item` is expected to not perform any IO so that it can be parallelized by Maggma. Builders can be chained together into a array and then saved as a JSON file to be run on a production system.
