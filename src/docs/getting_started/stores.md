# Using `Store`

A `Store` is just a wrapper to access data from somewhere. That somewhere is typically a MongoDB collection, but it could also be GridFS which lets you keep large binary objects. `maggma` makes GridFS and MongoDB collections feel the same. Beyond that it adds in something that looks like GridFS but is actually using AWS S3 as the storage space. Finally, `Store` can actually perform logic, concatenating two or more `Stores` together to make them look like one data source for instance. This means you only have to write a `Builder` for one scenario and the choice of `Store` lets you control how and where the data goes.

## The `Store` interface

### Initializing a Store

All `Store`s have a few basic arguments that are critical to understand. Any `Store` has two special fields: `key` and `last_updated_field`. The `key` defines how the `Store` tells documents part. Typically this is `_id` in MongoDB, but you could use your own field. `last_updated_field` tells `Store` how to order the documents by a date field.  `Store`s can also take a `Validator` object to make sure the data going into obeys some schema.

### Using a Store

Stores provide a number of basic methods that make easy to use:

- query: standard mongo style `find` method that lets you search the store
- query_one: same as above but limits to the first document
- update: update the documents into the collection, this will override documents if the key field matches. You can temporarily provide extra fields to key these documents if you don't to maintain duplicates, for instance keying on both `key` and `last_udpated_field`
- ensure_index: this creates an index the underlying data-source for fast querying
- distinct: gets distinct values of a field
- groupby: similar to query but performs a grouping operation and returns sets of documents
- remove_docs: removes documents from the underlying data source
- last_updated: finds the most recently updated `last_updated_field` value and returns that. Usefull for knowing how old a data-source is
- newer_in: finds all documents that are newer in the target collection and returns their `key`s. This is a very usefull way of performing incremental processing
