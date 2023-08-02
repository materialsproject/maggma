# Using `Store`

A `Store` is just a wrapper to access data from a data source. That data source is typically a MongoDB collection, but it could also be an Amazon S3 bucket, a GridFS collection, or folder of files on disk. `maggma` makes interacting with all of these data sources feel the same (see the `Store` interface, below). `Store` can also perform logic, concatenating two or more `Store` together to make them look like one data source for instance.

The benefit of the `Store` interface is that you only have to write a `Builder` once. As your data moves or evolves, you simply point it to different `Store` without having to change your processing code.

## List of Stores

Current working and tested `Store` include:

- `MongoStore`: interfaces to a MongoDB Collection
- `MemoryStore`: just a Store that exists temporarily in memory
- `JSONStore`: builds a MemoryStore and then populates it with the contents of the given JSON files
- `FileStore`: query and add metadata to files stored on disk as if they were in a database
- `GridFSStore`: interfaces to GridFS collection in MongoDB
- `S3Store`: provides an interface to an S3 Bucket either on AWS or self-hosted solutions ([additional documentation](advanced_stores.md))
- `ConcatStore`: concatenates several Stores together so they look like one Store
- `MongoURIStore`: MongoDB Introduced advanced URIs including their special "mongodb+srv://" which uses a combination of SRV and TXT DNS records to fully setup the client. This store is to safely handle these kinds of URIs.
- `MongograntStore`: uses Mongogrant to get credentials for MongoDB database
- `VaultStore`: uses Vault to get credentials for a MongoDB database
- `AliasingStore`: aliases keys from the underlying store to new names
- `SandboxStore: provides permission control to documents via a `_sbxn` sandbox key
- `JointStore`: joins several MongoDB collections together, merging documents with the same `key`, so they look like one collection
- `AzureBlobStore`: provides an interface to Azure Blobs for the storage of large amount of data

## The `Store` interface

All `Store` provide a number of basic methods that facilitate querying, updating, and removing data:

- `query`: Standard mongo style `find` method that lets you search the store.
- `query_one`: Same as above but limits returned results to just the first document that matches your query. Very useful for understanding the structure of the returned data.
- `update`: Update the documents into the collection. This will override documents if the key field matches.
- `ensure_index`: This creates an index for the underlying data-source for fast querying.
- `distinct`: Gets distinct values of a field.
- `groupby`: Similar to query but performs a grouping operation and returns sets of documents.
- `remove_docs`: Removes documents from the underlying data source.
- `last_updated`: Finds the most recently updated `last_updated_field` value and returns that. Useful for knowing how old a data-source is.
- `newer_in`: Finds all documents that are newer in the target collection and returns their `key`s. This is a very useful way of performing incremental processing.

### Initializing a Store

All `Store`s have a few basic arguments that are critical for basic usage. Every `Store` has two attributes that the user should customize based on the data contained in that store: `key` and `last_updated_field`. The `key` defines how the `Store` tells documents apart. Typically this is `_id` in MongoDB, but you could use your own field (be sure all values under the key field can be used to uniquely identify documents). `last_updated_field` tells `Store` how to order the documents by a date, which is typically in the `datetime` format, but can also be an ISO 8601-format (ex: `2009-05-28T16:15:00`) `Store`s can also take a `Validator` object to make sure the data going into it obeys some schema.

### Using a Store

You must connect to a store by running `store.connect()` before querying or updating the store.
If you are operating on the stores inside of another code it is recommended to use the built-in context manager,
which will take care of the `connect()` automatically, e.g.:

```python
with MongoStore(...) as store:
    store.query()
```
