# Using `Store`

A `Store` is just a wrapper to access data from a data source. That data source is typically a MongoDB collection, but it could also be an Amazon S3 bucket, a GridFS collection, or folder of files on disk. `maggma` makes interacting with all of these data sources feel the same (see the [`Store` interface](#the-store-interface), below). `Store` can also perform logic, concatenating two or more `Store` together to make them look like one data source for instance.

The benefit of the `Store` interface is that you only have to write a `Builder` once. As your data moves or evolves, you simply point it to different `Store` without having to change your processing code.

## Structuring `Store` data

Because `Store` is built around a MongoDB-like query syntax, data that goes into `Store` needs to be structured similarly to MongoDB data. In python terms,
that means **the data in a `Store` must be structured as a `list` of `dict`**,
where each `dict` represents a single record (called a 'document').

```python
data = [{"AM": "sunrise"}, {"PM": "sunset"} ... ]
```

Note that this structure is very similar to the widely-used [JSON](https://en.wikipedia.org/wiki/JSON) format. So structuring your data in this manner
enables highly flexible storage options -- you can easily write it to a `.json`
file, place it in a `Store`, insert it into a Mongo database, etc. `maggma` is
designed to facilitate this.

In addition to being structured as a `list` of `dict`, **every document (`dict`)
must have a key that uniquely identifies it.** By default, this key is the `task_id`, but it can be set to any value you
like using the `key` argument when you instantiate a `Store`.

```python
data = [{"task_id": 1, "AM": "sunrise"}, {"task_id: 2, "PM": "sunset"} ... ]
```

Just to emphasize - **every document must have a `task_id`, and the value of `task_id` must be unique for every document**. The rest of the document structure
is up to you, but `maggma` works best when every document follows a pre-defined
schema (i.e., all `dict` have the same set of keys / same structure).

## The `Store` interface

All `Store` provide a number of basic methods that facilitate querying, updating, and removing data:

- `query`: Standard mongo style `find` method that lets you search the store. See [Understanding Queries](query_101.md) for more details about the query syntax.
- `query_one`: Same as above but limits returned results to just the first document that matches your query. Very useful for understanding the structure of the returned data.
- `count`: Counts documents in the `Store`
- `distinct`: Returns a list of distinct values of a field.
- `groupby`: Similar to query but performs a grouping operation and returns sets of documents.
- `update`: Update (insert) documents into the `Store`. This will overwrite documents if the key field matches.
- `remove_docs`: Removes documents from the underlying data source.
- `newer_in`: Finds all documents that are newer in the target collection and returns their `key`s. This is a very useful way of performing incremental processing.
- `ensure_index`: Creates an index for the underlying data-source for fast querying.
- `last_updated`: Finds the most recently updated `last_updated_field` value and returns that. Useful for knowing how old a data-source is.

!!! Note
    If you are familiar with `pymongo`, you may find the comparison table below
    helpful. This table illustrates how `maggma` method and argument names map
    onto `pymongo` concepts.


    | `maggma`    | `pymongo` equivalent |
    | -------- | ------- |
    | **methods** |
    | `query_one`  | `find_one`    |
    | `query` | `find`     |
    | `count`    | `count_documents`    |
    | `distinct`    | `distinct`    |
    | `groupby`    | `group`    |
    | `update`    | `insert`    |
    | **arguments** |
    | `criteria={}` | `filter={}` |
    | `properties=[]` | `projection=[]` |


## Creating a Store

All `Store`s have a few basic arguments that are critical for basic usage. Every `Store` has two attributes that the user should customize based on the data contained in that store: `key` and `last_updated_field`.

The `key` defines how the `Store` tells documents apart. Typically this is `_id` in MongoDB, but you could use your own field (be sure all values under the key field can be used to uniquely identify documents).

`last_updated_field` tells `Store` how to order the documents by a date, which is typically in the `datetime` format, but can also be an ISO 8601-format (ex: `2009-05-28T16:15:00`) `Store`s can also take a `Validator` object to make sure the data going into it obeys some schema.

In the example below, we create a `MongoStore`, which connects to a MongoDB database.
To create this store, we have to provide `maggma` the connection details to the
database like the hostname, collection name, and authentication info. Note that
we've set `key='name'` because we want to use that `name` as our unique identifier.

```python
>>> store = MongoStore(database="my_db_name",
                       collection_name="my_collection_name",
                       username="my_username",
                       password="my_password",
                       host="my_hostname",
                       port=27017,
                       key="name",
                    )
```

The specific arguments required to create a `Store` depend on the underlying
format. For example, the `MemoryStore`, which just loads data into memory,
requires no arguments to instantiate. Refer to the [list of Stores](#list-of-stores)
below (and their associated documentation) for specific details.

## Connecting to a `Store`

You must connect to a store by running `store.connect()` before querying or updating the store.
If you are operating on the stores inside of another code it is recommended to use the built-in context manager, e.g.:

```python
with MongoStore(...) as store:
    store.query()
```

This will take care of the `connect()` automatically while ensuring that the
connection is closed properly after the store tasks are complete.

## List of Stores

Current working and tested `Store` include the following. Click the name of
each store for more detailed documentation.

- [`MongoStore`](/maggma/reference/stores/#maggma.stores.mongolike.MongoStore): interfaces to a MongoDB Collection using port and hostname.
- [`MongoURIStore`](/maggma/reference/stores/#maggma.stores.mongolike.MongoURIStore): interfaces to a MongoDB Collection using a "mongodb+srv://" URI.
- [`MemoryStore`](/maggma/reference/stores/#maggma.stores.mongolike.MemoryStore): just a Store that exists temporarily in memory
- [`JSONStore`](/maggma/reference/stores/#maggma.stores.mongolike.JSONStore): builds a MemoryStore and then populates it with the contents of the given JSON files
- [`FileStore`](/maggma/reference/stores/#maggma.stores.file_store.FileStore): query and add metadata to files stored on disk as if they were in a database
- [`GridFSStore`](/maggma/reference/stores/#maggma.stores.gridfs.GridFSStore): interfaces to GridFS collection in MongoDB using port and hostname.
- [`GridFSURIStore`](/maggma/reference/stores/#maggma.stores.gridfs.GridFSURIStore): interfaces to GridFS collection in MongoDB using a "mongodb+srv://" URI.
- [`S3Store`](/maggma/reference/stores/#maggma.stores.aws.S3Store): provides an interface to an S3 Bucket either on AWS or self-hosted solutions ([additional documentation](advanced_stores.md))
- [`ConcatStore`](/maggma/reference/stores/#maggma.stores.compound_stores.ConcatStore): concatenates several Stores together so they look like one Store
- [`VaultStore`](/maggma/reference/stores/#maggma.stores.advanced_stores.VaultStore): uses Vault to get credentials for a MongoDB database
- [`AliasingStore`](/maggma/reference/stores/#maggma.stores.advanced_stores.AliasingStore): aliases keys from the underlying store to new names
- `SandboxStore: provides permission control to documents via a `_sbxn` sandbox key
- [`JointStore`](/maggma/reference/stores/#maggma.stores.compound_stores.JointStore): joins several MongoDB collections together, merging documents with the same `key`, so they look like one collection
- [`AzureBlobStore`](/maggma/reference/stores/#maggma.stores.azure.AzureBlobStore): provides an interface to Azure Blobs for the storage of large amount of data
- [`MontyStore`](/maggma/reference/stores/#maggma.stores.mongolike.MontyStore): provides an interface to [montydb](https://github.com/davidlatwe/montydb) for in-memory or filesystem-based storage
- [`MongograntStore`](/maggma/reference/stores/#maggma.stores.advanced_stores.MongograntStore): (DEPRECATED) uses Mongogrant to get credentials for MongoDB database
