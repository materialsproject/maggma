# 5-minute `maggma` quickstart

## Install

Open your terminal and run the following command.

``` shell
pip install --upgrade maggma
```

## Format your data

Structure your data as a `list` of `dict` objects,
where each `dict` represents a single record (called a 'document'). Below,
we've created some data to represent info about the Teenage Mutant Ninja Turtles.

```python
>>> turtles = [{"name": "Leonardo", "color": "blue", "tool": "sword"},
               {"name": "Donatello","color": "purple", "tool": "staff"},
               {"name": "Michelangelo", "color": "orange", "tool": "nunchuks"},
               {"name":"Raphael", "color": "red", "tool": "sai"}
            ]
```

Structuring your data in this manner
enables highly flexible storage options -- you can easily write it to a `.json`
file, place it in a `Store`, insert it into a Mongo database, etc. `maggma` is
designed to facilitate this.

In addition to being structured as a `list` of `dict`, **every document (`dict`)
must have a key that uniquely identifies it.** By default, this key is the `task_id`, but it can be set to any value you
like using the `key` argument when you instantiate a `Store`. In the example above,
`name` can serve as a key because all documents have it, and the values are all unique.


See [Using Stores](getting_started/stores.md/#structuring-store-data) for more details on structuring data.

## Create a `Store`

`maggma` contains `Store` classes that connect to MongoDB, Azure, S3 buckets,
`.json` files, system memory, and many more data sources. Regardless of the
underlying storage platform, all `Store` classes implement the same interface
for connecting and querying.

The simplest store to use is the `MemoryStore`. It simply loads your data into
memory and makes it accessible via `Store` methods like `query`, `distinct`, etc. Note that for this particular store, your data is not saved anywhere -
once you close it, the data are lost from RAM! Note that in this example,
we've set `key='name'` when creating the `Store` because we want to use `name` as our unique identifier.

```python
>>> from maggma.stores import MemoryStore
>>> store = MemoryStore(key="name")
```

See [Using Stores](getting_started/stores.md/#list-of-stores) for more details on available `Store` classes.

## Connect to the `Store`

Before you can interact with a store, you have to `connect()`. This is as simple
as

```python
store.connect()
```

When you are finished, you can close the connection with `store.close()`.

A cleaner (and recommended) way to make sure connections are appropriately closed
is to access `Store` through a context manager (a `with` statement), like this:

```python
with store as s:
    s.query()
```

## Add your data to the `Store`

To add data to the store, use `update()`.

```python
with store as s:
    s.update(turtles)
```

## Query the `Store`

Now that you have added your data to a `Store`, you can leverage `maggma`'s
powerful API to query and analyze it. Here are some examples:

See how many documents the `Store` contains
```python
>>> store.count()
4
```

Query a single document to see its structure
```python
>>> store.query_one({})
{'_id': ObjectId('66746d29a78e8431daa3463a'), 'name': 'Leonardo', 'color': 'blue', 'tool': 'sword'}
```

List all the unique values of the `color` field
```python
>>> store.distinct('color')
['purple', 'orange', 'blue', 'red']
```

See [Understanding Queries](getting_started/query_101.md) for more example queries and [the `Store` interface](getting_started/stores.md/#the-store-interface) for more details about available `Store`
methods.
