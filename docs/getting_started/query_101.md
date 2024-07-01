# Understanding Queries

Putting your data into a `maggma` `Store` gives you powerful search, summary,
and analytical capabilities. All are based on "queries", which specify how
you want to search your data, and which parts of it you want to get in return.

`maggma` query syntax closely follows [MongoDB Query syntax](https://www.mongodb.com/docs/manual/tutorial/query-documents/). In this tutorial, we'll cover the syntax of the most common query operations. You can refer to the
[MongoDB](https://www.mongodb.com/docs/manual/tutorial/query-documents/) or  [pymongo](https://pymongo.readthedocs.io/en/stable/tutorial.html) (python interface to MongoDB) documentation for examples of more advanced use cases.

Let's create an example dataset describing the [Teenage Mutant Ninja Turtles](https://en.wikipedia.org/wiki/Teenage_Mutant_Ninja_Turtles).

```python
>>> turtles = [{"name": "Leonardo",
                "color": "blue",
                "tool": "sword",
                "occupation": "ninja"
                },
               {"name": "Donatello",
                "color": "purple",
                "tool": "staff",
                "occupation": "ninja"
                },
               {"name": "Michelangelo",
                "color": "orange",
                "tool": "nunchuks",
                "occupation": "ninja"
                },
               {"name":"Raphael",
               "color": "red",
               "tool": "sai",
               "occupation": "ninja"
                },
               {"name":"Splinter",
               "occupation": "sensei"
                }
            ]
```

Notice how this data follows the principles described in [Structuring `Store` data](stores.md/#structuring-store-data):
- every document (`dict`) has a `name` key with a unique value
- every document has a common set of keys (`name`,
`occupation`).
- Note that SOME documents also share the keys `tool` and `color`, but not all. This is OK.

For the rest of this tutorial, we will assume that this data has already been
added to a `Store` called `tmnt_store`, which we are going to query.

## The `query` method

`Store.query()` is the primary method you will use to search your data.

- `query`
always returns a generator yielding any and all documents that match the query
you provide.
- There are no mandatory arguments. If you run `query()` you will get a generator
containing all documents in the `Store`
- The first (optional) argument is `criteria`, which is a query formatted as a `dict` as described in the next section.
- You can also specify `properties`, which is a list of fields from the documents you want to return. This is useful when working with large documents because then you only have to download the data you need rather than the entire document.
- You can also `skip` every N documents, `limit` the number of documents returned, and `sort` the result by some field.

Since `query` returns a generator, you will typically want to turn the results into a list, or use them in a `for` loop.

Turn into a list
```python
results = [d for d in store.query()]
```

Use in a `for` loop
```python
for doc in store.query():
    print(doc)
```

## The structure of a query

A query is also a `dict`. Each key in the dict corresponds to a fjeld in the
documents you want to query (such as `name`, `color`, etc.), and the value
is the value of that key that you want to match. For example, a query to
select all documents where `occupation` is `ninja`, would look like

```python
{"occupation": "ninja"}
```

This query will be passed as an argument to `Store` methods like `query_one`,
`query`, and `count`, as demonstrated next.


## Example queries

### Match a single value

To select all records where a field matches a single value, set the key to
the field you want to match and its value to the value you are looking for.

Return all records where 'occupation' is 'ninja'
```python
>>> with tmnt_store as store:
...     results = list(store.query({"occupation": "ninja"}))
>>> len(results)
4
```

Return all records where 'name' is 'Splinter'

```python
>>> with tmnt_store as store:
...     results = list(store.query({"name": "Splinter"}))
>>> len(results)
1
```

### Match any value in a list: `$in`

To find all documents where a field matches one of several different
values, use `$in` with a list of the value you want to search.

```python
>>> with tmnt_store as store:
...     results = list(store.query({"color": {"$in": ["red", "blue"]}}))
>>> len(results)
2
```

`$in` is an example of a "query operator". Others include:

- `$nin`: a value is NOT in a list (the inverse of the above example)
- `$gt`, `$gte`: greater than, greater than or equal to a value
- `$lt`, `$lte`: greater than, greater than or equal to a value
- `$ne`: not equal to a value
- `$not`: inverts the effect of a query expression, returning results that
    do NOT match.

See the [MongoDB docs](https://www.mongodb.com/docs/manual/reference/operator/query/#query-selectors) for a complete list.

!!! Note

    When using query operators like `$in`, you must include a nested `dict` in
    your query, where the operator is the key and the search parameters are
    the value, e.g., the dictionary `{"$in": ["red", "blue"]}` is the **value**
    associated with the search field (`color`) in the parent dictionary.

### Nested fields

Suppose that our documents had a nested structure, for example, by having
separate fields for first and last name:

```python
>>> turtles = [{"name":
                    {"first": "Leonardo",
                     "last": "turtle"
                     },
                "color": "blue",
                "tool": "sword",
                "occupation": "ninja"
                },
                ...
                ]
```

You can query nested fields by placing a period `.` between each level in the
hierarchy. For example:

```python
>>> with tmnt_store as store:
...     results = list(store.query({"name.first": "Splinter"}))
>>> len(results)
1
```

### Numerical Values

You can query numerical values in analogous fashion to the examples given above.

!!! Note
    When querying on numerical values, be mindful of the `type` of the data.
    Data stored in `json` format is often converted entirely to `str`, so if
    you use a numerical query operator like `$gte`, you might not get the
    results you expect unless you first verify that the numerical data
    in the `Store` is a `float` or `int` .
