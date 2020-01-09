# Writing a Builder

## Builder Architecture

A `Builder` is a class that inherits from `maggma.core.Builder` and implement 3 methods:

* `get_items`:  This method should return some iterable of items to run through `process_items`
* `process_item`: This method should take a single item, process it, and return the processed item
* `update_targets`: This method should take a list of processed items and update the target stores.

To make this less abstract, we will write a builder that multiplies the "a" sub-document by a pre-configured `multiplier`. Let's assume we have some source collection in MongoDB with documents that look like this:

``` json
{
    "id": 1,
    "a": 3,
    "last_updated": "2019-11-3"
}
```

## Class definition and `__init__`

A simple class definition for a Maggma-based builder looks like this:

``` python
from maggma.core import Builder
from maggma.core import Store

class MultiplyBuilder(Builder):
    """
    Simple builder that multiplies the "a" sub-document by pre-set value
    """
```

The `__init__` for a builder can have any set of parameters. Generally, you want a source `Store` and a target `Store` along with any parameters that configure the builder. Due to the `MSONable` pattern, any parameters to `__init__` have to be stored as attributes. A simple `__init__` would look like this:

``` python
    def __init__(self, source: Store, target: Store, multiplier: int = 2, **kwargs):
        """
        Arguments:
            source: the source store
            target: the target store
            multiplier: the multiplier to apply to "a" sub-document
        """
        self.source = source
        self.target = target
        self.multiplier = multiplier
        self.kwargs = kwargs

        super().__init__(sources=source,targets=target,**kwargs)
```

Python type annotations provide a really nice way of documenting the types we expect and being able to later type check using `mypy`. We defined the type for `source` and `target` as `Store` since we only care that implements that pattern. How exactly these `Store`s operate doesn't concern us here.

Note that the `__init__` arguments: `source`, `target`, `multiplier`, and `kwargs` get saved as attributess:

``` python
        self.source = source
        self.target = target
        self.multiplier = multiplier
        self.kwargs = kwargs
```

Finally, we want to call the base `Builder`'s `__init__` to tell it our sources and targets for this builder. In addition, we pass along any extra parameters that might configured the base builder class.

``` python
super().__init__(sources=source,targets=target,**kwargs)
```

Calling the parent class `__init__` is a good practice as sub-classing builders is a good way to encapsulate complex logic.

## `get_items`

`get_items` is conceptually a simple method to implement, but in practice can easily be more code than the rest of the builder. All of the logic for getting data from the sources has to happen here, which requires some planning. `get_items` should also sort all of the data into induvidual **items** to process. This simple builder has a very easy `get_items`:

``` python

    def get_items(self) -> Iterator:
        """
        Gets induvidual documents to multiply
        """

        return self.source.query()
```

Here, get items just returns the results of `query()` from the store. It could also have been written as a generator:

``` python

    def get_items(self) -> Iterable:
        """
        Gets induvidual documents to multiply
        """
        for doc in self.source.query():
            yield doc
```

We could have also returned a list of items:

``` python

    def get_items(self) -> Iterable:
        """
        Gets induvidual documents to multiply
        """
        docs = list(self.source.query())
```

## `process_item`

`process_item` just has to do the parallelizable work on each item. Since the item is whatever comes out of `get_items`, you know exactly what it should be. It may be a single document, a list of documents, a mapping, a set, etc.

Our simple process item just has to multiply one field by `self.mulitplier`:

``` python

    def process_items(self, item : Dict) -> Dict:
        """
        Multiplies the "a" sub-document by self.multiplier
        """
        new_item = dict(**item)
        new_item["a"] *= self.multiplier
        return new_item
```

## `update_targets`

Finally, we have to put the processed item in to the target store:

``` python

    def update_targets(self,items: List[Dict]):
        """
        Adds the processed items into the target store
        """
        self.target.update(items)
```

Putting it all together we get:

``` python
from typing import Dict, Iterable, List
from maggma.core import Builder
from maggma.core import Store

class MultiplyBuilder(Builder):
    """
    Simple builder that multiplies the "a" sub-document by pre-set value
    """


    def __init__(self, source: Store, target: Store, multiplier: int = 2, **kwargs):
        """
        Arguments:
            source: the source store
            target: the target store
            multiplier: the multiplier to apply to "a" sub-document
        """
        self.source = source
        self.target = target
        self.multiplier = multiplier
        self.kwargs = kwargs

        super().__init__(sources=source,targets=target,**kwargs)

    def get_items(self) -> Iterable:
        """
        Gets induvidual documents to multiply
        """
        docs = list(self.source.query())

    def process_items(self, item : Dict) -> Dict:
        """
        Multiplies the "a" sub-document by self.multiplier
        """
        new_item = dict(**item)
        new_item["a"] *= self.multiplier
        return new_item


    def update_targets(self,items: List[Dict]):
        """
        Adds the processed items into the target store
        """
        self.target.update(items)
```
