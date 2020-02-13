# Map Builder

`maggma` has a built in builder called the `MapBuilder` which handles a number of tedious tasks in writing a builder. This class is designed to be used similar to a map operator in any other framework in even the map function in python. `MapBuilder` will take each document in the source store, apply the function you give it, and then store that in the target store. It handles incremental building, keeping track of errors, getting only the data you need, managing timeouts, and deleting orphaned documents through configurational options.

Let's create the same `MultiplierBuilder` we wrote earlier using `MapBuilder`:

``` python
from maggma.builders import MapBuilder
from maggma.core import Store

class MultiplyBuilder(MapBuilder):
    """
    Simple builder that multiplies the "a" sub-document by pre-set value
    """
```

Just like before we define a new class, but this time it should inherit from `MapBuilder`.

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

        kwargs = {k,v in kwargs.items() if k not in ["projection","delete_orphans","timeout","store_process_time","retry_failed"]}

        super().__init__(source=source,
                         target=target,
                         projection=["a"],
                         delete_orphans=False,
                         timeout=10,
                         store_process_time=True,
                         retry_failed=True,
                         **kwargs)
```

MapBuilder has a number of configurational options that you can hardcode as above or expose as properties for the user through **kwargs:

- projection: list of the fields you want to project. This can reduce the data transfer load if you only need certain fields or sub-documents from the source documents
- delete_orphans: this will delete documents in the target which don't have a corresponding document in the source
- timeout: optional timeout on the process function
- store_process_timeout: adds the process time into the target document for profiling
- retry_failed: retries running the process function on previously failed documents

Finally let's get to the hard part which is running our function. We do this by defining `unary_function`

``` python

    def unary_function(self,item):
        return {"a": item["a"] * self.multiplier}

```

Note that we're not returning all the extra information typically kept in the originally item. Normally, we would have to write code that copies over the source `key` and convert it to the target `key`. Same goes for the `last_updated_field`. `MapBuilder` takes care of this, while also recording errors, processing time, and the Builder version.
