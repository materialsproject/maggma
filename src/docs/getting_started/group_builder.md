# Group Builder

Another advanced template in `maggma` is the `GroupBuilder`, which groups documents together before applying your function on the group of items. Just like `MapBuilder`, `GroupBuilder` also handles incremental building, keeping track of errors, getting only the data you need, and managing timeouts. GroupBuilder won't delete orphaned documents since that reverse relationshop isn't valid.

Let's create a simple `ResupplyBuilder`, which will look at the inventory of items and determine what items need resupply. The source document will look something like this:

``` JSON
{
    "name": "Banana",
    "type": "fruit",
    "quantity": 20,
    "minimum": 10,
    "last_updated": "2019-11-3T19:09:45"
}
```

Our builder should give us documents that look like this:

``` JSON
{
    "names": ["Grapes", "Apples", "Bananas"],
    "type": "fruit",
    "resupply": {
        "Apples": 10,
        "Bananes": 0,
        "Grapes": 5
    },
    "last_updated": "2019-11-3T19:09:45"
}
```

To begin, we define our `GroupBuilder`:

``` python

from maggma.builders import GroupBuilder
from maggma.core import Store

class ResupplyBuilder(GroupBuilder):
    """
    Simple builder that determines which items to resupply
    """

    def __init__(inventory: Store, resupply: Store,resupply_percent : int = 100, **kwargs):
        """
        Arguments:
            inventory: current inventory information
            resupply: target resupply information
            resupply_percent: the percent of the minimum to include in the resupply
        """
        self.inventory = inventory
        self.resupply = resupply
        self.resupply_percent = resupply_percent
        self.kwargs = kwargs

        super().__init__(source=inventory, target=resupply, grouping_properties=["type"], **kwargs)
```

Note that unlike the previous `MapBuilder` example, we didn't call the source and target stores as such. Providing more usefull names is a good idea in writing builders to make it clearer what the underlying data should look like.

`GroupBuilder` inherits from `MapBuilder` so it has the same configurational parameters.

- projection: list of the fields you want to project. This can reduce the data transfer load if you only need certain fields or sub-documents from the source documents
- timeout: optional timeout on the process function
- store_process_timeout: adds the process time into the target document for profiling
- retry_failed: retries running the process function on previously failed documents

One parameter that doens't work in `GroupBuilder` is `delete_orphans`, since the Many-to-One relationshop makes determining orphaned documents very difficult.

Finally let's get to the hard part which is running our function. We do this by defining `unary_function`

``` python
    def unary_function(self, items: List[Dict]) -> Dict:
        resupply = {}

        for item in items:
            if item["quantity"] > item["minimum"]:
                resupply[item["name"]] = int(item["minimum"] * self.resupply_percent )
            else:
                resupply[item["name"]] = 0
        return {"resupply": resupply}
```

Just as in `MapBuilder`, we're not returning all the extra information typically kept in the originally item. Normally, we would have to write code that copies over the source `key` and convert it to the target `key`. Same goes for the `last_updated_field`. `GroupBuilder` takes care of this, while also recording errors, processing time, and the Builder version.`GroupBuilder` also keeps a plural version of the `source.key` field, so in this example, all the `name` values wil be put together and kept in `names`
