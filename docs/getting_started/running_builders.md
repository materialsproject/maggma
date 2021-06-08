# Running Builders

`maggma` is designed to run build-pipelines in a production environment. Builders can be run directly in a python environment, but this gives you none of the performance features such as multiprocessing. The base `Builder` class implements a simple `run` method that can be used to run that builder:

``` python
class MultiplyBuilder(Builder):
    """
    Simple builder that multiplies the "a" sub-document by pre-set value
    """

    ...


my_builder = MultiplyBuilder(source_store,target_store,multiplier=3)
my_builder.run()
```

A better way to run this builder would be to use the `mrun` command line tool. Since evrything in `maggma` is MSONable, we can use `monty` to dump the builders into a JSON file:

``` python
from monty.serialization import dumpfn

dumpfn(my_builder,"my_builder.json")
```

Then we can run the builder using `mrun`:

``` shell
mrun my_builder.json
```

`mrun` has a number of usefull options:

``` shell
mrun --help
Usage: mrun [OPTIONS] [BUILDERS]...

Options:
  -v, --verbose                   Controls logging level per number of v's
  -n, --num-workers INTEGER RANGE
                                  Number of worker processes. Defaults to
                                  single processing
  --help                          Show this message and exit.
```

We can use the `-n` option to control how many workers run `process_items` in parallel.
Similarly, `-v` controls the logging verbosity from just WARNINGs to INFO to DEBUG output.

The result will be something that looks like this:

``` shell

2020-01-08 14:33:17,187 - Builder - INFO - Starting Builder Builder
2020-01-08 14:33:17,217 - Builder - INFO - Processing 100 items
Get: 100%|██████████████████████████████████| 100/100 [00:00<00:00, 15366.00it/s]
2020-01-08 14:33:17,235 - MultiProcessor - INFO - Processing batch of 1000 items
Update Targets: 100%|█████████████████████████| 100/100 [00:00<00:00, 584.51it/s]
Process Items: 100%|██████████████████████████| 100/100 [00:00<00:00, 567.39it/s]
```

There are progress bars for each of the three steps, which lets you understand what the slowest step is and the overall progress of the system.

## Running Distributed

`maggma` can distribute work across multiple computers. There are two steps to this:

1. Run a `mrun` manager by providing it with a `--url` to listen for workers on and `--num-chunks`(`-N`) which tells `mrun` how many sub-pieces to break up the work into. You can can run fewer workers then chunks. This will cause `mrun` to call the builder's `prechunk` to get the distribution of work and run distributd work on all workers
2. Run `mrun` workers b y providing it with a `--url` to listen for a manager and `--num-workers` (`-n`) to tell it how many processes to run in this worker.

The `url` argument takes a fully qualified url including protocol. `tcp` is recommended:
Example: `tcp://127.0.0.1:8080`


## Running Scripts

`mrun` has the ability to run Builders defined in python scripts or in jupyter-notebooks.

The only requirements are:

1. The builder file has to be in a sub-directory from where `mrun` is called.
2. The builders you want to run are in a variable called `__builder__` or `__builders__`

`mrun` will run the whole python/jupyter file, grab the builders in these variables and adds these builders to the builder queue.

Assuming you have a builder in a python file: `my_builder.py`
``` python
class MultiplyBuilder(Builder):
    """
    Simple builder that multiplies the "a" sub-document by pre-set value
    """

    ...

__builder__ = MultiplyBuilder(source_store,target_store,multiplier=3)
```

You can use `mrun` to run this builder and parallelize for you:
``` shell
mrun -n 2 -v my_builder.py
```


## Running multiple builders

`mrun` can run multiple builders. You can have multiple builders in a single file: `json`, `python`, or `jupyter-notebook`. Or you can chain multiple files in the order you want to run them:
``` shell
mrun -n 32 -vv my_first_builder.json builder_2_and_3.py last_builder.ipynb
```

`mrun` will then execute the builders in these files in order.


## Reporting Build State

`mrun` has the ability to report the status of the build pipeline to a user-provided `Store`. To do this, you first have to save the `Store` as a JSON or YAML file. Then you can use the `-r` option to give this to `mrun`. It will then periodicially add documents to the `Store` for one of 3 different events:

* `BUILD_STARTED` - This event tells us that a new builder started, the names of the `sources` and `targets` as well as the `total` number of items the builder expects to process
* `UPDATE` - This event tells us that a batch of items was processed and is going to `update_targets`. The number of items is stored in `items`.
* `BUILD_ENDED` - This event tells us the build process finished this specific builder. It also indicates the total number of `errors` and `warnings` that were caught during the process.

These event docs also contain the `builder`, a `build_id` which is unique for each time a builder is run and anonymous but unique ID for the machine the builder was run on.
