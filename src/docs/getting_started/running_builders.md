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
