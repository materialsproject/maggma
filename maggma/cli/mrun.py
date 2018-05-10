#!/usr/bin/env python
# coding utf-8

from maggma.runner import Runner
from monty.serialization import loadfn
import argparse
import logging
import sys
from maggma.utils import TqdmLoggingHandler


def main():
    parser = argparse.ArgumentParser(description="mrun is a script to run builders written using the Maggma framework.")
    parser.add_argument(
        "builder",
        help="Builder file in either json or yaml format. Can contain a list of builders or a predefined Runner")
    parser.add_argument(
        "-n",
        "--num_workers",
        type=int,
        default=0,
        help="Number of worker processes. Defaults to use as many as available.")
    parser.add_argument('-v', '--verbose', action='count', default=0, help="Controls logging level per number of v's")
    parser.add_argument(
        "--dry_run",
        action="store_true",
        default=False,
        help="Dry run loading the builder file. Does not run the builders")
    args = parser.parse_args()

    # Set Logging
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(len(levels) - 1, args.verbose)]  # capped to number of levels
    root = logging.getLogger()
    root.setLevel(level)
    ch = TqdmLoggingHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    root.addHandler(ch)

    objects = loadfn(args.builder)

    if isinstance(objects, list):
        # If this is a list of builders
        runner = Runner(objects, num_workers=args.num_workers)
    elif isinstance(objects, Runner):
        # This is a runner:
        root.info("Changing number of workers from default in input file")
        runner = Runner(objects.builders, args.num_workers)
    else:
        root.error("Couldn't properly read the builder file.")

    if not args.dry_run:
        runner.run()


if __name__ == "__main__":
    main()
