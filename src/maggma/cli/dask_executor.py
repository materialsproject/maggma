#!/usr/bin/env/python
# coding utf-8

from logging import getLogger
from typing import List, Union

from maggma.cli.settings import CLISettings
from maggma.core import Builder

try:
    import dask
    from dask.distributed import LocalCluster, SSHCluster
except ImportError:
    raise ImportError("Both dask and distributed are required to use Dask as a broker")

settings = CLISettings()


def dask_executor(
    builders: List[Builder],
    dashboard_port: int,
    hostfile: str,
    dask_threads: int,
    dask_workers: int,
    memory_limit,
    processes: bool,
    scheduler_address: str,
    scheduler_port: int,
):
    """
    Dask executor for processing builders. Constructs Dask task graphs
    that will be submitted to a Dask scheduler for distributed processing
    on a Dask cluster.
    """
    logger = getLogger("Scheduler")

    if hostfile:
        with open(hostfile) as file:
            hostnames = file.read().split()

        logger.info(
            f"""Starting distributed Dask cluster, with scheduler at {hostnames[0]}:{scheduler_port},
            and workers at: {hostnames[1:]}:{scheduler_port}..."""
        )
    else:
        hostnames = None
        logger.info(f"Starting Dask LocalCluster with scheduler at: {scheduler_address}:{scheduler_port}...")

    client = setup_dask(
        dashboard_port=dashboard_port,
        hostnames=hostnames,
        memory_limit=memory_limit,
        n_workers=dask_workers,
        nthreads=dask_threads,
        processes=processes,
        scheduler_address=scheduler_address,
        scheduler_port=scheduler_port,
    )

    logger.info(f"Dask dashboard available at: {client.dashboard_link}")

    for builder in builders:
        logger.info(f"Working on {builder.__class__.__name__}")
        builder.connect()
        items = builder.get_items()

        task_graph = []
        for chunk in items:
            docs = dask.delayed(builder.get_processed_docs)(chunk)
            built_docs = dask.delayed(builder.process_item)(docs)
            update_store = dask.delayed(builder.update_targets)(built_docs)
            task_graph.append(update_store)

        dask.compute(*task_graph)

    client.shutdown()


def setup_dask(
    dashboard_port: int,
    hostnames: Union(List[str], None),
    memory_limit,
    n_workers: int,
    nthreads: int,
    processes: bool,
    scheduler_address: str,
    scheduler_port: int,
):
    logger = getLogger("Cluster")

    logger.info("Starting clutser...")

    if hostnames:
        cluster = SSHCluster(
            hosts=hostnames,
            scheduler_options={"port": scheduler_port, "dashboard_address": f":{dashboard_port}"},
            worker_options={"n_workers": n_workers, "nthreads": nthreads, "memory_limit": memory_limit},
        )
    else:
        cluster = LocalCluster(
            dashboard_address=f":{dashboard_port}",
            host=scheduler_address,
            memory_limit=memory_limit,
            n_workers=n_workers,
            processes=processes,
            scheduler_port=scheduler_port,
            threads_per_worker=nthreads,
        )

    logger.info(f"Cluster started with config: {cluster}")

    return cluster.get_client()
