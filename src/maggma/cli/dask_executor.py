#!/usr/bin/env/python
# coding utf-8

from logging import getLogger
from typing import List

from maggma.cli.settings import CLISettings
from maggma.core import Builder

try:
    import dask
    from dask.distributed import LocalCluster, SSHCluster
except ImportError:
    raise ImportError("Both dask and distributed are required to use Dask as a broker")

settings = CLISettings()


def dask_executor(
    scheduler_address: str,
    scheduler_port: int,
    dask_hosts: str,
    builders: List[Builder],
    dask_workers: int,
    processes: bool,
):
    """
    Dask executor for processing builders. Constructs Dask task graphs
    that will be submitted to a Dask scheduler for distributed processing
    on a Dask cluster.
    """
    logger = getLogger("Scheduler")

    if dask_hosts:
        with open(dask_hosts) as file:
            dask_hosts = file.read().split()

        logger.info(
            f"""Starting distributed Dask cluster, with scheduler at {dask_hosts[0]}:{scheduler_port},
            and workers at: {dask_hosts[1:]}:{scheduler_port}..."""
        )
    else:
        logger.info(f"Starting Dask LocalCluster with scheduler at: {scheduler_address}:{scheduler_port}...")

    client = setup_dask(
        address=scheduler_address, port=scheduler_port, hosts=dask_hosts, n_workers=dask_workers, processes=processes
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


def setup_dask(address: str, port: int, hosts: List[str], n_workers: int, processes: bool):
    logger = getLogger("Cluster")

    logger.info("Starting clutser...")

    if hosts:
        cluster = SSHCluster(hosts=hosts, scheduler_port=port, n_workers=n_workers)
    else:
        cluster = LocalCluster(host=address, scheduler_port=port, n_workers=n_workers, processes=processes)

    logger.info(f"Cluster started with config: {cluster}")

    return cluster.get_client()
