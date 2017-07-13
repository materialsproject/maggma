from maggma.helpers import get_mpi


def _run_builder_in_mpi_collective_comm(builder, scatter=True):
    """
    Since all the items to be processed are fetched on the master node at once, this
    implementation could be problematic if there are large number of items or small number of
    large items.

    At the moment it is hard to get around this: only pickleable objects can be passed
    around using MPI and generators/Queues(uses thread locking internally) are not pickleable!!

    Args:
        builder (Builder): Any object of class that subclasses Builder
        scatter (bool): if True then the items are scattered from the master to slaves, else
            broadcasted.
    """

    (comm, rank, size) = get_mpi()

    items = None

    # get all items at the master
    if rank == 0:
        items = list(builder.get_items())

    # pad items if necessary and scatter it to the slaves
    # ==>
    # large memory consumption(if the number and/or size of the items are large) ONLY at the master
    if scatter:
        itm = None
        if rank == 0:
            n = len(items)
            chunk_size, n_chunks = (n // size, size) if size <= n else (1, n)
            itm = [items[r * chunk_size:(r + 1) * chunk_size] for r in range(n_chunks)]
            # if there are processes than elements, pad the scattering list
            itm.extend([[] for _ in range(max(0, size - n))])
            if 0 < n % size < n:
                itm[-1].extend(items[size * chunk_size:])
                # print("size", size, chunk_size)

        itm = comm.scatter(itm, root=0)

        builder.process_item(itm)

    # broadcast all items from the master to the slaves.
    # ==>
    # large memory consumption(if the number and/or size of the items are large) on ALL NODES.
    else:
        items = comm.bcast(items, root=0) if comm else items

        n = len(items)
        chunk_size = n // size

        # adjust chuck size if the data size is not divisible by the
        # number of processors
        if rank == 0:
            if n % size != 0:
                chunk_size = chunk_size + n % size

        items_chunk = items[rank:rank + chunk_size]

        for itm in items_chunk:
            builder.process_item(itm)
