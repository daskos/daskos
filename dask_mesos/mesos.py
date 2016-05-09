from __future__ import absolute_import, division, print_function

import os
from uuid import uuid4

from dask.async import get_async
from dask.context import _globals
from dask.optimize import cull, fuse
from kazoo.client import KazooClient
from satyr.apis.multiprocessing import Pool, Queue
from toolz import curry, partial, pipe


def get(dsk, keys, optimizations=[], num_workers=None,
        docker='lensa/dask.mesos:latest',
        zk=os.getenv('ZOOKEEPER_HOST', '127.0.0.1:2181'),
        mesos=os.getenv('MESOS_MASTER', '127.0.0.1:5050'),
        **kwargs):
    """Mesos get function appropriate for Bags

    Parameters
    ----------

    dsk: dict
        dask graph
    keys: object or list
        Desired results from graph
    optimizations: list of functions
        optimizations to perform on graph before execution
    num_workers: int
        Number of worker processes (defaults to number of cores)
    docker: string
        Default docker image name to run the dask in
    zk: string
        Zookeeper host and port the distributed Queue should connect to
    mesos: string
        Mesos Master hostname and port the Satyr framework should connect to
    """
    pool, kazoo = _globals['pool'], _globals['kazoo']

    if pool is None:
        pool = Pool(name='dask-pool', master=mesos, processes=num_workers)
        pool.start()
        cleanup_pool = True
    else:
        cleanup_pool = False

    if kazoo is None:
        kazoo = KazooClient(hosts=zk)
        kazoo.start()
        cleanup_kazoo = True
    else:
        cleanup_kazoo = False

    # Optimize Dask
    dsk2 = fuse(dsk, keys)
    dsk3 = pipe(dsk2, partial(cull, keys=keys), *optimizations)

    def apply_async(execute_task, args):
        try:
            func = args[1][0]

            # extract satyr params from function property
            params = func.satyr
            del func.satyr

            # recreate task definition from func and its args
            args[1] = (func,) + args[1][1:]
        except AttributeError:
            params = {}
        finally:
            if 'docker' not in params:
                params['docker'] = docker

        return pool.apply_async(execute_task, args, **params)

    try:
        # Run
        queue = Queue(kazoo, str(uuid4()))
        result = get_async(apply_async, 1e4, dsk3, keys, queue=queue, **kwargs)
    finally:
        if cleanup_kazoo:
            kazoo.stop()
        if cleanup_pool:
            pool.stop()

    return result
