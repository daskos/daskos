from __future__ import absolute_import, division, print_function

import os
from copy import copy
from uuid import uuid4

from dask.async import get_async
from dask.context import _globals
from dask.optimize import cull, fuse
from kazoo.client import KazooClient
from satyr.apis.multiprocessing import Pool, Queue
from toolz import curry, partial, pipe


class SatyrPack(object):

    def __init__(self, fn, params=None):
        self.fn = fn
        self.params = params

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def __getstate__(self):
        return {'fn': self.fn}


def get(dsk, keys, optimizations=[], num_workers=None,
        docker='lensa/dask.mesos',
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
    dsk2, dependencies = cull(dsk, keys)
    dsk3, dependencies = fuse(dsk2, keys, dependencies)
    dsk4 = pipe(dsk3, *optimizations)

    def apply_async(execute_task, args):
        key = args[0]
        func = args[1][0]
        params = func.params if isinstance(func, SatyrPack) else {}

        params['id'] = key
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
