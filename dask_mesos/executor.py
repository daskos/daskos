from __future__ import absolute_import, division, print_function

from satyr.apis.futures import MesosPoolExecutor

import os
from copy import copy
from uuid import uuid4
from toolz import curry, partial, pipe, first

from dask import compute
from dask.base import Base
from dask.async import get_async
from dask.optimize import cull, fuse

from kazoo.client import KazooClient
from satyr.apis.multiprocessing import Pool, Queue

from concurrent.futures import ThreadPoolExecutor, Future



# TODO: default cache

class SatyrPack(object):

    def __init__(self, fn, params=None):
        self.fn = fn
        self.params = params

    def __repr__(self):
        return 'satyr-{}'.format(self.fn.__name__)

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def __getstate__(self):
        return {'fn': self.fn}


class MesosExecutor(MesosPoolExecutor):

    def __init__(self, num_workers=-1, zk=os.getenv('ZOOKEEPER_HOST', '127.0.0.1:2181'),
                 *args, **kwargs):
        self.zk = KazooClient(hosts=zk)
        super(MesosExecutor, self).__init__(*args, **kwargs)

    def start(self):
        self.zk.start()
        self.threadpool = ThreadPoolExecutor(max_workers=int(10e6))
        super(MesosExecutor, self).start()
        return self

    def stop(self):
        super(MesosExecutor, self).stop()
        self.threadpool.shutdown()
        self.zk.stop()

    def get(self, dsk, keys, optimize_graph=True, docker='lensa/dask.mesos', **kwargs):
        """ Compute dask graph
        Parameters
        ----------
        dsk: dict
        keys: object, or nested lists of objects
        restrictions: dict (optional)
            A mapping of {key: {set of worker hostnames}} that restricts where
            jobs can take place
        Examples
        --------
        >>> from operator import add  # doctest: +SKIP
        >>> e = MesosExecutor('127.0.0.1:8787')  # doctest: +SKIP
        >>> e.get({'x': (add, 1, 2)}, 'x')  # doctest: +SKIP
        3
        See Also
        --------
        Executor.compute: Compute asynchronous collections
        """
        # Optimize Dask
        dsk2, dependencies = cull(dsk, keys)
        if optimize_graph:
            dsk3, dependencies = fuse(dsk2, keys, dependencies)
        else:
            dsk3 = dsk2

        def apply_async(execute_task, args):
            key = args[0]
            func = args[1][0]
            params = func.params if isinstance(func, SatyrPack) else {}

            params['id'] = key
            if 'docker' not in params:
                params['docker'] = docker

            return self.submit(execute_task, args, **params)

        # Run
        queue = Queue(self.zk, str(uuid4()))
        result = get_async(apply_async, 1e4, dsk3, keys, queue=queue, **kwargs)

        return result


    def compute(self, args, sync=False, **kwargs):
        """ Compute dask collections on cluster
        Parameters
        ----------
        args: iterable of dask objects or single dask object
            Collections like dask.array or dataframe or dask.value objects
        sync: bool (optional)
            Returns Futures if False (default) or concrete values if True
        Returns
        -------
        List of Futures if input is a sequence, or a single future otherwise
        Examples
        --------
        >>> from dask import do, value
        >>> from operator import add
        >>> x = dask.do(add)(1, 2)
        >>> y = dask.do(add)(x, x)
        >>> xx, yy = executor.compute([x, y])  # doctest: +SKIP
        >>> xx  # doctest: +SKIP
        <Future: status: finished, key: add-8f6e709446674bad78ea8aeecfee188e>
        >>> xx.result()  # doctest: +SKIP
        3
        >>> yy.result()  # doctest: +SKIP
        6
        Also support single arguments
        >>> xx = executor.compute(x)  # doctest: +SKIP
        See Also
        --------
        Executor.get: Normal synchronous dask.get function
        """
        if isinstance(args, (list, tuple, set, frozenset)):
            singleton = False
        else:
            args = [args]
            singleton = True

        futures = []
        for arg in args:
            if isinstance(arg, Base):
                f = Future()
                f.set_running_or_notify_cancel()
                futures.append(f)
            else:
                futures.append(arg)

        def setter(future):
            for f, r in zip(futures, future.result()):
                f.set_result(r)

        result = self.threadpool.submit(compute, *args, get=self.get, **kwargs)
        result.add_done_callback(setter)

        if sync:
            result.result()
            futures = [f.result() for f in futures]

        if singleton:
            return first(futures)
        else:
            return futures
