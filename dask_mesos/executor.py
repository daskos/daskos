from __future__ import absolute_import, division, print_function

import logging
import os
from uuid import uuid4

from concurrent.futures import Future, ThreadPoolExecutor
from dask import compute
from dask.async import get_async
from dask.base import Base
from dask.context import _globals
from dask.optimize import cull, fuse
from kazoo.client import KazooClient
from satyr.apis.futures import MesosPoolExecutor
from satyr.queue import Queue
from toolz import first, merge


def get(dsk, keys, **kwargs):
    """Mesos implementation of dask.get
    Parameters
    ----------
    dsk: dict
        A dask dictionary specifying a workflow
    keys: key or list of keys
        Keys corresponding to desired data
    Examples
    --------
    >>> dsk = {'x': 1, 'y': 2, 'z': (inc, 'x'), 'w': (add, 'z', 'y')}
    >>> get(dsk, 'w')
    4
    >>> get(dsk, ['w', 'y'])
    (4, 2)
    """
    executor = _globals['executor']

    if executor is None:
        with MesosExecutor(name='dask-mesos-get') as executor:
            return executor.get(dsk, keys, **kwargs)
    else:
        return executor.get(dsk, keys, **kwargs)


# # TODO: default cache
class MesosExecutor(MesosPoolExecutor):

    def __init__(self, num_workers=-1,
                 zk=os.getenv('ZOOKEEPER_HOST', '127.0.0.1:2181'),
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

    def get(self, dsk, keys, optimize_graph=True, docker='lensa/dask.mesos',
            params={}, threaded=True, **kwargs):
        """ Compute dask graph
        Parameters
        ----------
        dsk: dict
        keys: object, or nested lists of objects
        optimize_graph: bool
        docker: string, default docker image for computations on mesos
        params: dict, mesos options per dask key
        threaded: bool, offload task without mesos parameters to threads
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

            if threaded and key not in params:
                logging.info('Task `{}` is calculating in threads'.format(key))
                return self.threadpool.submit(execute_task, *args)

            options = params.get(key, {})
            options['id'] = key
            if 'docker' not in options:
                options['docker'] = docker
            logging.info('Task `{}` is calculating on mesos'.format(key))
            return self.submit(execute_task, args, **options)

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
        from .delayed import MesosDelayed

        if isinstance(args, (list, tuple, set, frozenset)):
            singleton = False
        else:
            args = [args]
            singleton = True

        futures = []
        params = []
        for arg in args:
            if isinstance(arg, MesosDelayed):
                params.append(arg.params)
            if isinstance(arg, Base):
                f = Future()
                f.set_running_or_notify_cancel()
                futures.append(f)
            else:
                futures.append(arg)

        def setter(future):
            for f, r in zip(futures, future.result()):
                f.set_result(r)

        params = merge(*params)
        result = self.threadpool.submit(compute, *args, get=self.get,
                                        params=params, **kwargs)
        result.add_done_callback(setter)

        if sync:
            result.result()
            futures = [future.result() for future in futures]

        if singleton:
            return first(futures)
        else:
            return futures
