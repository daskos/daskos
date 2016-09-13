from __future__ import print_function, division, absolute_import

import os

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('satyr.scheduler')
logger.setLevel(logging.DEBUG)

from functools import partial
from threading import Thread
from time import sleep

from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado import gen

from distributed.deploy import LocalCluster
from satyr.scheduler import SchedulerDriver
from satyr import QueueScheduler, PythonTask
from satyr.messages import PythonExecutor, Cpus, Mem

from distributed import Nanny, Worker, Scheduler as DaskScheduler
from distributed.utils import ignoring, sync, All
from distributed.http.scheduler import HTTPScheduler


# NON FUNCTIONAL AT ALL RIGHT NOW
#python -m dask_mesos.distributed.mesos


### start loop in the background
# self.loop = loop or IOLoop()
# if not self.loop._running:
#     self._thread = Thread(target=self.loop.start)
#     self._thread.daemon = True
#     self._thread.start()
#     while not self.loop._running:
#         sleep(0.001)


class MesosCluster(SchedulerDriver):

    def __init__(self, n_workers=None, threads_per_worker=None,
                 loop=None, scheduler_host='127.0.0.1', scheduler_port=8786,
                 services={'http': HTTPScheduler}, *args, **kwargs):
        self.port = scheduler_port
        self.host = scheduler_host
        self.loop = loop or IOLoop()
        self.dask = DaskScheduler(loop=self.loop, ip=self.host,
                                  services=services)

        self.mesos = QueueScheduler()
        super(MesosCluster, self).__init__(self.mesos, *args, **kwargs)

    def start(self):
        super(MesosCluster, self).start()
        self.dask.start(self.port)
        self.workers = []

    def stop(self):
        """ Close the cluster """
        super(MesosCluster, self).stop()
        self.dask.close(fast=True)
        del self.workers[:]

    def start_worker(self, port=0, ncores=0, **kwargs):
        """ Add a new worker to the running cluster
        Parameters
        ----------
        port: int (optional)
            Port on which to serve the worker, defaults to 0 or random
        ncores: int (optional)
            Number of threads to use.  Defaults to number of logical cores
        nanny: boolean
            If true start worker in separate process managed by a nanny
        Examples
        --------
        >>> c = LocalCluster()  # doctest: +SKIP
        >>> c.start_worker(ncores=2)  # doctest: +SKIP
        Returns
        -------
        The created Worker or Nanny object.  Can be discarded.
        """
        def start_worker(W):
            loop = IOLoop()
            w = W(loop=loop)
            w.start()
            loop.start()

        N = partial(Nanny, self.host, self.port, **kwargs)
        executor = PythonExecutor(docker='lensa/dask.mesos')
        task = PythonTask(name='dask-worker', fn=start_worker, args=[N],
                          executor=executor, resources=[Cpus(0.5), Mem(256)])
        self.mesos.submit(task)
        self.workers.append(task)

    def stop_worker(self, w):
        """ Stop a running worker
        Examples
        --------
        >>> c = LocalCluster()  # doctest: +SKIP
        >>> w = c.start_worker(ncores=2)  # doctest: +SKIP
        >>> c.stop_worker(w)  # doctest: +SKIP
        """
        # kill mesos task
        self.workers.remove(w)

    def __del__(self):
        self.stop()

    @property
    def scheduler_address(self):
        return self.scheduler.address


if __name__ == '__main__':

    with MesosCluster(name='e') as m:
        m.start_worker()
        m.mesos.wait()
 

    
