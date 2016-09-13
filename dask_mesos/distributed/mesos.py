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

from mesos.interface import mesos_pb2
from satyr.proxies.messages import TaskID


class DaskExecutor(PythonExecutor):

    proto = mesos_pb2.ExecutorInfo(
        labels=mesos_pb2.Labels(
            labels=[mesos_pb2.Label(key='dask')]))

    def __init__(self, docker='lensa/dask.mesos', *args, **kwargs):
        super(DaskExecutor, self).__init__(docker=docker, *args, **kwargs)
        self.command.value = 'python -m dask_mesos.distributed.executor'


class MesosCluster(SchedulerDriver):

    def __init__(self, n_workers=None, threads_per_worker=None,
                 loop=None, scheduler_host='127.0.0.1', scheduler_port=8786,
                 services={'http': HTTPScheduler}, silence_logs=logging.CRITICAL,
                 *args, **kwargs):
        if silence_logs:
            for l in ['distributed.scheduler',
                      'distributed.worker',
                      'distributed.core',
                      'distributed.nanny']:
                logging.getLogger(l).setLevel(silence_logs)
        self.port = scheduler_port
        self.host = scheduler_host
        self.loop = loop or IOLoop()
        if not self.loop._running:
            self._thread = Thread(target=self.loop.start)
            self._thread.daemon = True
            self._thread.start()
            while not self.loop._running:
                sleep(0.001)

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
        executor = DaskExecutor(docker='lensa/dask.mesos')
        task = PythonTask(name='dask-worker', fn=partial(Nanny, self.host, self.port, **kwargs),
                          executor=executor, resources=[Cpus(0.2), Mem(128)])
        self.mesos.submit(task)
        self.workers.append(task.id.value)
        return task.id.value

    def stop_worker(self, w):
        """ Stop a running worker
        Examples
        --------
        >>> c = LocalCluster()  # doctest: +SKIP
        >>> w = c.start_worker(ncores=2)  # doctest: +SKIP
        >>> c.stop_worker(w)  # doctest: +SKIP
        """
        self.kill(TaskID(value=w))
        self.workers.remove(w)

    # def __del__(self):
    #    self.stop()

    @property
    def scheduler_address(self):
        return self.scheduler.address


if __name__ == '__main__':
    import time
    with MesosCluster(name='e', silence_logs=logging.INFO) as m:
        w1 = m.start_worker()
        time.sleep(5)
        w2 = m.start_worker()
        time.sleep(5)
        w3 = m.start_worker()
        time.sleep(5)
        m.stop_worker(w3)
        time.sleep(5)
        m.stop_worker(w2)
        m.mesos.wait()
 

    
