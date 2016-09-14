from __future__ import print_function, division, absolute_import

import os

import logging

from functools import partial
from threading import Thread
from time import sleep

from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado import gen

from distributed.deploy import LocalCluster
from satyr.scheduler import SchedulerDriver
from satyr import QueueScheduler, PythonTask
from satyr.messages import PythonExecutor, Cpus, Mem, Disk

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

    def __init__(self, n_workers=0, threads_per_worker=None, loop=None,
                 name='dask-distributed', master=os.getenv('MESOS_MASTER'),
                 worker_cpus=1, worker_memory=512, worker_disk=0, docker='lensa/dask.mesos',
                 scheduler_host='127.0.0.1', scheduler_port=8786,
                 services={'http': HTTPScheduler}, silence_logs=logging.CRITICAL):
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

        self.n_workers = n_workers
        self.worker_cpus = worker_cpus
        self.worker_memory = worker_memory
        self.worker_disk = worker_disk
        self.docker = docker

        self.scheduler = DaskScheduler(loop=self.loop, ip=self.host,
                                       services=services)
        self.mesos = QueueScheduler()
        super(MesosCluster, self).__init__(self.mesos, name=name, master=master)

    def start(self):
        super(MesosCluster, self).start()
        self.scheduler.start(self.port)
        self.workers = []

        for i in range(self.n_workers):
            self.start_worker(name='dask-worker-{}'.format(i))

    def stop(self):
        """ Close the cluster """
        super(MesosCluster, self).stop()
        self.scheduler.close(fast=True)
        del self.workers[:]

    def start_worker(self, port=0, ncores=0, name='dask-worker',
                     cpus=None, memory=None, disk=None, **kwargs):
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
        resources = [Cpus(cpus or self.worker_cpus),
                     Mem(memory or self.worker_memory),
                     Disk(disk or self.worker_disk)]
        callable = partial(Nanny, self.host, self.port, **kwargs)

        executor = DaskExecutor(docker=self.docker)
        task = PythonTask(name=name, fn=callable, executor=executor,
                          resources=resources)

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
        task_id = TaskID(value=w)
        task = self.mesos.tasks[task_id]

        # python task inited with this state, hopefully mesos doesn't use it internally
        if task.status.state == 'TASK_STAGING':  
            del self.mesos.tasks[task_id]
        else:
            self.kill(task_id)

        self.workers.remove(w)

    # def __del__(self):
    #    self.stop()

    @property
    def scheduler_address(self):
        return self.scheduler.address

