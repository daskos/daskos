from functools import partial
import sys
from time import sleep, time
import unittest

import logging
import pytest

from satyr.utils import timeout

from distributed.deploy.local import LocalCluster
from distributed import Client, Worker, Nanny

from distributed.deploy.utils_test import ClusterTest
from dask_mesos.distributed import MesosCluster
from satyr.proxies.messages import TaskID
from distributed import Client


def wait_for_deployment(mc, seconds=10):
    with timeout(seconds):
        while not all([s.state == 'TASK_RUNNING'
                       for s in mc.mesos.statuses.values()]):
            sleep(0.01)

def test_start_stop_workers():
    with MesosCluster(silence_logs=logging.INFO,
                      worker_cpus=0.1,
                      worker_memory=128) as mc:
        w1 = mc.start_worker()
        assert len(mc.mesos.tasks) == 1
        assert len(mc.workers) == 1
        assert w1 in mc.workers

        w2 = mc.start_worker()
        assert len(mc.mesos.tasks) == 2
        assert len(mc.workers) == 2
        assert w2 in mc.workers

        w3 = mc.start_worker()
        assert len(mc.mesos.tasks) == 3
        assert len(mc.workers) == 3
        assert w3 in mc.workers

        wait_for_deployment(mc, 30)

        mc.stop_worker(w3)
        assert len(mc.workers) == 2
        assert len(mc.mesos.tasks) == 3
        assert w3 not in mc.workers
        with timeout(10):
            while TaskID(value=w3) in mc.mesos.tasks:
                sleep(0.05)

        mc.stop_worker(w1)
        assert len(mc.workers) == 1
        assert len(mc.mesos.tasks) == 2
        assert w1 not in mc.workers
        with timeout(10):
            while TaskID(value=w1) in mc.mesos.tasks:
                sleep(0.05)

        mc.stop_worker(w2)
        assert len(mc.workers) == 0
        assert len(mc.mesos.tasks) == 1
        assert w1 not in mc.workers
        with timeout(10):
            while TaskID(value=w2) in mc.mesos.tasks:
                sleep(0.05)

        assert len(mc.mesos.tasks) == 0


def test_simple():
    def inc(x):
        return x + 1

    with MesosCluster(3, silence_logs=False,
                      worker_cpus=0.3, worker_memory=128) as mc:
        with Client((mc.scheduler.ip, mc.scheduler.port)) as e:
            x = e.submit(inc, 1)
            assert x.result() == 2
            assert x.key in mc.scheduler.tasks



