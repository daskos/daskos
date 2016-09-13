from __future__ import absolute_import, division, print_function

import pytest

from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado import gen

from satyr.messages import PythonTask, PythonTaskStatus
from satyr.utils import RemoteException
from satyr import ExecutorDriver

from distributed import Nanny
from dask_mesos.distributed import TornadoExecutor


def create_nanny(loop):
    return Nanny('127.0.0.1', 8989, loop=loop)


def test_worker_status_updates(mocker):
    executor = TornadoExecutor()
    assert isinstance(executor.loop, IOLoop)

    driver = mocker.Mock()

    task = PythonTask(fn=create_nanny)
    executor.on_launch(driver, task)

    calls = driver.update.call_args_list
    args, kwargs = calls[0]
    status = args[0]
    assert isinstance(status, PythonTaskStatus)
    assert status.state == 'TASK_RUNNING'
    assert status.data is None
    assert len(calls) == 1


def test_start_multiple(mocker):
    executor = TornadoExecutor()
    driver = mocker.Mock()

    task = PythonTask(fn=create_nanny)
    executor.on_launch(driver, task)
    assert len(executor.workers) == 1

    task = PythonTask(fn=create_nanny)
    executor.on_launch(driver, task)
    assert len(executor.workers) == 2

    task = PythonTask(fn=create_nanny)
    executor.on_launch(driver, task)
    assert len(executor.workers) == 3


def test_start_stop_single(mocker):
    executor = TornadoExecutor()
    driver = mocker.Mock()

    task1 = PythonTask(fn=create_nanny)
    task2 = PythonTask(fn=create_nanny)

    executor.on_launch(driver, task1)
    assert executor.workers[task1.id]

    executor.on_launch(driver, task2)
    assert executor.workers[task2.id]

    executor.on_kill(driver, task1.id)
    assert len(executor.workers) == 1
    assert task1.id not in executor.workers
    assert task2.id in executor.workers
