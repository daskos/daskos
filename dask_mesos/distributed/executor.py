from __future__ import absolute_import, division, print_function

import logging
import multiprocessing
import sys
from time import sleep
from threading import Thread
import traceback
from functools import partial

from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado import gen

from satyr.interface import Executor
from satyr.messages import PythonTaskStatus
from satyr.executor import ExecutorDriver


class TornadoExecutor(Executor):

    def __init__(self):
        self.loop = IOLoop()
        if not self.loop._running:
            self._thread = Thread(target=self.loop.start)
            self._thread.daemon = True
            self._thread.start()
            while not self.loop._running:
                sleep(0.001)

        self.tasks = {}
        self.workers = {}

    def on_launch(self, driver, task):
        # meyba detach to a thread
        self.tasks[task.id] = task
        status = partial(PythonTaskStatus, task_id=task.id)
        driver.update(status(state='TASK_RUNNING'))

        try:
            logging.info('Starting worker {}'.format(task.id))
            fn, args, kwargs = task.data
            worker = fn(loop=self.loop)
            self.workers[task.id] = worker
            self.loop.add_callback(worker._start)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb = ''.join(traceback.format_tb(exc_traceback))
            logging.exception('Worker errored with {}'.format(e))
            driver.update(status(state='TASK_FAILED',
                                 data=(e, tb),
                                 message=e.message))

    def on_kill(self, driver, task_id):
        task = self.tasks.pop(task_id)
        worker = self.workers.pop(task_id)
        status = PythonTaskStatus(task_id=task.id,
                                  state='TASK_FINISHED')  # or TASK_KILLED

        self.loop.add_callback(worker._close)  # wait until stops
        driver.update(status)

        if not len(self.workers):
            logging.info('Executor stops due to no more executing '
                         'tasks left')
            driver.stop()

    def on_shutdown(self, driver):
        with ignoring(gen.TimeoutError, StreamClosedError, OSError):
            yield All([w._close() for w in self.workers.values()])
        driver.stop()


if __name__ == '__main__':
    status = ExecutorDriver(TornadoExecutor()).run()
    code = 0 if status == mesos_pb2.DRIVER_STOPPED else 1
    sys.exit(code)
