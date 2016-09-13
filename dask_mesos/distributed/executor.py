from __future__ import absolute_import, division, print_function

import logging
import multiprocessing
import sys
import threading
import traceback
from functools import partial

from satyr.interface import Executor
from satyr.messages import PythonTaskStatus
from satyr.executor import ExecutorDriver, ThreadExecutor

### 
# 1. send task_running status update
# 2. start task (nanny) with the executor's ioloop
# 3. if any error occurs send task_failed
# 4. on_kill -> call nanny's _close coroutine and send task_finished or task_killed status update
#
# def run(self, driver, task):
#     status = partial(PythonTaskStatus, task_id=task.id)
#     driver.update(status(state='TASK_RUNNING'))
#     logging.info('Sent TASK_RUNNING status update')

#     try:
#         logging.info('Executing task...')
#         result = task()
#     except Exception as e:
#         exc_type, exc_value, exc_traceback = sys.exc_info()
#         tb = ''.join(traceback.format_tb(exc_traceback))
#         logging.exception('Task errored with {}'.format(e))
#         driver.update(status(state='TASK_FAILED',
#                              data=(e, tb),
#                              message=e.message))
#         logging.info('Sent TASK_RUNNING status update')
#     else:
#         driver.update(status(state='TASK_FINISHED', data=result))
#         logging.info('Sent TASK_FINISHED status update')
#     finally:
#         del self.tasks[task.id]
#         if self.is_idle():  # no more tasks left
#             logging.info('Executor stops due to no more executing '
#                          'tasks left')
#             driver.stop()


class TornadoExecutor(ThreadExecutor):

    def __init__(self):
        self.loop = IOLoop()
        self.tasks = {}

    def is_idle(self):
        return not len(self.tasks)

    def on_launch(self, driver, task):
        # TODO start task in the loop instead of a thread
        thread = threading.Thread(target=self.run, args=(driver, task))
        self.tasks[task.id] = thread  # track tasks runned by this executor
        thread.start()

    def on_kill(self, driver, task_id):
        self.tasks[task_id].stop() # nanny._close
        del self.tasks[task_id]

        if self.is_idle():  # no more tasks left
            logging.info('Executor stops due to no more executing '
                         'tasks left')
            driver.stop()


    def on_shutdown(self, driver):
        driver.stop()


if __name__ == '__main__':
    status = ExecutorDriver(ThreadExecutor()).run()
    code = 0 if status == mesos_pb2.DRIVER_STOPPED else 1
    sys.exit(code)
