from __future__ import absolute_import, division, print_function

import pytest
from dask_mesos.executor import MesosExecutor


@pytest.yield_fixture
def executor(mocker):
    with MesosExecutor(name='test-get') as executor:
        mocker.spy(executor, 'submit')
        mocker.spy(executor.threadpool, 'submit')
        yield executor
