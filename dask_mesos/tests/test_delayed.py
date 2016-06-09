from __future__ import absolute_import, division, print_function

import pytest
from concurrent.futures import Future
from dask.delayed import DelayedLeaf, delayed
from dask_mesos.delayed import mesos
from dask_mesos.executor import MesosExecutor, SatyrPack
from satyr.proxies.messages import Cpus, Disk, Mem


@pytest.yield_fixture
def executor():
    with MesosExecutor(name='test-get') as executor:
        yield executor


def test_mesos_is_delayed():
    def add(x, y):
        return x + y

    add1 = delayed(add)
    add2 = mesos(add)

    assert isinstance(add2, add1.__class__)
    assert add1(2, 3).compute() == add2(2, 3).compute()


def test_mesos_attributes():
    def mul(x, y):
        return x * y

    mul1 = mesos(mul, cpus=1, mem=128, disk=256)

    @mesos(cpus=3, docker='testimg', name='test')
    def mul2(x, y):
        return x * y

    expected1 = {'resources': [Cpus(1), Mem(128), Disk(256)],
                 'name': 'dask.mesos'}
    expected2 = {'resources': [Cpus(3), Mem(64), Disk(0)],
                 'docker': 'testimg',
                 'name': 'test'}

    assert isinstance(mul1, DelayedLeaf)
    assert isinstance(mul2, DelayedLeaf)
    assert isinstance(mul1._data, SatyrPack)
    assert isinstance(mul2._data, SatyrPack)
    assert mul1._data.params == expected1
    assert mul2._data.params == expected2


def test_delayed_compute(executor):
    @mesos(cpus=0.1, mem=128)
    def add(x, y):
        return x + y

    @mesos(cpus=0.2, mem=128)
    def mul(x, y):
        return x * y

    s = add(1, 2)
    m = mul(s, 10)
    z = add(s, m)

    assert z.compute(get=executor.get) == 33
    assert mul(s, z).compute(get=executor.get) == 99


def test_executor_async_compute(executor):
    @mesos(cpus=0.1, mem=128)
    def add(x, y):
        return x + y

    @mesos(cpus=0.2, mem=128)
    def mul(x, y):
        return x * y

    s = add(1, 2)
    m = mul(s, 10)
    z = add(s, m)

    f = executor.compute(z)
    assert isinstance(f, Future)
    assert f.result() == 33

    fs = executor.compute([z, m, s])
    assert isinstance(fs, list)
    assert all([isinstance(r, Future) for r in fs])

    assert [r.result() for r in fs] == [33, 30, 3]


def test_executor_sync_compute(executor):
    @mesos(cpus=0.1, mem=128)
    def add(x, y):
        return x + y

    @mesos(cpus=0.2, mem=128)
    def mul(x, y):
        return x * y

    s = add(1, 2)
    m = mul(s, 10)
    z = add(s, m)

    f = executor.compute(z, sync=True)
    assert f == 33

    fs = executor.compute([z, m, s], sync=True)
    assert fs == [33, 30, 3]
