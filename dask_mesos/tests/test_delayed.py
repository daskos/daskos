from __future__ import absolute_import, division, print_function

import time
from dask.delayed import delayed, DelayedLeaf
from dask_mesos import get, mesos
from dask_mesos.satyr import SatyrPack
from satyr.proxies.messages import Cpus, Disk, Mem


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


def test_mesos_compute():
    @mesos(cpus=0.1)
    def add(x, y):
        time.sleep(10)
        return x + y

    @mesos(cpus=0.2)
    def mul(x, y):
        time.sleep(5)
        return x * y

    s = add(1, 2)
    m = mul(s, 10)
    z = add(s, m)

    assert z.compute(get=get) == 33
