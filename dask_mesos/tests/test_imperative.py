from __future__ import absolute_import, division, print_function

from dask import delayed
from dask_mesos.mesos import get
from dask_mesos.imperative import mesos
from satyr.proxies.messages import Cpus, Mem, Disk


def test_mesos_is_delayed():
    def add(x, y):
        return x + y

    add1 = delayed(add)
    add2 = mesos(add)

    assert add1.__name__ == add2.__name__
    assert add1(2, 3).compute() == add2(2, 3).compute()


def test_mesos_attributes():
    def mul(x, y):
        return x * y

    mul1 = mesos(mul, cpus=1, mem=128, disk=256)

    @mesos(cpus=3, docker='testimg')
    def mul2(x, y):
        return x * y

    expected1 = {'resources': [Cpus(1), Mem(128), Disk(256)]}
    expected2 = {'resources': [Cpus(3), Mem(64), Disk(0)],
                 'docker': 'testimg'}
    assert mul1.satyr == expected1
    assert mul2.satyr == expected2


def test_mesos_compute():
    @mesos(cpus=0.1)
    def add(x, y):
        return x + y

    @mesos(cpus=0.2)
    def mul(x, y):
        return x * y

    s = add(1, 2)
    m = mul(s, 10)
    z = add(s, m)

    assert z.compute(get=get) == 33
