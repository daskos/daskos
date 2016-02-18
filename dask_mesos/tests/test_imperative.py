from __future__ import absolute_import, division, print_function

from dask.imperative import do
from dask_mesos.imperative import mesos


def add(x, y):
    return x + y


def test_mesos_is_do():
    add1 = do(add)
    add2 = mesos(add)

    assert add1.__name__ == add2.__name__
    assert add1(2, 3).compute() == add2(2, 3).compute()


def test_mesos_attributes():
    add1 = mesos(add, cpus=1, mem=128, disk=256)
    add2 = mesos(add, docker='testimg', cpus=3)

    assert add1.mesos_settings == {'cpus': 1, 'mem': 128, 'disk': 256}
    assert add2.mesos_settings == {'cpus': 3, 'docker': 'testimg'}
