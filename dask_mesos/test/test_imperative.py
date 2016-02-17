import pytest

from dask.imperative import do
from dask.utils import raises

from operator import add
from debas.imperative import mesos


def test_mesos_is_do():
    add1 = do(add)
    add2 = mesos(add)

    assert add1.__name__ == add2.__name__
    assert add1(2, 3).compute() == add2(2, 3).compute()


def test_mesos_attributes():
    add1 = mesos(add, cpu=1, mem=128, disk=256)
    add2 = mesos(add, docker='testimg', cpu=3)

    assert add1.resources == {'cpu': 1,
                              'mem': 128,
                              'disk': 256}
    assert add2.resources == {'cpu': 3,
                              'mem': 128,
                              'disk': None}
    assert add2.docker == 'testimg'
