from __future__ import absolute_import, division, print_function

import os
import pytest
import logging
from operator import add, mul
from cloudpickle import loads
from dask import get
from dask.callbacks import Callback
from dask_mesos.context import Lock, Persist
from kazoo.client import NoNodeError
from kazoo.recipe.lock import LockTimeout


zookeeper_host = os.environ.get('ZOOKEEPER_HOST')


@pytest.yield_fixture
def zk():
    pytest.importorskip('kazoo')
    pytest.mark.skipif(zookeeper_host is None,
                       reason='No ZOOKEEPER_HOST envar defined')

    from kazoo.client import KazooClient
    zk = KazooClient(hosts=zookeeper_host)
    try:
        zk.start()
        yield zk
    finally:
        zk.delete('/epos', recursive=True)
        zk.stop()


class Ran(Callback):

    def __init__(self, *args, **kwargs):
        self.steps = []

    def _pretask(self, key, dsk, state):
        self.steps.append(key)


@pytest.fixture(scope="module")
def dsk1():
    return {'x': 1,
            'y': 2,
            'z': (add, 'x', 'y'),  # 3
            'w': (sum, ['x', 'y', 'z'])}  # 6


@pytest.fixture(scope="module")
def dsk2():
    return {'a': 2,
            'b': 5,
            'e': (mul, 'a', 'b'),  # 10
            'f': (lambda a, power: a ** power, 'a', 2),  # 4
            's': (lambda x, y: x / y, 'f', 'e'),  # 0.4
            'w': (add, 'f', 's')}  # 4.4


def test_persisting(zk, dsk1):
    with Persist(zk, name="dsk1"):
        with Ran() as r:
            assert get(dsk1, 'w') == 6
            assert r.steps == ['z', 'w']
        with Ran() as r:
            assert get(dsk1, 'w') == 6
            assert r.steps == []

        assert loads(zk.get("/epos/dsk1/z")[0]) == 3
        assert loads(zk.get("/epos/dsk1/w")[0]) == 6

    # tests ephemeral=False, znode still exists after context handler
    assert loads(zk.get("/epos/dsk1/w")[0]) == 6


def test_ephemeral_persisting(zk, dsk2):
    with Persist(zk, name="dsk2", ns="/test/dags", ephemeral=True):
        with Ran() as r:
            assert get(dsk2, 'e') == 10
            assert r.steps == ['e']
        with Ran() as r:
            assert get(dsk2, 's') == 0.4
            assert r.steps == ['f', 's']
        with Ran() as r:
            assert get(dsk2, 's') == 0.4
            assert r.steps == []

        assert loads(zk.get("/test/dags/dsk2/e")[0]) == 10

    with pytest.raises(NoNodeError):
        zk.get("/test/dags/dsk2/e")


def test_locking(zk, dsk1):
    with pytest.raises(LockTimeout):
        # two identical locks; the second cannot acquire
        with Lock(zk, name="dsk1", timeout=1), \
                Lock(zk, name="dsk1", timeout=1):
            get(dsk1, 'w')

    # test lock release
    with Lock(zk, name="dsk1"):
        get(dsk1, 'w')
        get(dsk1, 'w')


def test_ephemeral_locking(zk, dsk2):
    with pytest.raises(LockTimeout):
        with Lock(zk, name="dsk2", timeout=1, ephemeral=True), \
                Lock(zk, name="dsk2", timeout=1, ephemeral=True):
            get(dsk2, 'f')

    with pytest.raises(NoNodeError):
        zk.get("/epos/dsk2")
