from __future__ import absolute_import, division, print_function

from concurrent.futures import Future
from dask import delayed
from daskos.delayed import MesosDelayed, MesosDelayedLeaf, mesos
from daskos.utils import key_split
from satyr.proxies.messages import Cpus, Disk, Mem


# tests are not modules, so these are not picklable
@mesos(cpus=0.1, mem=256, docker='test1')
def add(x, y):
    return x + y


@mesos(cpus=0.2, mem=128, docker='test2')
def mul(x, y):
    return x * y

add_params = {'docker': 'test1',
              'force_pull': False,
              'resources': [Cpus(0.1), Mem(256), Disk(0)],
              'envs': {},
              'uris': []}
mul_params = {'docker': 'test2',
              'force_pull': False,
              'resources': [Cpus(0.2), Mem(128), Disk(0)],
              'envs': {},
              'uris': []}


def test_mesos_is_delayed():
    def add(x, y):
        return x + y

    add1 = delayed(add)
    add2 = mesos(add)

    assert isinstance(add2, add1.__class__)
    assert add1(2, 3).compute() == add2(2, 3).compute()


def test_mesos_delayed_types():
    s = add(1, 2)
    m = mul(s, 10)
    z = add(s, m)

    assert isinstance(add, MesosDelayedLeaf)
    assert isinstance(mul, MesosDelayedLeaf)
    assert isinstance(s, MesosDelayed)
    assert isinstance(m, MesosDelayed)
    assert isinstance(z, MesosDelayed)


def test_mesos_delayed_params():
    s = add(1, 2)
    m = mul(s, 10)
    z = add(s, m)

    assert add.params[add.key] == add_params
    assert mul.params[mul.key] == mul_params

    for d in [s, m, z]:
        assert d.dask.keys() == d.params.keys()

        for key, params in d.params.items():
            fn = key_split(key)
            if fn == 'add':
                assert params == add_params
            elif fn == 'mul':
                assert params == mul_params


def test_mesos_delayed_compute():
    @mesos(cpus=0.1, mem=128)
    def add(x, y):
        return x + y

    @mesos(cpus=0.11, mem=129)
    def mul(x, y):
        return x * y

    s = add(1, 2)
    m = mul(s, 10)
    z = add(s, m)
    w = add(s, s + 2)  # s + 2 calculated in threads

    assert z.compute() == 33
    assert mul(s, z).compute() == 99
    assert w.compute() == 8


def test_mesos_executor_async_compute(executor):
    @mesos(cpus=0.11, mem=128)
    def add(x, y):
        return x + y

    @mesos(cpus=0.1, mem=129)
    def mul(x, y):
        return x * y

    s = add(1, 2)
    m = mul(s, 10)
    z = add(s, m)
    w = add(s, s + 2)  # s + 2 calculated in threads

    f = executor.compute(z)
    assert isinstance(f, Future)
    assert f.result() == 33
    assert executor.submit.call_count == 3

    fs = executor.compute([z, m, s])
    assert isinstance(fs, list)
    assert all([isinstance(r, Future) for r in fs])
    assert [r.result() for r in fs] == [33, 30, 3]
    assert executor.submit.call_count == 6

    f = executor.compute(w)
    assert isinstance(f, Future)
    assert f.result() == 8
    assert executor.submit.call_count == 8  # inly incremented by 2 instead of 3


def test_mesos_executor_sync_compute(executor):
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
