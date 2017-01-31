from __future__ import absolute_import, division, print_function

from operator import add

import pytest
from satyr.proxies.messages import Cpus, Disk, Mem

inc = lambda x: x + 1
dsk = {'x': 1,
       'y': 2,
       'z': (inc, 'x'),
       'w': (add, 'z', 'y')}


def test_get_threaded(executor):
    assert executor.get(dsk, 'w') == 4
    assert executor.submit.call_count == 0
    assert executor.threadpool.submit.call_count == 2

    assert executor.get(dsk, ['w', 'z']) == (4, 2)
    assert executor.submit.call_count == 0
    assert executor.threadpool.submit.call_count == 4


def test_get_nonthreaded(executor):
    assert executor.get(dsk, 'w', threaded=False) == 4
    assert executor.submit.call_count == 2
    assert executor.threadpool.submit.call_count == 0

    assert executor.get(dsk, ['w', 'z'], threaded=False) == (4, 2)
    assert executor.submit.call_count == 4
    assert executor.threadpool.submit.call_count == 0


def test_get_both(executor):
    params = {'z': {'resources': [Cpus(0.17), Mem(64), Disk(0)]}}
    assert executor.get(dsk, 'w', params=params) == 4
    assert executor.submit.call_count == 1
    assert executor.threadpool.submit.call_count == 1

    assert executor.get(dsk, ['w', 'z'], params=params) == (4, 2)
    assert executor.submit.call_count == 2
    assert executor.threadpool.submit.call_count == 2


def test_nested_get(executor):
    dsk = {'x': 1, 'y': 2, 'a': (add, 'x', 'y'), 'b': (sum, ['x', 'y'])}
    assert executor.get(dsk, ['a', 'b'], threaded=False) == (3, 3)
    assert executor.submit.call_count == 2


def test_get_without_computation(executor):
    dsk = {'x': 1}
    assert executor.get(dsk, 'x', threaded=False) == 1
    assert executor.submit.call_count == 0


def test_exceptions_rise_to_top(executor):
    def bad(x):
        raise ValueError()

    dsk = {'x': 1, 'y': (bad, 'x')}

    with pytest.raises(ValueError):
        executor.get(dsk, 'y', threaded=False)
        assert executor.submit.call_count == 1
