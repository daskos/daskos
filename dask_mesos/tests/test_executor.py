from operator import add

import pytest
from dask.context import set_options
#from dask_mesos import get
from dask_mesos.executor import MesosExecutor

inc = lambda x: x + 1


@pytest.yield_fixture
def executor():
    with MesosExecutor(name='test-get') as executor:
        yield executor


def test_get(executor):
    dsk = {'x': 1,
           'y': 2,
           'z': (inc, 'x'),
           'w': (add, 'z', 'y')}
    assert executor.get(dsk, 'w') == 4
    assert executor.get(dsk, ['w', 'z']) == (4, 2)


def test_nested_get(executor):
    dsk = {'x': 1, 'y': 2, 'a': (add, 'x', 'y'), 'b': (sum, ['x', 'y'])}
    assert executor.get(dsk, ['a', 'b']) == (3, 3)


def test_get_without_computation(executor):
    dsk = {'x': 1}
    assert executor.get(dsk, 'x') == 1


def test_exceptions_rise_to_top(executor):
    def bad(x):
        raise ValueError()

    dsk = {'x': 1, 'y': (bad, 'x')}
    with pytest.raises(ValueError):
        executor.get(dsk, 'y')
