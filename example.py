from __future__ import absolute_import, division, print_function

from dask import set_options
from dask.imperative import do
from dask_mesos.imperative import mesos
from dask_mesos.mesos import get


@mesos(mem=512, cpus=1)
def inc(x):
    """Run stuff on mesos w/o docker but w/ resources set."""
    return x + 1


@do
def add(x, y):
    """Run stuff on mesos w/o docker and w/ default resource setup."""
    return x + y


@mesos(mem=512, cpus=1, image='bdas-master-3:5000/satyr')
def mul(x, y):
    """Run stuff on mesos w/ docker and specified resources."""
    return x * y


with set_options(get=get):
    """This context ensures that both @do and @mesos will run on mesos."""
    one = inc(0)
    alot = add(one, 789)
    gigalot = mul(alot, inc(alot))
    print(mul(alot, gigalot).compute())
