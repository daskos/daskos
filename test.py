from dask_mesos.mesos import get, get_satyr
from dask import set_options
from dask.imperative import do

@do
def inc(x):
    return x+1

@do
def add(x, y):
    return x+y

@do
def mul(x, y):
    return x*y


with set_options(get=get):
    x = inc(666)
    y = mul(666, 777)
    print add(y, x).compute()
