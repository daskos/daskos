[![Build Status](http://52.0.47.203:8000/api/badges/lensacom/dask.mesos/status.svg)](http://52.0.47.203:8000/lensacom/dask.mesos)

Run dask workflows on your Mesos cluster.

# Installation

**Prerequisits:** [satyr](https://github.com/lensacom/satyr), [dask](https://github.com/dask/dask.git), [toolz](https://pypi.python.org/pypi/toolz). All of them should be installed w/ the following commands:

```bash
git clone git@github.com:lensacom/dask.mesos.git
cd dask.mesos
pip install .
```

# Example

```python
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
```

Or check [example.py](example.py) for a docker only example.

# Configuring dask.mesos tasks

You can configure your mesos tasks in your decorator, currently the following options are available:

* **mem**: Memory in MB to reserver for the task.
* **cpus**: The amount of cpus to use for the task.
* **image**: A docker image name. If not set, mesos containerizer will be used.

Both mem and cpus are defaults to some low values set in _satyr_ so you're encouraged to override them here. More options like constraints, other resources are on the way.

# Configuring satyr

Satyr runs the mesos framework behind dask.mesos so you may find yourself in situations where you have to pass options to it (can't really think of any occasion you wouldn't wanna do that) which you can do with environment variables.

`SATYR_ID`: id of the mesos framework (defaults to `satyr`).

`SATYR_NAME`: name of the mesos framework (defaults to `Satyr`).

`SATYR_USER`: user of the mesos framework (defaults to `root`).

`SATYR_MASTER`: mesos master url (defaults to `192.168.1.127:5050`). This is the only option that you really need to set to get things work.

`SATYR_MAX_TASKS`: number of tasks to run simultaneously (defaults to `10`, overridden in dask.mesos).

`SATYR_COMMAND`: the command to run on the executor (defaults to `python -m satyr.executors.pickled`). Satyr starts a new executor for each task to make it possible to use different docker images for different tasks. This command is used to start the mesos executor for both dockerized and normal runs. Change it only if you know what you're doing.

`SATYR_FILTER_REFUSE_SECONDS`: how much time should mesos wait after a refused resource offer to try again (defaults to `1`, overriden in dask.mesos).

`SATYR_PERMANENT`: should satyr run even if there are no tasks left in it's queue (defaults to true, overriden in dask.mesos).

# Available containers

For most use cases you'll have to build your own images but for some simple tasks or testing we have some pre-built images for you. For the mesos decorator you can use the `bdas-master-3:5000/satyr` image and for starting the framework from w/ dask.mesos use a command like this: `docker run --net=host --rm -e SATYR_MASTER=bdas-master-2:5050 bdas-master-3:5000/dask.mesos python /dask.mesos/example.py`.
