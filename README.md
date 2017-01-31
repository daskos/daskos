[![Build Status](http://52.0.47.203:8000/api/badges/lensacom/dask.mesos/status.svg)](http://52.0.47.203:8000/lensacom/dask.mesos)
[![Join the chat at https://gitter.im/lensacom/dask.mesos](https://badges.gitter.im/lensacom/dask.mesos.svg)](https://gitter.im/lensacom/dask.mesos?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[Apache Mesos](http://mesos.apache.org/) backend for [Dask](https://github.com/dask/dask) scheduling library.
Run distributed python dask workflows on your Mesos cluster.

## Notable Features

 - distributively run tasks in docker container
 - specify resource requirements per task
 - bin packing for optimized resource utilization

## Installation

**Prerequisits:** [satyr](https://github.com/lensacom/satyr), [dask](https://github.com/dask/dask.git), [toolz](https://pypi.python.org/pypi/toolz). All of them should be installed w/ the following commands:

`pip install dask.mesos` or use [lensa/dask.mesos](https://hub.docker.com/r/lensa/dask.mesos/) Docker image

Configuration:
- `MESOS_MASTER=zk://127.0.0.1:2181/mesos`
- `ZOOKEEPER_HOST=127.0.0.1:2181`


## Example

```python
from __future__ import absolute_import, division, print_function

from daskos import mesos, MesosExecutor


@mesos(cpus=0.1, mem=64)
def add(x, y):
    """Run add on mesos with specified resources"""
    return x + y


@mesos(cpus=0.3, mem=128, image='lensa/daskos')
def mul(x, y):
    """Run mul on mesos in specified docker image"""
    return x * y


with MesosExecutor(name='dask') as executor:
    """This context handles Mesos scheduler's lifecycle"""
    a, b = 23, 89
    alot = add(a, b)
    gigalot = mul(alot, add(10, 2))

    gigalot.compute(get=executor.get)  # (a + b) * (10 + 2)
    executor.compute([alot, gigalot])  # list of futures
```


## Configuring daskos Tasks

You can configure your mesos tasks in your decorator, currently the following options are available:

* **cpus**: The amount of cpus to use for the task.
* **mem**: Memory in MB to reserver for the task.
* **disk**: The amount of disk to use for the task.
* **image**: A docker image name. If not set, mesos containerizer will be used.

Both mem and cpus are defaults to some low values set in _satyr_ so you're encouraged to override them here. More options like constraints, other resources are on the way.
