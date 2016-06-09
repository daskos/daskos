from __future__ import absolute_import, division, print_function

import cloudpickle
from functools import partial
from dask.callbacks import Callback
from kazoo.client import NoNodeError
from kazoo.recipe.lock import Lock as ZkLock


class ZookeeperBase(Callback):

    def __init__(self, zk, name, ns='/epos', ephemeral=False):
        self.zk = zk
        self.ephemeral = ephemeral

        template = '/{ns}/{name}/{task}'.replace('//', '/')
        self.path = partial(template.format, ns=ns, name=name)

    def _load(self, task):
        return cloudpickle.loads(self.zk.get(self.path(task=task))[0])

    def _save(self, task, value):
        return self.zk.create(self.path(task=task), cloudpickle.dumps(value),
                              makepath=True)

    def _finish(self, dsk, state, failed):
        pass

    def __exit__(self, *args):
        if self.ephemeral:
            self.zk.delete(self.path(task='').rstrip('/'), recursive=True)
        super(ZookeeperBase, self).__exit__(*args)


class Persist(ZookeeperBase):

    def _start(self, dsk):
        persisted = {}
        for k, v in dsk.items():
            try:
                persisted[k] = self._load(k)
            except NoNodeError:
                pass

        overlap = set(dsk) & set(persisted)
        for key in overlap:
            dsk[key] = persisted[key]

    def _posttask(self, key, value, dsk, state, id):
        self._save(key, value)


class Lock(ZookeeperBase):

    def __init__(self, zk, name, ns='/epos', ephemeral=False,
                 blocking=True, timeout=None):
        super(Lock, self).__init__(
            zk=zk, name=name, ns=ns, ephemeral=ephemeral)
        self.blocking = blocking
        self.timeout = timeout
        self.locks = {}

    def _pretask(self, key, dsk, state):
        self.locks[key] = ZkLock(self.zk, self.path(task=key))
        self.locks[key].acquire(blocking=self.blocking,
                                timeout=self.timeout)

    def _posttask(self, key, value, dsk, state, id):
        self.locks[key].release()
        del self.locks[key]

    def __exit__(self, *args):
        for key, lock in self.locks.items():
            lock.release()
            del self.locks[key]
        super(Lock, self).__exit__(*args)
