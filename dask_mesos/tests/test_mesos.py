import pytest
from satyr.config import Config


def test_get_satyr(monkeypatch):
    def mock_create_satyr(config):
        assert isinstance(config, Config)
        return 'SATYR'

    monkeypatch.setattr('satyr.multiprocess.create_satyr', mock_create_satyr)
    from dask_mesos.mesos import get_satyr
    assert get_satyr() == 'SATYR'


test_mesos_settings = [
    ({'cpus': 0, 'mem': 1, 'disk': 2}, {'resources': {
     'cpus': 0, 'mem': 1, 'disk': 2}, 'image': None}),
    ({'cpu': -1, 'cpus': 0, 'mem': 1},
     {'resources': {'cpus': 0, 'mem': 1}, 'image': None}),
    ({'cpus': 0, 'mem': 1, 'image': 'xxx:1.1'}, {
     'resources': {'cpus': 0, 'mem': 1}, 'image': 'xxx:1.1'}),
    ({'image': 'xxx:1.1'}, {'resources': {}, 'image': 'xxx:1.1'})
]


@pytest.mark.parametrize('input,result', test_mesos_settings)
def test_create_satyr_compatible_config_no_image(input, result):
    from dask_mesos.mesos import create_satyr_compatible_config
    assert create_satyr_compatible_config(input) == result
