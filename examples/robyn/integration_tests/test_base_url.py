import os

import pytest
import requests

_TEST_PORT = os.environ.get("ROBYN_TEST_PORT", "8080")

from integration_tests.helpers.network_helpers import get_network_host


@pytest.mark.benchmark
def test_default_url_index_request(default_session):
    BASE_URL = f"http://127.0.0.1:{_TEST_PORT}"
    res = requests.get(f"{BASE_URL}")
    assert res.status_code == 200


@pytest.mark.benchmark
def test_local_index_request(session):
    BASE_URL = f"http://127.0.0.1:{_TEST_PORT}"
    res = requests.get(f"{BASE_URL}")
    assert os.getenv("ROBYN_HOST") == "127.0.0.1"
    assert res.status_code == 200


@pytest.mark.benchmark
def test_global_index_request(global_session):
    host = get_network_host()
    BASE_URL = f"http://{host}:{_TEST_PORT}"
    res = requests.get(f"{BASE_URL}")
    assert os.getenv("ROBYN_HOST") == f"{host}"
    assert res.status_code == 200
