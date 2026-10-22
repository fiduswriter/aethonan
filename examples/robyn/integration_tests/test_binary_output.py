import pytest
import os

from integration_tests.helpers.http_methods_helpers import get

BASE_URL = f"http://127.0.0.1:{os.environ.get('ROBYN_TEST_PORT', '8080')}"


@pytest.mark.benchmark
@pytest.mark.parametrize(
    "route, text",
    [
        ("/sync/octet", "sync octet"),
        ("/async/octet", "async octet"),
        ("/sync/octet/response", "sync octet response"),
        ("/async/octet/response", "async octet response"),
    ],
)
def test_binary_output(route: str, text: str, session):
    r = get(route)
    assert r.headers.get("Content-Type") == "application/octet-stream"
    assert r.text == text
