import os
from contextlib import contextmanager

import pytest

from dagster_utils.utils import auth

THIS_DIR = os.path.dirname(__file__)


@contextmanager
def not_raises(exception):
    """Helper function to explicitly test functions don't raise errors

    Raises:
        pytest.fail: Raises exception and fails test
    """
    try:
        yield
    except exception:
        raise pytest.fail(f"Raised exception {exception}")


def test_fetch_authentication_type_not_supported():
    auth_dict = {"type": "foo", "name": "bar"}

    with pytest.raises(NameError):
        auth.fetch_authentication(auth_dict)


def test_fetch_local_auth_returns_dict():
    mock_filepath = os.path.join(THIS_DIR, "mocks", "mock_auth.json")
    fetched_auth = auth.fetch_authentication({"type": "local", "name": mock_filepath})

    assert type(fetched_auth) is dict


def test_fetch_parameter_store_auth_returns_value(monkeypatch):
    # TODO: change to moto3
    import boto3

    class MockSSMParameter:
        def __init__(self, *args, **kwargs):
            pass

        def get_parameter(self, *args, **kwargs):
            return {"Parameter": {"Value": '{"foo": "bar"}'}}

    monkeypatch.setattr(boto3, "client", MockSSMParameter)
    fetched_auth = auth.fetch_authentication(
        {"type": "parameterStore", "name": "/foo/bar/baz"}
    )

    assert fetched_auth == {"foo": "bar"}
