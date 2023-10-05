import pytest

from dagster_utils import utils


@pytest.fixture
def mock_fetch_auth(monkeypatch):
    def mock_fetch_authentication(*args, **kwargs):
        return {"access_token": ""}

    monkeypatch.setattr(utils, "fetch_authentication", mock_fetch_authentication)
