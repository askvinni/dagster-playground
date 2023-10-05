import urllib
from abc import ABC, abstractmethod

import requests
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import PrivateAttr

import dagster_utils.utils as utils

logger = get_dagster_logger()


class BaseMiddleware(ABC):
    _auth = PrivateAttr(None)

    def get_auth(self):
        if self.auth_config is not None:
            if self._auth == PrivateAttr(None) or None:
                self._auth = utils.fetch_authentication(self.auth_config)
            return self._auth
        else:
            return None

    @property
    def auth(self):
        if self.auth_config is not None:
            if self._auth == PrivateAttr(None) or None:
                self._auth = utils.fetch_authentication(self.auth_config)
            return self._auth
        else:
            return None


class BaseGoogleAPI(ConfigurableResource, BaseMiddleware):
    _headers = PrivateAttr(None)
    _auth = PrivateAttr(None)

    def setup_for_execution(self, _) -> None:
        self._auth = self.get_auth()
        self._headers = self.get_headers()

    def get_headers(self):
        logger.warn(self.auth)
        r = requests.post(
            f"https://oauth2.googleapis.com/token?{urllib.parse.urlencode(self._auth)}"
        )
        logger.info("Generated Google access token")
        return {"Authorization": f"Bearer {r.json()['access_token']}"}
