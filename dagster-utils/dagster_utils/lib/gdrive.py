from typing import Optional

import requests
from dagster import ConfigurableResource, Field, List, Out, get_dagster_logger, op

from ._base_middleware import BaseGoogleAPI
from ._types import UtilsFileSystemOutputType

logger = get_dagster_logger()


# ###############################
# DAGSTER SPECIFIC
# ###############################


# ###############################
# API LIB
# ###############################


class UtilsGMailClient(BaseGoogleAPI, ConfigurableResource):
    class Config:
        extra = "allow"
        frozen = False

    auth_config: Optional[dict[str, str]] = {
        "name": "/access/gmail/dataextract",
        "type": "parameterStore",
    }
