import json
import os

import boto3
from dagster import get_dagster_logger

logger = get_dagster_logger()


def fetch_authentication(auth) -> dict:
    """Fetches authentication dynamically calling a different function depending on type

    Args:
        auth (AuthConfig): specifies the type and name of file where authentication is saved

    Raises:
        BaseException: Raises exception if auth located somewhere not supported
    """
    if auth["type"] == "local":
        fetched_auth = _fetch_local_auth(auth["name"])
    elif auth["type"] == "parameterStore":
        fetched_auth = _fetch_parameter_store_auth(auth["name"])
    else:
        raise NameError("Authentication type not supported")

    return fetched_auth


def _fetch_local_auth(name: str) -> dict:
    with open(name) as f:
        return json.load(f)


def _fetch_parameter_store_auth(name: str) -> dict:
    ssm = boto3.client("ssm")
    parameter = ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
    return json.loads(parameter)
