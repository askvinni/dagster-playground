from typing import Optional

import pandas as pd
from dagster import usable_as_dagster_type
from pydantic import BaseModel


@usable_as_dagster_type
class UtilsFileSystemOutputType(BaseModel):
    """Represents a file in the file system. Includes the properties `filename`,
    `extension`, and `content`, representing the bytes-encoded content."""

    filename: str
    content: bytes
    meta: Optional[dict]

    @property
    def extension(self):
        return self.filename.split(".")[-1].lower()


@usable_as_dagster_type
class UtilsWebAPIOutputType(BaseModel):
    """Represents the type that should be outputted by a Web API."""

    data: list[dict]
    meta: Optional[dict]


@usable_as_dagster_type
class UtilsSinkInputType(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    """Represents the type that should be used to load data into a utils-defined sink.
    Setting `load_to_snow` to True will trigger the Snowflake IO Manager and save the data
    into a Snowflake table"""

    dest_asset: str
    load_to_snow: bool = False
    data: pd.DataFrame
    meta: Optional[dict]
