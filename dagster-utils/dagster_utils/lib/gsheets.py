import os
from typing import Optional

import requests
from dagster import Config, List, OpExecutionContext, Out, get_dagster_logger, op

from dagster_utils.utils.dicts import safeget

from ._base_middleware import BaseGoogleAPI
from ._types import UtilsFileSystemOutputType

logger = get_dagster_logger()


# ###############################
# API LIB
# ###############################


class UtilsGSheetsClient(BaseGoogleAPI):
    auth_config: Optional[dict[str, str]] = {
        "name": "/access/gmail/dataextract",
        "type": "parameterStore",
    }

    def fetch_sheet_from_id(
        self,
        key,
        sheet_id,
        sheet_name=None,
    ) -> UtilsFileSystemOutputType:
        uri = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&"
        if sheet_name:
            uri += f"sheet={sheet_name}"

        r = requests.get(uri, headers=self._headers)

        return UtilsFileSystemOutputType(
            filename=f"{key}.csv",
            content=r.content,
        )

    def fetch(self, options) -> list[UtilsFileSystemOutputType]:
        res = []
        for key, sheet_id in safeget(options, "sheet_mapping").items():
            r = requests.get(
                f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&",
                headers=self._headers,
            )

            res.append(
                UtilsFileSystemOutputType(
                    filename=f"{key}.csv",
                    content=r.content,
                )
            )

        return res


# ###############################
# DAGSTER SPECIFIC
# ###############################


class FetchFromGSheetsConfig(Config):
    sheet_mapping: dict[str, str]
    multi_dimensional_partition_id: Optional[str] = None


@op(
    description=str(
        "Fetches data from a Google Sheets source. Source middlewares do only minimal transformation "
        "returns a list of `UtilsFileSystemOutputType` with the provided key in the `sheet_mapping` "
        "as the `filename` property (csv), and the spreadsheet contents as the `contents` property."
    ),
    out=Out(dagster_type=List[UtilsFileSystemOutputType]),
)
def fetch_from_gsheets(
    config: FetchFromGSheetsConfig,
    gsheets: UtilsGSheetsClient,
) -> List[UtilsFileSystemOutputType]:
    return gsheets.fetch(config.dict())


@op(
    description=str(
        "Fetches data from a Google Sheets source. Source middlewares do only minimal transformation "
        "returns a list of `UtilsFileSystemOutputType` with the provided key in the `sheet_mapping` "
        "as the `filename` property (csv), and the spreadsheet contents as the `contents` property."
    ),
    out=Out(dagster_type=UtilsFileSystemOutputType),
)
def fetch_from_gsheets_with_partition(
    context: OpExecutionContext,
    config: FetchFromGSheetsConfig,
    gsheets: UtilsGSheetsClient,
) -> UtilsFileSystemOutputType:
    if config.multi_dimensional_partition_id:
        mdimensional_id = config.multi_dimensional_partition_id
        sheet_key = context.partition_key.keys_by_dimension[mdimensional_id]
        sheet_id = config.sheet_mapping[sheet_key]

        return gsheets.fetch_sheet_from_id(sheet_key, sheet_id)

    elif context.has_partition_key and len(config.sheet_mapping) == 1:
        sheet_key = list(config.sheet_mapping.keys())[0]
        sheet_id = config.sheet_mapping[sheet_key]

        return gsheets.fetch_sheet_from_id(
            sheet_key,
            sheet_id,
            context.partition_key,
        )

    else:
        raise NotImplementedError(
            (
                "Function fetch_from_gsheets_with_partition should "
                "have either multi-dimensional partitions or a single "
                "item in the sheet_mapping config argument"
            )
        )


# ###############################
# STUB
# ###############################


class StubUtilsGSheetsClient(UtilsGSheetsClient):
    stubs_dir: str
    sheet: str = None
    auth_config: Optional[dict[str, str]] = None

    def get_headers(self) -> str:
        return {"Authorization": f"Bearer"}

    def fetch_sheet_from_id(self, key, sheet_id):
        if self.sheet is not None:
            with open(os.path.join(self.stubs_dir, f"{self.sheet}.csv"), "rb") as f:
                return UtilsFileSystemOutputType(
                    filename=f"{self.sheet}.csv",
                    content=f.read(),
                )
        else:
            return UtilsFileSystemOutputType(
                filename=f"{key}.csv",
                content=b"my;cool;csv\n1;2;3\n1;2;3",
            )

    def fetch(self, options):
        if self.sheet is not None:
            with open(os.path.join(self.stubs_dir, f"{self.sheet}.csv"), "rb") as f:
                return [
                    UtilsFileSystemOutputType(
                        filename=f"{self.sheet}.csv",
                        content=f.read(),
                    )
                ]
        else:
            res = []
            for key, sheet_id in safeget(options, "sheet_mapping").items():
                res.append(
                    UtilsFileSystemOutputType(
                        filename=f"{key}.csv",
                        content=b"my;cool;csv\n1;2;3\n1;2;3",
                    )
                )

            return res
