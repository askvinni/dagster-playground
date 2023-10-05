import os

from dagster import build_op_context

from dagster_utils.lib import (
    StubUtilsGSheetsClient,
    UtilsFileSystemOutputType,
    UtilsGSheetsClient,
    fetch_from_gsheets,
)

THIS_DIR = os.path.dirname(__file__)


# Dagster tests
def test_gsheets_resource_init(mock_fetch_auth):
    resource = UtilsGSheetsClient()

    assert type(resource) is UtilsGSheetsClient


def test_fetch_from_gsheets():
    context = build_op_context(
        config={"sheet_mapping": {"some_key": "some_id"}},
        resources={"gsheets": StubUtilsGSheetsClient(stubs_dir=".")},
    )

    assert fetch_from_gsheets(context) == [
        UtilsFileSystemOutputType(
            filename="some_key.csv",
            content=b"my;cool;csv\n1;2;3\n1;2;3",
        )
    ]
