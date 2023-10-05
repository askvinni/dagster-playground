from dagster import List, check_dagster_type

from dagster_utils.lib import *


def test_UtilsFileSystemOutputType():
    assert check_dagster_type(
        UtilsFileSystemOutputType,
        UtilsFileSystemOutputType(filename="my_cool_file.txt", content=b"cool_content"),
    ).success

    assert check_dagster_type(
        List[UtilsFileSystemOutputType],
        [
            UtilsFileSystemOutputType(
                filename="my_cool_file.txt", content=b"cool_content"
            ),
            UtilsFileSystemOutputType(
                filename="my_other_cool_file.txt", content=b"more_cool_content"
            ),
        ],
    ).success


def test_UtilsWebAPIOutputType():
    assert check_dagster_type(
        UtilsWebAPIOutputType,
        UtilsWebAPIOutputType(data=[{"foo": "bar"}]),
    ).success

    assert check_dagster_type(
        List[UtilsWebAPIOutputType],
        [
            UtilsWebAPIOutputType(data=[{"foo": "bar"}]),
            UtilsWebAPIOutputType(data=[{"foo": "bar"}], meta={"some": "dict"}),
        ],
    ).success


def test_UtilsSinkInputType():
    df = pd.DataFrame([{"foo": "bar"}])

    assert check_dagster_type(
        UtilsSinkInputType,
        UtilsSinkInputType(dest_asset="my_cool_asset", data=df),
    ).success

    assert check_dagster_type(
        List[UtilsSinkInputType],
        [
            UtilsSinkInputType(dest_asset="my_other_cool_asset", data=df),
            UtilsSinkInputType(dest_asset="my_other_cool_asset", data=df),
        ],
    ).success
