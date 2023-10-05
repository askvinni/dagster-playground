import pandas as pd
from dagster import AssetKey, DagsterType, build_input_context, build_output_context

from dagster_utils.dagsterhub import UtilsS3IOManager
from dagster_utils.lib import StubSnowflakeClient, UtilsSinkInputType


def test_utils_s3_io_manager(mock_s3_bucket, mock_s3_resource, aws_creds):
    manager = UtilsS3IOManager(
        bucket="test-bucket",
        utils_snow=StubSnowflakeClient(),
    )

    out_context = build_output_context(name="abc", step_key="123")
    in_context = build_input_context(
        upstream_output=out_context,
        dagster_type=DagsterType(
            type_check_fn=lambda _, x: True,
            name="mock_io_dagster_type_test",
        ),
    )
    [i for i in manager.handle_output(out_context, "my_string")]
    assert manager.load_input(in_context) == "my_string"


def test_utils_s3_io_manager_load_to_snow(mock_s3_bucket, mock_s3_resource, aws_creds):
    # TODO: add checks for metadata entries
    manager = UtilsS3IOManager(
        bucket="test-bucket",
        utils_snow=StubSnowflakeClient(),
    )

    out = UtilsSinkInputType(
        load_to_snow=True,
        dest_asset="my_cool_asset",
        data=pd.DataFrame({"foo": "bar", "baz": "qux"}, index=[0]),
    )
    out_context = build_output_context(
        asset_key=out.dest_asset,
        step_key="some_key",
        name="some_name",
    )
    in_context = build_input_context(
        upstream_output=out_context,
        asset_key=AssetKey(out.dest_asset),
        dagster_type=DagsterType(
            type_check_fn=lambda _, x: True,
            name="mock_io_dagster_type_test",
        ),
    )
    [i for i in manager.handle_output(out_context, out)]
    assert manager.load_input(in_context).dest_asset == out.dest_asset
    assert manager.load_input(in_context).data.equals(out.data)


def test_utils_s3_io_manager_load_to_snow_partitioned(
    mock_s3_bucket, mock_s3_resource, aws_creds
):
    # TODO: add checks for metadata entries
    manager = UtilsS3IOManager(
        bucket="test-bucket",
        utils_snow=StubSnowflakeClient(),
    )

    out = UtilsSinkInputType(
        load_to_snow=True,
        dest_asset="my_cool_asset",
        data=pd.DataFrame({"foo": "bar", "baz": "qux"}, index=[0]),
    )
    out_context = build_output_context(
        asset_key=out.dest_asset,
        step_key="some_key",
        name="some_name",
        partition_key="some_partition",
    )
    in_context = build_input_context(
        upstream_output=out_context,
        asset_key=AssetKey(out.dest_asset),
        dagster_type=DagsterType(
            type_check_fn=lambda _, x: True,
            name="mock_io_dagster_type_test",
        ),
    )
    [i for i in manager.handle_output(out_context, out)]
    assert manager.load_input(in_context).dest_asset == out.dest_asset
    assert manager.load_input(in_context).data.equals(out.data)
