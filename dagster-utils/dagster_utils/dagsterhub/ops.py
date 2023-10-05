import io

import pandas as pd
from dagster import (
    Config,
    DynamicPartitionsDefinition,
    Field,
    OpExecutionContext,
    Out,
    job,
    op,
)

from dagster_utils.lib import (
    UtilsFileSystemOutputType,
    UtilsSinkInputType,
    UtilsWebAPIOutputType,
)


class WebAPIOutputToSinkInputConfig(Config):
    dest_asset: str


@op(out=Out(UtilsSinkInputType, io_manager_key="utils_s3_io_manager"))
def webapioutput_to_sinkinput(
    config: WebAPIOutputToSinkInputConfig,
    obj: UtilsWebAPIOutputType,
) -> UtilsSinkInputType:
    return UtilsSinkInputType(
        load_to_snow=True,
        dest_asset=config.dest_asset,
        data=pd.DataFrame(obj.data),
    )


class CSVToSinkInputConfig(Config):
    dest_asset: str


@op(out=Out(UtilsSinkInputType, io_manager_key="utils_s3_io_manager"))
def csv_to_utilssinkinput(
    config: WebAPIOutputToSinkInputConfig,
    csv_obj: UtilsFileSystemOutputType,
) -> UtilsSinkInputType:
    source_data = csv_obj.content.decode("utf-8")
    string_toread = io.StringIO()
    string_toread.write(source_data)
    # Read from start of file
    string_toread.seek(0)
    df = pd.read_csv(string_toread)

    return UtilsSinkInputType(
        dest_asset=config.dest_asset,
        load_to_snow=True,
        data=df,
    )


@op(
    config_schema={
        "dynamic_partition_name": str,
        "partitions_to_add": Field(list, is_required=False),
        "partition_to_delete": Field(str, is_required=False),
    },
    description=(
        "Utility to be used when necessary to delete or add dynamic partitions."
    ),
)
def add_or_delete_dynamic_partitions(context: OpExecutionContext):
    partitions = DynamicPartitionsDefinition(
        name=context.op_config["dynamic_partition_name"]
    )

    if context.op_config.get("partitions_to_add") is not None:
        context.instance.add_dynamic_partitions(
            partitions.name,
            context.op_config.get("partitions_to_add"),
        )

    if context.op_config.get("partition_to_delete") is not None:
        for partition in context.op_config.get("partition_to_delete"):
            context.instance.delete_dynamic_partition(
                partitions.name,
                partition,
            )


@job
def add_or_delete_dynamic_partitions_job():
    add_or_delete_dynamic_partitions()
