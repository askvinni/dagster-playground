import io
import pickle
from typing import Union

from dagster import (
    ConfigurableIOManager,
    InputContext,
    MetadataValue,
    OutputContext,
    get_dagster_logger,
)
from dagster_aws.s3.utils import construct_s3_client

from dagster_utils.lib import UtilsSinkInputType, UtilsSnowflakeClient

PICKLE_PROTOCOL = 5

logger = get_dagster_logger()


class UtilsS3IOManager(ConfigurableIOManager):
    class Config:
        # This is ugly and a workaround, but IO Managers don't seem to be able to manage state
        # Long-term solution would be to create an S3 Resource which can also be used as a lib
        # Out of scope for now, don't foresee this use case coming up that soon.
        extra = "allow"
        frozen = False

    bucket: str
    s3_prefix: str = None
    utils_snow: UtilsSnowflakeClient

    @property
    def s3(self):
        if not hasattr(self, "_s3"):
            self._s3 = construct_s3_client(max_attempts=5)
        return self._s3

    def _get_path(self, context: Union[InputContext, OutputContext]) -> str:
        if context.has_asset_key:
            path = context.get_asset_identifier()
        else:
            path = ["storage", *context.get_identifier()]

        if self.s3_prefix and self.s3_prefix != "":
            final_path = [self.s3_prefix, *path]
        else:
            final_path = path
        return "/".join(final_path)

    def _uri_for_key(self, key):
        return f"s3://{self.bucket}/{key}"

    def load_input(self, context: InputContext):
        if context.dagster_type.typing_type == type(None):
            return None

        key = self._get_path(context)
        context.log.debug(f"Loading S3 object from: {self._uri_for_key(key)}")
        obj = pickle.loads(
            self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        )

        return obj

    def handle_output(self, context: OutputContext, obj):
        key = self._get_path(context)
        path = self._uri_for_key(key)
        context.log.debug(f"Writing S3 object at: {path}")

        pickled_obj = pickle.dumps(obj, PICKLE_PROTOCOL)
        pickled_obj_bytes = io.BytesIO(pickled_obj)
        self.s3.upload_fileobj(pickled_obj_bytes, self.bucket, key)
        yield {"uri": MetadataValue.path(path)}

        if isinstance(obj, UtilsSinkInputType):
            context.log.debug(f"Attempting snowflake upload")
            if obj.load_to_snow:
                context.log.debug(f"Object should be uploaded to snowflake")
                parquet_path = self._upload_df(obj, key)

                yield {
                    "S3 parquet storage path": MetadataValue.path(
                        f"s3://{self.bucket}/{parquet_path}"
                    )
                }
                yield {"Rows": MetadataValue.int(len(obj.data.index))}

                yield from self.utils_snow.copy_into_landing_area(
                    context,
                    parquet_path,
                )
                yield {"Loaded to snowflake": MetadataValue.bool(True)}

        else:
            yield {"Loaded to snowflake": MetadataValue.bool(False)}

    def _upload_df(self, obj, filekey):
        remote_filepath = f"{filekey}.parquet"

        out_buffer = io.BytesIO()
        obj.data.to_parquet(out_buffer, index=False)

        self.s3.put_object(
            Bucket=self.bucket,
            Key=remote_filepath,
            Body=out_buffer.getvalue(),
        )

        logger.info(f"File uploaded to S3 with path {remote_filepath}")

        return remote_filepath
