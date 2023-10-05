from dagster import (
    ConfigurableResource,
    MetadataValue,
    Optional,
    OutputContext,
    get_dagster_logger,
)
from pydantic import PrivateAttr
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

logger = get_dagster_logger()


class UtilsSnowflakeClient(ConfigurableResource):
    stage: str = "ETLHUB_LOADS"
    account: str
    user: str
    password: str
    database: str
    warehouse: str

    _conn = PrivateAttr()

    def setup_for_execution(self, _):
        url = URL(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            warehouse=self.warehouse,
            timezone="UTC",
        )
        self._conn = create_engine(url).connect()

    def copy_into_landing_area(self, context: OutputContext, remote_filepath):
        asset_key_path = context.asset_key.path
        schema = asset_key_path[-2] if len(asset_key_path) > 1 else "src_landing"
        table = asset_key_path[-1]
        if context.has_asset_partitions:
            partition_key = context.asset_partition_key
            print(partition_key)
        else:
            partition_key = None
        self._conn.execute(
            self._get_landing_cleanup_statement(table, schema, partition_key)
        )
        self._conn.execute(
            self._get_copy_into_statement(remote_filepath, table, schema, partition_key)
        )

        yield {
            "Query": MetadataValue.text(self._get_select_statement(table, schema, None))
        }

    def _get_copy_into_statement(
        self,
        remote_filepath: str,
        table: str,
        schema: str,
        partitions: None,
    ):
        if "*" in remote_filepath:
            files = f"PATTERN = '{remote_filepath}'"
        else:
            files = f"FILES =('{remote_filepath}')"

        if partitions is not None:
            return (
                f"COPY INTO {schema}.{table}(DATA, PARTITION)\n"
                f"FROM(SELECT $1, '{partitions}' FROM @{schema}.{self.stage})\n"
                f"{files}\n"
                f"FILE_FORMAT = (type = '{remote_filepath.split('.')[-1]}')"
                "FORCE=TRUE;"
            )
        else:
            return (
                f"COPY INTO {schema}.{table}(DATA) FROM @{schema}.{self.stage}\n"
                f"{files}\n"
                f"FILE_FORMAT = (type = '{remote_filepath.split('.')[-1]}');"
            )

    def _get_landing_cleanup_statement(
        self, table: str, schema: str, partitions=None
    ) -> str:
        """
        Returns a SQL statement that deletes data in the given table to make way for the output data
        being written.
        """
        return f"DELETE FROM {schema}.{table} {self._source_load_at_delete_clause(partitions)}"

    def _source_load_at_delete_clause(self, partitions=None) -> str:
        if partitions is not None:
            return f"WHERE partition = '{partitions}'"
        else:
            return f"WHERE source_load_at < DATEADD(days, -5, CURRENT_TIMESTAMP())"

    def _get_select_statement(
        self,
        table: str,
        schema: str,
        columns: Optional[list[str]] = None,
    ):
        col_str = ", ".join(columns) if columns else "*"
        return f"""SELECT {col_str} FROM {schema}.{table}"""


# ###############################
# STUB
# ###############################


class MockSFConn:
    def execute(self, *args, **kwargs):
        return self.MockSFRes()

    class MockSFRes:
        def __init__(self):
            pass

        def fetchone(self, *args, **kwargs):
            return [1]


class StubSnowflakeClient(UtilsSnowflakeClient):
    stage: str = None
    account: str = None
    user: str = None
    password: str = None
    database: str = None
    warehouse: str = None

    # Tests don't seem to correctly call the setup_for_execution function
    # so we need to default to the mock without overriding the function.
    # This might break in the future
    _conn = PrivateAttr(MockSFConn())

    def setup_for_execution(self, _):
        self._conn = MockSFConn()
