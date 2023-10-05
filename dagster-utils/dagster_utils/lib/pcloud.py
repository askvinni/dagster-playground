import os
from contextlib import ContextDecorator
from datetime import datetime
from typing import Optional

import requests
from dagster import (
    Config,
    ConfigurableResource,
    DynamicPartitionsDefinition,
    JobDefinition,
    List,
    OpExecutionContext,
    Out,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    get_dagster_logger,
    op,
    schedule,
)
from pydantic import PrivateAttr

from dagster_utils.utils import check

from ._base_middleware import BaseMiddleware
from ._types import UtilsFileSystemOutputType

logger = get_dagster_logger()

PCLOUD_BASE_URL = "https://eapi.pcloud.com"
PCLOUD_DATE_FORMAT = "%a, %d %b %Y %X %z"

# ###############################
# API LIB
# ###############################


class UtilspCloudClient(
    ConfigurableResource,
    BaseMiddleware,
    ContextDecorator,
):
    class Config:
        extra = "allow"
        frozen = False

    base_url: str = PCLOUD_BASE_URL
    auth_config: Optional[dict[str, str]] = {
        "name": "/access/pcloud/root_folder_api_key",
        "type": "parameterStore",
    }

    @property
    def headers(self):
        if not hasattr(self, "_headers"):
            self._headers = {"Authorization": f"Bearer {self.auth.get('token')}"}
        return self._headers

    @property
    def session(self):
        if not hasattr(self, "_session"):
            self._session = requests.Session()
        return self._session

    @property
    def now(self):
        if not hasattr(self, "_now"):
            self._now = datetime.now()
        return self._now

    def __enter__(self):
        self.session
        return self

    def __exit__(self, *args, **kwargs):
        self.close_session()

    def close_session(self):
        self.session.close()

    def list_files_in_folder_id(self, folder_id, params={}) -> dict:
        folder_res = self._make_session_request(
            endpoint="listfolder",
            params={"folderid": str(folder_id)} | params,
        )

        return folder_res.json()["metadata"]

    def read_files_by_id(self, file_ids: list[str]) -> list:
        file_ids = check.list_param(file_ids, "file_ids", str)

        # TODO: I wanna make this yield the file, otherwise it can get quite heavy, but this would need refactoring on the fermentation processor
        return [self._read_file_by_id(file_id) for file_id in file_ids]

    def fetch_folder_to_process(self, root_folder_id, folder_name) -> dict:
        """Assumption: the folder to be processed will be two levels down from the root folder.
        Its parent (subfolder from root folder) will be named according to the current year.
        """

        pcloud_root_res = self.list_files_in_folder_id(
            root_folder_id, {"recursive": True, "nofiles": True}
        )["contents"]

        current_year_folder = [
            item["contents"]
            for item in pcloud_root_res
            if item["name"] == str(self.now.year)
        ][0]

        folder_to_process = [
            item for item in current_year_folder if item["name"] == folder_name
        ][0]

        return folder_to_process

    def _read_file_by_id(self, file_id: str):
        file_info = self._make_session_request(
            endpoint="stat",
            params={"fileid": file_id},
        ).json()["metadata"]

        fd = self._make_session_request(
            endpoint="file_open",
            params={"flags": "0x0400", "fileid": file_id},
        ).json()["fd"]

        file_contents = self._make_session_request(
            endpoint="file_read",
            params={
                "fd": str(fd),
                "count": str(file_info["size"]),
            },
        )

        # close file
        self._make_session_request(
            endpoint="file_close",
            params={"fd": fd},
        )

        return UtilsFileSystemOutputType(
            filename=file_info["name"],
            content=file_contents.content,
            meta={
                "file_size": file_info["size"],
                "content_type": file_info["contenttype"],
                "created_at": datetime.strptime(
                    file_info["created"],
                    PCLOUD_DATE_FORMAT,
                ),
                "last_modified": datetime.strptime(
                    file_info["modified"],
                    PCLOUD_DATE_FORMAT,
                ),
            },
        )

    def _make_session_request(
        self,
        endpoint: str,
        params: dict,
        headers: dict = None,
    ) -> requests.Response:
        req_headers = self.headers if headers is None else headers

        r = self.session.request(
            method="GET",
            url=f"{self.base_url}/{endpoint}",
            headers=req_headers,
            params=params,
        )
        return r


# ###############################
# DAGSTER SPECIFIC
# ###############################


class ReadpCloudFilesByIdConfig(Config):
    file_ids: list[str]


@op(
    description=str(
        "Fetches data from a pCloud source, returns a list of type "
        "`UtilsFileSystemOutputType` with the properties `filename` and `content`."
    ),
    out=Out(List[UtilsFileSystemOutputType]),
)
def read_pcloud_files_by_id(
    config: ReadpCloudFilesByIdConfig,
    pcloud: UtilspCloudClient,
) -> list:
    return pcloud.read_files_by_id(config.file_ids)


class FetchpCloudRootFolderConfig(Config):
    pcloud_root_folder: str
    partition_dim: Optional[str]


@op
def fetch_pcloud_root_folder(
    context: OpExecutionContext,
    config: FetchpCloudRootFolderConfig,
    pcloud: UtilspCloudClient,
) -> dict:
    if hasattr(context.partition_key, "keys_by_dimension"):
        folder_name = context.partition_key.keys_by_dimension[config.partition_dim]
    else:
        folder_name = context.partition_key
    return pcloud.fetch_folder_to_process(config.pcloud_root_folder, folder_name)


# SCHEDULING


def make_pcloud_file_schedule(
    job: JobDefinition,
    cron_schedule: str,
    root_folder_id: str,
    mins_diff: int = 24 * 60,  # Default 1 day
    schedule_name: Optional[str] = None,
    subfolder_to_process: Optional[str] = None,
    run_config: Optional[dict] = None,
    dynamic_partitions_def: Optional[DynamicPartitionsDefinition] = None,
    multi_partition_spec: Optional[str] = None,
) -> ScheduleDefinition:
    """Creates a file schedule that will periodically poll pcloud for modified files.
    Based on a few assumptions:
    - Root folder will include one folder for each year
    - Each year folder will be checked and returned for processing if recently modified
    - If no subfolder_to_process is passed, the schedule will look for recently modified
        files within the first level under the year folder.


    Args:
        job (JobDefinition): Job definition that will be triggered by the schedule
        cron_schedule (str): Crontab notation for the schedule
        root_folder_id (str): Root folder to start processing, should contain the year folders
        mins_diff (int, optional): Schedule will only trigger jobs for files modified within the
            mins_diff time window. Defaults to a day.
        subfolder_to_process (Optional[str], optional): If the files to be monitored aren't directly
            under a year folder, pass the subfolder name. Defaults to None.
        run_config (Optional[dict], optional): Config to launch the dagster run. Defaults to None.
        dynamic_partitions_def (Optional[DynamicPartitionsDefinition], optional): If present, the
            schedule will add a dynamic partition before kicking off the run for that partition.
            Defaults to None.
    """
    now = datetime.now()

    def _get_mins_diff(date_str, pcloud_now=None):
        if pcloud_now is not None:
            now = pcloud_now
        modified = datetime.strptime(date_str, PCLOUD_DATE_FORMAT).replace(tzinfo=None)
        return (now - modified).total_seconds() / 60

    @schedule(
        name=schedule_name if schedule_name else f"{job.name}_schedule",
        job=job,
        cron_schedule=cron_schedule,
    )
    def pcloud_file_schedule(
        context: ScheduleEvaluationContext,
        pcloud: UtilspCloudClient,
    ):
        folder_contents = pcloud.list_files_in_folder_id(
            folder_id=root_folder_id,
            params={"recursive": True, "nofiles": True},
        )["contents"]

        current_year_folder = [
            item["contents"]
            for item in folder_contents
            if item["name"] == str(now.year)
        ][0]

        folders_to_process = [
            item
            for item in current_year_folder
            if "WIP" not in item["name"].upper()
            and _get_mins_diff(item["modified"], pcloud.now) < mins_diff
        ]

        if subfolder_to_process:
            folder_names = [
                folder["name"]
                for folder in folders_to_process
                for subfolder in folder["contents"]
                if subfolder["name"] == subfolder_to_process
                and _get_mins_diff(subfolder["modified"], pcloud.now) < mins_diff
            ]
        else:
            folder_names = []

            for folder in folders_to_process:
                process_contents = pcloud.list_files_in_folder_id(
                    folder_id=folder["folderid"],
                )["contents"]
                if any(
                    [
                        item["name"]
                        for item in process_contents
                        if not item["isfolder"]
                        and _get_mins_diff(item["modified"], pcloud.now) < mins_diff
                    ]
                ):
                    folder_names.append(folder["name"])

        if dynamic_partitions_def is not None:
            context.instance.add_dynamic_partitions(
                dynamic_partitions_def.name,
                folder_names,
            )

            run_reqs = [
                RunRequest(
                    partition_key=f"{folder}|{multi_partition_spec}"
                    if multi_partition_spec
                    else folder,
                    run_key=folder,
                    run_config=run_config,
                )
                for folder in folder_names
            ]
        else:
            run_reqs = [
                RunRequest(run_key=folder, run_config=run_config)
                for folder in folder_names
            ]

        pcloud.close_session()
        return run_reqs

    return pcloud_file_schedule


# ###############################
# STUB
# ###############################


class StubUtilspCloudClient(UtilspCloudClient):
    stubs_dir: str
    base_url: str = PCLOUD_BASE_URL
    datetime_str: Optional[str] = "2022-02-01"
    auth_config: Optional[dict[str, str]] = None

    @property
    def now(self):
        if self.datetime:
            return self.datetime
        else:
            return datetime(2022, 1, 31, 23, 59, 59).replace(tzinfo=None)

    @property
    def datetime(self):
        if hasattr(self, "datetime_str"):
            return datetime.strptime(self.datetime_str, "%Y-%m-%d")

    def _make_session_request(
        self,
        endpoint: str,
        params: dict,
        headers: dict = None,
    ):
        from dagster_utils.lib._mock_requests_response import MockRequestResponse

        if endpoint == "file_close":
            return MockRequestResponse(b"{}")

        mock_filepath = ""
        for k, v in params.items():
            mock_filepath += f"{k}={v}&"

        with open(
            os.path.join(self.stubs_dir, endpoint, mock_filepath[:-1]), "rb"
        ) as f:
            content = f.read()

        return MockRequestResponse(content)
