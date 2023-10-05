import os
from datetime import timezone

from dagster import build_op_context, build_schedule_context, job

from dagster_utils.lib.pcloud import *

STUBS_DIR = os.path.join(os.path.dirname(__file__), "_stub", "pcloud")


def test_read_pcloud_files_by_id(mock_fetch_auth):
    with build_op_context(
        config={"file_ids": ["some_id"]},
        resources={"pcloud": StubUtilspCloudClient(stubs_dir=STUBS_DIR)},
    ) as context:
        res = read_pcloud_files_by_id(context)

    assert res == [
        UtilsFileSystemOutputType(
            filename="some_name",
            content=b"some file content",
            meta={
                "file_size": 2,
                "content_type": "some-content-type",
                "created_at": datetime(2022, 10, 19, 7, 56, 33, tzinfo=timezone.utc),
                "last_modified": datetime(2022, 10, 19, 7, 56, 33, tzinfo=timezone.utc),
            },
        )
    ]


def test_make_pcloud_schedule():
    @op
    def my_op():
        return 1

    @job
    def my_job():
        my_op()

    file_schedule_definition = make_pcloud_file_schedule(
        job=my_job,
        cron_schedule="* * * * *",
        root_folder_id="123456",
        run_config={"resources": {}},
        subfolder_to_process="log_mock",
    )

    assert type(file_schedule_definition) == ScheduleDefinition

    with build_schedule_context(
        resources={"pcloud": StubUtilspCloudClient(stubs_dir=STUBS_DIR)}
    ) as context:
        assert [i for i in file_schedule_definition(context)] == [
            RunRequest(
                run_key="some_folder",
                run_config={"resources": {}},
            )
        ]


def test_make_pcloud_schedule_with_dynamic_partitions():
    from dagster import DagsterInstance, DynamicPartitionsDefinition

    my_partitions_def = DynamicPartitionsDefinition(name="mock")

    @op
    def my_op():
        return 1

    @job(partitions_def=my_partitions_def)
    def my_job():
        my_op()

    file_schedule_definition = make_pcloud_file_schedule(
        job=my_job,
        cron_schedule="* * * * *",
        root_folder_id="123456",
        dynamic_partitions_def=my_partitions_def,
        run_config={"resources": {}},
    )

    assert type(file_schedule_definition) == ScheduleDefinition

    with build_schedule_context(
        instance=DagsterInstance.get(),
        resources={"pcloud": StubUtilspCloudClient(stubs_dir=STUBS_DIR)},
    ) as context:
        assert file_schedule_definition(context) == [
            RunRequest(
                run_key="some_folder",
                run_config={"resources": {}},
                partition_key="some_folder",
            )
        ]
