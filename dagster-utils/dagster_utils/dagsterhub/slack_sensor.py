import os

from dagster import DefaultSensorStatus
from dagster_slack import make_slack_on_run_failure_sensor


def slack_message_fn(context) -> str:
    return (
        f":daggy-fail: Job {context.pipeline_run.pipeline_name} failed! :daggy-fail: \n"
        f"Error: {context.failure_event.message}"
    )


sensor_status = (
    DefaultSensorStatus.RUNNING
    if os.getenv("DAGSTER_DEPLOYMENT", "dev") == "prod"
    else DefaultSensorStatus.STOPPED
)

slack_on_run_failure = make_slack_on_run_failure_sensor(
    channel="C03TT8ATCAW",
    slack_token=os.getenv("SLACK_TOKEN"),
    text_fn=slack_message_fn,
    dagit_base_url="htts://dagster.mushlabs.cloud",
    default_status=sensor_status,
)
