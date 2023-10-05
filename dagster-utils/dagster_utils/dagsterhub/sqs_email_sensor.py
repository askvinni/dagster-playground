import boto3
from dagster import DefaultSensorStatus, RunRequest, SkipReason, sensor


def generate_sqs_sensor(
    sensor_name: str,
    jobs: list,
    queue_url: str,
    message_to_job: dict,
    interval: int = None,
):
    @sensor(
        name=sensor_name,
        minimum_interval_seconds=interval,
        jobs=jobs,
        default_status=DefaultSensorStatus.RUNNING,
    )
    def sqs_sensor():
        sqs = boto3.client("sqs")

        response = sqs.receive_message(QueueUrl=queue_url)
        try:
            message = response["Messages"][0]
            receipt_handle = message["ReceiptHandle"]
            message_body = message["Body"]

            if message_body.lower() in message_to_job.keys():
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                return RunRequest(
                    run_key=receipt_handle, job_name=message_to_job[message_body]
                )
            else:
                return SkipReason("Message does not match any jobs in this repository")
        except Exception:
            return SkipReason("No new messages in SQS queue.")

    return sqs_sensor
