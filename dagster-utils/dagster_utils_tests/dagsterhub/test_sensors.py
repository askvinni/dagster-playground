import boto3
import pytest
from dagster import RunRequest, SkipReason, job, op

from dagster_utils.dagsterhub import generate_sqs_sensor


@op
def some_op():
    return "foo"


@job
def some_job():
    some_op()


@pytest.fixture
def mock_sqs(monkeypatch):
    class MockSQSClient:
        def __init__(self, *args, **kwargs):
            pass

        def receive_message(self, *args, **kwargs):
            if kwargs["QueueUrl"] == "example.com":
                return {}
            elif kwargs["QueueUrl"] == "foobar":
                return {"Messages": [{"ReceiptHandle": "foo", "Body": "bar"}]}

        def delete_message(self, *args, **kwargs):
            return {}

    monkeypatch.setattr(boto3, "client", MockSQSClient)


def test_generate_sqs_sensor_returns_skipreason(mock_sqs):
    sensor = generate_sqs_sensor("foo", [some_job], "example.com", {})

    assert sensor() == SkipReason("No new messages in SQS queue.")


def test_generate_sqs_sensor_message_no_match(mock_sqs):
    sensor = generate_sqs_sensor("foo", [some_job], "foobar", {})

    assert sensor() == SkipReason("Message does not match any jobs in this repository")


def test_generate_sqs_sensor_yields_runrequest(mock_sqs):
    sensor = generate_sqs_sensor("foo", [some_job], "foobar", {"bar": "bla"})

    assert sensor() == RunRequest(run_key="foo", job_name="bla")
