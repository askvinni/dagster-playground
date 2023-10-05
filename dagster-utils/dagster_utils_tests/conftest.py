import boto3
import pytest
from moto import mock_s3


@pytest.fixture(autouse=True)
def aws_creds(monkeypatch):
    """Mocked AWS Credentials for moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")


@pytest.fixture
def mock_s3_resource():
    with mock_s3():
        yield boto3.resource("s3").meta.client


@pytest.fixture
def mock_s3_bucket(mock_s3_resource):
    location = {"LocationConstraint": "eu-central-1"}
    yield mock_s3_resource.create_bucket(
        Bucket="test-bucket", CreateBucketConfiguration=location
    )
