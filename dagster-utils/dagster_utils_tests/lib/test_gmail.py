import os

from dagster import build_op_context

from dagster_utils.lib import (
    StubUtilsGMailClient,
    UtilsFileSystemOutputType,
    UtilsGMailClient,
    fetch_from_gmail,
)

STUBS_DIR = os.path.join(os.path.dirname(__file__), "_stub", "gmail")

# Dagster tests
def test_gmail_resource_init(mock_fetch_auth):
    resource = UtilsGMailClient()

    assert type(resource) is UtilsGMailClient


def test_fetch_from_gmail():
    context = build_op_context(
        config={"function": {"name": "extract_attachments", "args": {}}},
        resources={"gmail": StubUtilsGMailClient(stubs_dir=STUBS_DIR)},
    )

    op_res = fetch_from_gmail(context=context)

    assert op_res == [
        UtilsFileSystemOutputType(
            filename="some attachment.xml",
            content=b"\xff\xfeI\x00'\x00m\x00 \x00a\x00 \x00f\x00u\x00n\x00 \x00g\x00u\x00y\x00",
        )
    ]
    assert (
        op_res[0].content
        == b"\xff\xfeI\x00'\x00m\x00 \x00a\x00 \x00f\x00u\x00n\x00 \x00g\x00u\x00y\x00"
    )


# Integration tests
def test_entire_middleware():
    resource = StubUtilsGMailClient(stubs_dir=STUBS_DIR)

    gmail_res = resource.fetch(
        {"function": {"name": "extract_attachments", "args": {}}}
    )

    assert gmail_res == [
        UtilsFileSystemOutputType(
            filename="some attachment.xml",
            content=b"\xff\xfeI\x00'\x00m\x00 \x00a\x00 \x00f\x00u\x00n\x00 \x00g\x00u\x00y\x00",
        )
    ]


def test__extract_attachments():
    resource = StubUtilsGMailClient(stubs_dir=STUBS_DIR)

    attachments = resource._extract_attachments()
    assert attachments == [
        UtilsFileSystemOutputType(
            filename="some attachment.xml",
            content=b"\xff\xfeI\x00'\x00m\x00 \x00a\x00 \x00f\x00u\x00n\x00 \x00g\x00u\x00y\x00",
        )
    ]


# Test single functions
def test__is_attachment_part_true():
    resource = StubUtilsGMailClient(stubs_dir=STUBS_DIR)

    is_attachment = resource._is_attachment_part(
        {"filename": "some-file", "body": {"attachmentId": "some_id"}}
    )
    assert is_attachment == True


def test__is_attachment_part_false():
    resource = StubUtilsGMailClient(stubs_dir=STUBS_DIR)

    is_attachment = resource._is_attachment_part(
        {"filename": "", "body": {"attachmentId": "some_id"}}
    )
    assert is_attachment == False


def test__get_attachments_from_messages():
    resource = StubUtilsGMailClient(stubs_dir=STUBS_DIR)

    attachments = resource._get_attachments_from_message("foo")
    assert attachments == [
        UtilsFileSystemOutputType(
            filename="some attachment.xml",
            content=b"\xff\xfeI\x00'\x00m\x00 \x00a\x00 \x00f\x00u\x00n\x00 \x00g\x00u\x00y\x00",
        )
    ]
