import pytest

from dagster_utils.utils.check import *


def test_bytes_param():
    my_bytes = b"hello, world"

    assert bytes_param(my_bytes, "bytes_param") == b"hello, world"
    with pytest.raises(CheckError):
        assert bytes_param(None, "bytes_param") == None


def test_opt_bytes_param():
    my_bytes = b"hello, world"

    assert opt_bytes_param(my_bytes, "bytes_param") == b"hello, world"
    assert opt_bytes_param(None, "bytes_param") == None


def test_opt_nonempty_bytes_param():
    my_bytes = b"hello, world"

    assert opt_nonempty_bytes_param(my_bytes, "bytes_param") == b"hello, world"
    assert opt_nonempty_bytes_param(None, "bytes_param") == None


def test_bytes_elem():
    my_bytes = b"hello, world"

    assert bytes_elem({"bytes_param": my_bytes}, "bytes_param") == b"hello, world"
    with pytest.raises(CheckError):
        assert bytes_param({"bytes_param": my_bytes}, "bytes_param") == None


def test_opt_bytes_elem():
    my_bytes = b"hello, world"

    assert opt_bytes_elem({"bytes_param": my_bytes}, "bytes_param") == b"hello, world"
    assert opt_bytes_elem({"bytes_param": None}, "bytes_param") == None
