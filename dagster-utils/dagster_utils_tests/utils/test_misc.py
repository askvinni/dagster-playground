from dagster_utils.utils.misc import *


def test_logical_xor():
    assert logical_xor("hello", "world") is False
    assert logical_xor("hello", "") is True
    assert logical_xor("hello") is True
    assert logical_xor("hello", {}) is True
    assert logical_xor("hello", {"foo": "bar"}) is False
    assert logical_xor("hello", []) is True
    assert logical_xor("hello", [0]) is False
    assert logical_xor(1, 0) is True
    assert logical_xor(1, 1) is False
