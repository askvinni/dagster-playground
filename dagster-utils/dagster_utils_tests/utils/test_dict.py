from contextlib import contextmanager

import pytest

from dagster_utils.utils import dicts


@contextmanager
def not_raises(exception):
    """Helper function to explicitly test functions don't raise errors

    Raises:
        pytest.fail: Raises exception and fails test
    """
    try:
        yield
    except exception:
        raise pytest.fail(f"Raised exception {exception}")


def test_safeget_returns_none():
    dct = {"foo": {"bar": {}}}

    with not_raises(Exception):
        dct_foobarbaz = dicts.safeget(dct, "foo", "bar", "baz")

    assert dct_foobarbaz == None


def test_safeget_returns_nested_property():
    dct = {"foo": {"bar": {"baz": "qux"}}}

    with not_raises(Exception):
        dct_foobarbaz = dicts.safeget(dct, "foo", "bar", "baz")

    assert dct_foobarbaz == "qux"


def test_build_dicts_from_nested_properties():
    dct = {"foo": {"bar": {"baz": "qux"}}}

    dct_foobarbaz = dicts.build_dict_from_nested_properties(
        dct, {"quux": ["foo", "bar", "baz"]}
    )

    assert dct_foobarbaz == {"quux": "qux"}


def test_translate_dicts():
    dct = {"foo": "bar"}

    dct_foobarbaz = dicts.translate_dict(dct, {"foo": "baz"})

    assert dct_foobarbaz == {"baz": "bar"}


def test_merge_nested_dicts():
    dct = {"foo": {"bar": "baz"}}
    other_dct = {"foo": {"qux": "quux"}}

    assert dicts.merge_dicts(dct, other_dct) == {"foo": {"bar": "baz", "qux": "quux"}}
