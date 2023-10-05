from dagster._check import *
from dagster._check import _element_check_error, _param_type_mismatch_exception

# ########################
# ##### BYTES
# ########################


def bytes_param(
    obj: object, param_name: bytes, additional_message: Optional[bytes] = None
) -> bytes:
    if not isinstance(obj, bytes):
        raise _param_type_mismatch_exception(obj, bytes, param_name, additional_message)
    return obj


@overload
def opt_bytes_param(
    obj: object,
    param_name: bytes,
    default: bytes,
    additional_message: Optional[bytes] = ...,
) -> bytes:
    ...


@overload
def opt_bytes_param(
    obj: object,
    param_name: bytes,
    default: Optional[bytes] = ...,
    additional_message: Optional[bytes] = ...,
) -> Optional[bytes]:
    ...


def opt_bytes_param(
    obj: object,
    param_name: bytes,
    default: Optional[bytes] = None,
    additional_message: Optional[bytes] = None,
) -> Optional[bytes]:
    if obj is not None and not isinstance(obj, bytes):
        raise _param_type_mismatch_exception(obj, bytes, param_name, additional_message)
    return default if obj is None else obj


def opt_nonempty_bytes_param(
    obj: object,
    param_name: bytes,
    default: Optional[bytes] = None,
    additional_message: Optional[bytes] = None,
) -> Optional[bytes]:
    if obj is not None and not isinstance(obj, bytes):
        raise _param_type_mismatch_exception(obj, bytes, param_name, additional_message)
    return default if obj is None or obj == b"" else obj


def bytes_elem(
    ddict: Mapping, key: bytes, additional_message: Optional[bytes] = None
) -> bytes:
    dict_param(ddict, "ddict")

    value = ddict[key]
    if not isinstance(value, bytes):
        raise _element_check_error(key, value, ddict, bytes, additional_message)
    return value


def opt_bytes_elem(
    ddict: Mapping, key: bytes, additional_message: Optional[bytes] = None
) -> Optional[bytes]:
    dict_param(ddict, "ddict")

    value = ddict.get(key)
    if value is None:
        return None
    if not isinstance(value, bytes):
        raise _element_check_error(key, value, ddict, bytes, additional_message)
    return value
