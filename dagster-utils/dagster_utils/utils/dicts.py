from typing import Union


def safeget(dct: dict, *keys) -> Union[dict, None]:
    """Loops through dictionary checking if nested objects specified exist, returns None if any not available

    Args:
        dct (dict): dictionary to loop through
    """
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct


def build_dict_from_nested_properties(dict, info_location) -> dict[str, str]:
    return {
        property: safeget(dict, *info_location)
        for property, info_location in info_location.items()
    }


def translate_dict(dict, properties_dict):
    return {translated: dict.get(prop) for prop, translated in properties_dict.items()}


def merge_dicts(a, b, path=None):
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_dicts(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                raise Exception(f"Conflict at {'.'.join(path + [str(key)])}")
        else:
            a[key] = b[key]
    return a
