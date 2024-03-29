import re
import uuid
from typing import Any

SAFE_CHARS_PATTERN = r'^[a-zA-Z0-9-_.+,*`()$]+$'


def quote(obj: Any) -> str:
    return '"' + str(obj) + '"'


def obj_repr(obj, *attr_names) -> str:
    obj_name = obj.__class__.__name__
    return (
        f'{obj_name}(' +
        ', '.join(f'{attr_name}={repr(getattr(obj, attr_name))}' for attr_name in attr_names) +
        f')'
    )


def validate_use_safe_chars(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError(
            f"Value must be a string, not '{type(value)}'."
        )

    if re.match(SAFE_CHARS_PATTERN, value) is None:
        raise ValueError(
            f"Value should only use safe characters ({SAFE_CHARS_PATTERN}), but '{value}' is provided."
        )

    return value


def generate_uuid() -> str:
    return str(uuid.uuid4())
