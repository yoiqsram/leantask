import re
import uuid

SAFE_CHARS_PATTERN = r'^[a-zA-Z0-9-_.+,*`()$]+$'


def validate_use_safe_chars(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError(
            f"Value must be a string, not '{type(value)}'."
        )

    if re.match(SAFE_CHARS_PATTERN, value) is None:
        raise ValueError(
            f"Value should only use safe characters. ({SAFE_CHARS_PATTERN})"
        )

    return value


def generate_uuid():
    return str(uuid.uuid4())