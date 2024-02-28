from datetime import datetime
from pathlib import Path
from peewee import (
    Database, SqliteDatabase,
    AutoField, IntegerField, FloatField,
    CharField, FixedCharField, TextField,
    BooleanField, DateTimeField, Field,
    ForeignKeyField, SQL
)

from ..context import GlobalContext
from ..utils.string import generate_uuid

MD5_CHAR_LENGTH = 32
UUID_CHAR_LENGTH = 36
SMALL_CHAR_LENGTH = 50
MEDIUM_CHAR_LENGTH = 100
BIG_CHAR_LENGTH = 250


def open_db_connection(database_path: Path) -> Database:
    return SqliteDatabase(database_path)


def column_datetime(**kwargs) -> Field:
    return DateTimeField(**kwargs)


def column_current_datetime(**kwargs) -> Field:
    return column_datetime(null=False, default=datetime.now, **kwargs)


def column_modified_datetime(**kwargs) -> Field:
    return column_datetime(null=False, default=datetime.now, **kwargs)


def column_boolean(**kwargs) -> Field:
    return BooleanField(**kwargs)


def column_sequence(**kwargs) -> Field:
    return AutoField(**kwargs)


def column_integer(**kwargs) -> Field:
    return IntegerField(**kwargs)


def column_float(**kwargs) -> Field:
    return FloatField(**kwargs)


def column_small_string(**kwargs) -> Field:
    return CharField(SMALL_CHAR_LENGTH, **kwargs)


def column_medium_string(**kwargs) -> Field:
    return CharField(MEDIUM_CHAR_LENGTH, **kwargs)


def column_big_string(**kwargs) -> Field:
    return CharField(BIG_CHAR_LENGTH, **kwargs)


def column_text(**kwargs) -> Field:
    return TextField(**kwargs)


def column_md5_string(**kwargs) -> Field:
    return FixedCharField(MD5_CHAR_LENGTH, **kwargs)


def column_uuid_string(**kwargs) -> Field:
    return FixedCharField(max_length=36, **kwargs)


def column_uuid_primary_key(**kwargs) -> Field:
    return column_uuid_string(primary_key=True, default=generate_uuid, **kwargs)


def column_scheduler_session_id(**kwargs) -> Field:
    return column_uuid_string(
        default=lambda: GlobalContext.SCHEDULER_SESSION_ID,
        **kwargs
    )
