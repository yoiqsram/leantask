from sqlalchemy import (
    Column, Boolean, DateTime, Integer, String,
    ForeignKey, UniqueConstraint, func
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from ...context import GlobalContext
from ...utils.string import generate_uuid

MD5_STRING = String(32)
UUID_STRING = String(36)
SMALL_STRING = String(50)
MEDIUM_STRING = String(100)
BIG_STRING = String(250)


def column_current_datetime():
    return Column(DateTime, default=func.now(), nullable=False)


def column_modified_datetime():
    return Column(DateTime, server_default=func.now(), onupdate=func.now())


def column_md5():
    return Column(MD5_STRING, nullable=False)


def column_uuid_primary_key():
    return Column(UUID_STRING, primary_key=True, default=generate_uuid)


def column_scheduler_session_id():
    return Column(
        UUID_STRING,
        default=lambda: GlobalContext.SCHEDULER_SESSION_ID
    )


def unique_compound_constraint(table_name: str, *columns):
    return UniqueConstraint(
        *columns,
        name=table_name + '__' + '_'.join(columns) + '__unq'
    )
