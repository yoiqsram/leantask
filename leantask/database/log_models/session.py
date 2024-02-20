from ...enum import LogTableName
from ..base import LogModel
from ..common import (
    column_integer, column_big_string,
    column_uuid_primary_key, column_current_datetime
)


class SchedulerSessionModel(LogModel):
    id = column_uuid_primary_key()
    heartbeat = column_integer()
    worker = column_integer()
    log_path = column_big_string()

    created_datetime = column_current_datetime()

    class Meta:
        table_name = LogTableName.SCHEDULER_SESSION.value
