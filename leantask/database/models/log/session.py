from ._base import (
    LogModel, Column, Integer, BIG_STRING,
    column_uuid_primary_key, column_current_datetime
)
from ....enum import LogTableName
from ....utils.string import obj_repr


class SchedulerSessionModel(LogModel):
    __tablename__ = LogTableName.SCHEDULER_SESSION.value

    id = column_uuid_primary_key()
    heartbeat = Column(Integer, nullable=False)
    worker = Column(Integer, nullable=False)
    log_path = Column(BIG_STRING, nullable=False)

    created_datetime = column_current_datetime()

    def __repr__(self) -> str:
        return obj_repr(self, 'heartbeat', 'worker')
