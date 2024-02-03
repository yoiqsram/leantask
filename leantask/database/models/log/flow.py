from ._base import (
    LogModel, Boolean, Column, DateTime, Integer, ForeignKey,
    SMALL_STRING, MEDIUM_STRING, BIG_STRING, UUID_STRING,
    column_uuid_primary_key, column_current_datetime,
    column_md5, column_scheduler_session_id,
    relationship
)
from ....enum import LogTableName
from ....utils.string import obj_repr


class FlowLogModel(LogModel):
    __tablename__ = LogTableName.FLOW.value

    id = column_uuid_primary_key()
    name = Column(MEDIUM_STRING, primary_key=True, nullable=False)
    path = Column(BIG_STRING, nullable=False)
    checksum = column_md5()
    max_delay = Column(Integer)
    active = Column(Boolean, nullable=False)

    ref_id = Column(UUID_STRING, nullable=False)

    scheduler_session_id = column_scheduler_session_id()
    created_datetime = column_current_datetime()

    tasks = relationship(
        'TaskLogModel',
        back_populates='flow',
        cascade='all, delete-orphan'
    )

    flow_runs = relationship(
        'FlowRunLogModel',
        back_populates='flow',
        cascade='all, delete-orphan'
    )

    def __repr__(self) -> str:
        return obj_repr(self, 'name', 'path', 'active')


class FlowRunLogModel(LogModel):
    __tablename__ = LogTableName.FLOW_RUN.value

    id = column_uuid_primary_key()
    schedule_datetime = Column(DateTime)
    max_delay = Column(Integer)
    status = Column(SMALL_STRING, nullable=False)

    ref_id = Column(UUID_STRING, nullable=False)
    ref_flow_id = Column(UUID_STRING, ForeignKey('flows.ref_id'), nullable=False)
    ref_flow_schedule_id = Column(UUID_STRING)

    scheduler_session_id = column_scheduler_session_id()
    created_datetime = column_current_datetime()

    flow = relationship(
        'FlowLogModel',
        back_populates='flow_runs',
        uselist=False
    )

    task_runs = relationship(
        'TaskRunLogModel',
        back_populates='flow_run',
        cascade='all, delete-orphan'
    )

    def __repr__(self) -> str:
        return obj_repr(self, 'flow_id', 'schedule_datetime', 'max_delay', 'status')
