from ._base import (
    LogModel, Boolean, Column, DateTime, Integer, ForeignKey,
    SMALL_STRING, MEDIUM_STRING, BIG_STRING, UUID_STRING,
    column_uuid_primary_key, column_current_datetime,
    column_md5, column_scheduler_session_id,
    relationship
)
from ....enum import LogTableName


class FlowLogModel(LogModel):
    __tablename__ = LogTableName.FLOW.value

    id = column_uuid_primary_key()
    name = Column(MEDIUM_STRING, primary_key=True, nullable=False)
    path = Column(BIG_STRING, nullable=False)
    checksum = column_md5()
    max_delay = Column(Integer)
    active = Column(Boolean, default=False, nullable=False)

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
        cascade='all, delete-orphan',
    )

    def __repr__(self):
        return (
            f'<FlowLog(name={repr(self.name)}'
            f' path={repr(self.path)}'
            f' active={repr(self.active)})>'
        )


class FlowRunLogModel(LogModel):
    __tablename__ = LogTableName.FLOW_RUN.value

    id = column_uuid_primary_key()
    schedule_datetime = Column(DateTime)
    status = Column(SMALL_STRING, nullable=False)

    ref_id = Column(UUID_STRING, nullable=False)
    ref_flow_id = Column(UUID_STRING, ForeignKey('flows.ref_id'), nullable=False)
    ref_flow_schedule_id = Column(UUID_STRING)

    scheduler_session_id = column_scheduler_session_id()
    created_datetime = column_current_datetime()

    def __repr__(self):
        return (
            f'<FlowRunLog(flow_id={repr(self.flow_id)}'
            f' schedule_datetime={repr(self.schedule_datetime)}'
            f' status={repr(self.status)})>'
        )
