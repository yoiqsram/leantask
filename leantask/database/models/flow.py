from ._base import (
    Model, Boolean, Column, DateTime, Integer, ForeignKey,
    SMALL_STRING, MEDIUM_STRING, BIG_STRING, UUID_STRING,
    column_uuid_primary_key, column_current_datetime, column_modified_datetime,
    column_md5, column_scheduler_session_id,
    relationship
)
from ...enum import TableName
from ...utils.string import obj_repr


class FlowModel(Model):
    __tablename__ = TableName.FLOW.value

    id = column_uuid_primary_key()
    name = Column(MEDIUM_STRING, primary_key=True)
    path = Column(BIG_STRING, nullable=False)
    checksum = column_md5()
    max_delay = Column(Integer)
    active = Column(Boolean, default=False, nullable=False)

    created_datetime = column_current_datetime()
    modified_datetime = column_modified_datetime()

    tasks = relationship(
        'TaskModel',
        back_populates='flow',
        cascade='all, delete-orphan'
    )

    flow_schedule = relationship(
        'FlowScheduleModel',
        back_populates='flow',
        cascade='all, delete-orphan',
        uselist=False
    )

    flow_runs = relationship(
        'FlowRunModel',
        back_populates='flow',
        cascade='all, delete-orphan'
    )

    def __repr__(self) -> str:
        return obj_repr(self, 'id', 'name', 'path', 'active')


class FlowScheduleModel(Model):
    __tablename__ = TableName.FLOW_SCHEDULE.value

    id = column_uuid_primary_key()
    flow_id = Column(UUID_STRING, ForeignKey('flows.id'), nullable=False)
    schedule_datetime = Column(DateTime)
    is_manual = Column(Boolean, default=False)

    scheduler_session_id = column_scheduler_session_id()
    created_datetime = column_current_datetime()

    flow = relationship(
        'FlowModel',
        back_populates='flow_schedule',
        uselist=False
    )

    def __repr__(self) -> str:
        return obj_repr(self, 'id', 'flow_id', 'schedule_datetime', 'is_manual')


class FlowRunModel(Model):
    __tablename__ = TableName.FLOW_RUN.value

    id = column_uuid_primary_key()
    flow_id = Column(UUID_STRING, ForeignKey('flows.id'), nullable=False)
    schedule_datetime = Column(DateTime)
    max_delay = Column(Integer)
    status = Column(SMALL_STRING, nullable=False)

    flow_schedule_id = Column(UUID_STRING)

    created_datetime = column_current_datetime()
    modified_datetime = column_modified_datetime()

    flow = relationship(
        'FlowModel',
        back_populates='flow_runs',
        uselist=False
    )

    task_runs = relationship(
        'TaskRunModel',
        back_populates='flow_run',
        cascade='all, delete-orphan'
    )

    def __repr__(self) -> str:
        return obj_repr(self, 'id', 'flow_id', 'schedule_datetime', 'max_delay', 'status')
