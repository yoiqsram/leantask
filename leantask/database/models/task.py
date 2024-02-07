from ._base import (
    Model, Column, Integer, ForeignKey,
    SMALL_STRING, MEDIUM_STRING, UUID_STRING,
    column_uuid_primary_key, column_current_datetime, column_modified_datetime,
    unique_compound_constraint, relationship
)
from ...enum import TableName
from ...utils.string import obj_repr


class TaskModel(Model):
    __tablename__ = TableName.TASK.value

    id = column_uuid_primary_key()
    flow_id = Column(UUID_STRING, ForeignKey('flows.id'), nullable=False)
    name = Column(MEDIUM_STRING, nullable=False)
    retry_max = Column(Integer, default=0, nullable=False)
    retry_delay = Column(Integer, default=0, nullable=False)

    created_datetime = column_current_datetime()

    flow = relationship(
        'FlowModel',
        back_populates='tasks',
        uselist=False
    )

    task_runs = relationship(
        'TaskRunModel',
        back_populates='task',
        cascade='all, delete-orphan'
    )

    __table_args__ = (
        unique_compound_constraint(__tablename__, 'flow_id', 'name'),
    )

    def __repr__(self) -> str:
        return obj_repr(self, 'id', 'flow_id', 'name', 'retry_max')


class TaskDownstreamModel(Model):
    __tablename__ = TableName.TASK_DOWNSTREAM.value

    id = column_uuid_primary_key()
    task_id = Column(UUID_STRING, ForeignKey('tasks.id'), nullable=False)
    downstream_task_id = Column(UUID_STRING, ForeignKey('tasks.id'), nullable=False)

    task = relationship('TaskModel', foreign_keys=[task_id])
    downstream_task = relationship('TaskModel', foreign_keys=[downstream_task_id])

    def __repr__(self) -> str:
        return obj_repr(self, 'id', 'task_id', 'downstream_task_id')


class TaskRunModel(Model):
    __tablename__ = TableName.TASK_RUN.value

    id = column_uuid_primary_key()
    flow_run_id = Column(UUID_STRING, ForeignKey('flow_runs.id'), nullable=False)
    task_id = Column(UUID_STRING, ForeignKey('tasks.id'), nullable=False)
    attempt = Column(Integer, nullable=False)
    retry_max = Column(Integer, default=0, nullable=False)
    retry_delay = Column(Integer, default=0, nullable=False)
    status = Column(SMALL_STRING, nullable=False)

    created_datetime = column_current_datetime()
    modified_datetime = column_modified_datetime()

    flow_run = relationship(
        'FlowRunModel',
        back_populates='task_runs',
        uselist=False
    )

    task = relationship(
        'TaskModel',
        back_populates='task_runs',
        uselist=False
    )

    __table_args__ = (
        unique_compound_constraint(__tablename__, 'flow_run_id', 'task_id', 'attempt'),
    )

    def __repr__(self) -> str:
        return obj_repr(self, 'id', 'flow_run_id', 'task_id', 'status', 'attempt', 'retry_max')
