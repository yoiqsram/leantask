from ._base import (
    LogModel, Column, Integer, ForeignKey,
    SMALL_STRING, MEDIUM_STRING, UUID_STRING,
    column_uuid_primary_key, column_current_datetime,
    unique_compound_constraint, relationship
)
from ....enum import LogTableName


class TaskLogModel(LogModel):
    __tablename__ = LogTableName.TASK.value

    id = column_uuid_primary_key()
    name = Column(MEDIUM_STRING, nullable=False)

    ref_id = Column(UUID_STRING, nullable=False)
    ref_flow_id = Column(UUID_STRING, ForeignKey('flows.ref_id'), nullable=False)

    created_datetime = column_current_datetime()

    flow = relationship(
        'FlowLogModel',
        back_populates='tasks',
        uselist=False
    )

    task_downstreams = relationship(
        'TaskDownstreamLogModel',
        back_populates='task',
        cascade='all, delete-orphan'
    )

    task_runs = relationship(
        'TaskRunLogModel',
        back_populates='task',
        cascade='all, delete-orphan'
    )

    __table_args__ = (
        unique_compound_constraint(__tablename__, 'ref_flow_id', 'name'),
    )

    def __repr__(self):
        return (
            f'<TaskLog(name={repr(self.name)}'
            f' flow_id={repr(self.flow_id)})>'
        )


class TaskDownstreamLogModel(LogModel):
    __tablename__ = LogTableName.TASK_DOWNSTREAM.value

    id = column_uuid_primary_key()
    ref_task_id = Column(UUID_STRING, ForeignKey('tasks.ref_id'), nullable=False)
    ref_downstream_task_id = Column(UUID_STRING, ForeignKey('tasks.ref_id'), nullable=False)

    task = relationship('TaskModel', foreign_keys=[ref_task_id])
    downstream_task = relationship('TaskModel', foreign_keys=[ref_downstream_task_id])

    def __repr__(self):
        return (
            f'<TaskDownstream(task_id={repr(self.ref_task_id)}'
            f' downstream_task_id={repr(self.ref_downstream_task_id)})>'
        )


class TaskRunLogModel(LogModel):
    __tablename__ = LogTableName.TASK_RUN.value

    id = column_uuid_primary_key()
    attempt = Column(Integer, nullable=False)
    status = Column(SMALL_STRING, nullable=False)

    ref_id = Column(UUID_STRING, nullable=False)
    ref_flow_run_id = Column(UUID_STRING, ForeignKey('flow_runs.ref_id'), nullable=False)
    ref_task_id = Column(UUID_STRING, ForeignKey('tasks.ref_id'), nullable=False)

    created_datetime = column_current_datetime()

    flow_run = relationship(
        'FlowRunLogModel',
        back_populates='task_runs',
        uselist=False
    )

    task = relationship(
        'TaskLogModel',
        back_populates='task_runs',
        uselist=False
    )

    def __repr__(self):
        return (
            f'<TaskRunLog(flow_run_id={repr(self.flow_run_id)}'
            f' task_id={repr(self.task_id)}'
            f' attempt={repr(self.attempt)}'
            f' status={repr(self.status)})>'
        )
