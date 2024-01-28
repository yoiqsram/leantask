from ._base import (
    Model, Column, Integer, ForeignKey,
    SMALL_STRING, MEDIUM_STRING, UUID_STRING,
    column_uuid_primary_key, column_current_datetime, column_modified_datetime,
    unique_compound_constraint, relationship
)
from ...enum import TableName


class TaskModel(Model):
    __tablename__ = TableName.TASK.value

    id = column_uuid_primary_key()
    flow_id = Column(UUID_STRING, ForeignKey('flows.id'), nullable=False)
    name = Column(MEDIUM_STRING, nullable=False)

    created_datetime = column_current_datetime()

    flow = relationship(
        'FlowModel',
        back_populates='tasks',
        uselist=False
    )

    task_schedules = relationship(
        'TaskScheduleModel',
        back_populates='task',
        cascade='all, delete-orphan'
    )

    task_runs = relationship(
        'TaskRunModel',
        back_populates='task',
        cascade='all, delete-orphan'
    )

    __table_args__ = (
        unique_compound_constraint(__tablename__, 'flow_id', 'name'),
    )

    def __repr__(self):
        return (
            f'<Task(name={repr(self.name)}'
            f' flow_id={repr(self.flow_id)})>'
        )


class TaskDownstreamModel(Model):
    __tablename__ = TableName.TASK_DOWNSTREAM.value

    id = column_uuid_primary_key()
    task_id = Column(UUID_STRING, ForeignKey('tasks.id'), nullable=False)
    downstream_task_id = Column(UUID_STRING, ForeignKey('tasks.id'), nullable=False)

    task = relationship('TaskModel', foreign_keys=[task_id])
    downstream_task = relationship('TaskModel', foreign_keys=[downstream_task_id])

    def __repr__(self):
        return (
            f'<TaskDownstream(task_id={repr(self.task_id)}'
            f' downstream_task_id={repr(self.downstream_task_id)})>'
        )


class TaskScheduleModel(Model):
    __tablename__ = TableName.TASK_SCHEDULE.value

    id = column_uuid_primary_key()
    flow_schedule_id = Column(UUID_STRING, ForeignKey('flow_schedules.id'), nullable=False)
    task_id = Column(UUID_STRING, ForeignKey('tasks.id'), nullable=False)

    created_datetime = column_current_datetime()

    flow_schedule = relationship(
        'FlowScheduleModel',
        back_populates='task_schedules',
        uselist=False
    )

    task = relationship(
        'TaskModel',
        back_populates='task_schedules',
        uselist=False
    )

    def __repr__(self):
        return (
            f'<TaskSchedule(flow_schedule_id={repr(self.flow_schedule_id)}'
            f' task_id={repr(self.task_id)})>'
        )


class TaskRunModel(Model):
    __tablename__ = TableName.TASK_RUN.value

    id = column_uuid_primary_key()
    flow_run_id = Column(UUID_STRING, ForeignKey('flow_runs.id'), nullable=False)
    task_id = Column(UUID_STRING, ForeignKey('tasks.id'), nullable=False)
    attempt = Column(Integer, nullable=False)
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

    def __repr__(self):
        return (
            f'<TaskRun(flow_run_id={repr(self.flow_run_id)}'
            f' task_id={repr(self.task_id)}'
            f' attempt={repr(self.attempt)}'
            f' status={repr(self.status)})>'
        )
