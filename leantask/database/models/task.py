from ...enum import TableName
from ..base import BaseModel
from ..common import (
    ForeignKeyField, SQL,
    column_integer, column_small_string, column_medium_string, column_uuid_primary_key,
    column_datetime, column_current_datetime, column_modified_datetime
)
from ..log_models import TaskLogModel, TaskDownstreamLogModel, TaskRunLogModel
from .flow import FlowModel, FlowRunModel


class TaskModel(BaseModel):
    id = column_uuid_primary_key()
    flow = ForeignKeyField(
        FlowModel,
        backref='tasks',
        on_delete='CASCADE'
    )
    name = column_medium_string()
    retry_max = column_integer(default=0)
    retry_delay = column_integer(default=0)

    created_datetime = column_current_datetime()

    class Meta:
        table_name = TableName.TASK.value
        constraints = [SQL('UNIQUE (flow_id, name)')]
        log_model = TaskLogModel


class TaskDownstreamModel(BaseModel):
    id = column_uuid_primary_key()
    task = ForeignKeyField(
        TaskModel,
        backref='downstreams',
        on_delete='CASCADE'
    )
    downstream_task = ForeignKeyField(
        TaskModel,
        backref='upstreams',
        on_delete='CASCADE'
    )

    class Meta:
        table_name = TableName.TASK_DOWNSTREAM.value
        log_model = TaskDownstreamLogModel


class TaskRunModel(BaseModel):
    id = column_uuid_primary_key()
    flow_run = ForeignKeyField(
        FlowRunModel,
        backref='task_runs',
        on_delete='CASCADE'
    )
    task = ForeignKeyField(
        TaskModel,
        backref='task_runs',
        on_delete='CASCADE'
    )
    attempt = column_integer()
    retry_max = column_integer(default=0)
    retry_delay = column_integer(default=0)
    status = column_small_string()

    created_datetime = column_current_datetime()
    modified_datetime = column_modified_datetime()
    started_datetime = column_datetime(null=True)

    class Meta:
        table_name = TableName.TASK_RUN.value
        constraints = [SQL('UNIQUE (flow_run_id, task_id, attempt)')]
        log_model = TaskRunLogModel
