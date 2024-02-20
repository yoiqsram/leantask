from ...enum import LogTableName
from ..base import LogModel
from ..common import (
    ForeignKeyField,
    column_integer, column_small_string, column_medium_string,
    column_uuid_string, column_uuid_primary_key,
    column_current_datetime
)
from .flow import FlowLogModel, FlowRunLogModel
from .session import SchedulerSessionModel


class TaskLogModel(LogModel):
    id = column_uuid_primary_key()
    name = column_medium_string()
    retry_max = column_integer(default=0)
    retry_delay = column_integer(default=0)

    ref_id = column_uuid_string()
    ref_flow = ForeignKeyField(
        FlowLogModel,
        to_field=FlowLogModel.ref_id,
        backref='tasks',
        on_delete='CASCADE'
    )

    created_datetime = column_current_datetime()

    class Meta:
        table_name = LogTableName.TASK.value


class TaskDownstreamLogModel(LogModel):
    id = column_uuid_primary_key()

    ref_id = column_uuid_string()
    ref_task = ForeignKeyField(
        TaskLogModel,
        to_field=TaskLogModel.ref_id,
        backref='downstreams',
        on_delete='CASCADE'
    )
    ref_downstream_task = ForeignKeyField(
        TaskLogModel,
        to_field=TaskLogModel.ref_id,
        backref='upstreams',
        on_delete='CASCADE'
    )

    class Meta:
        table_name = LogTableName.TASK_DOWNSTREAM.value


class TaskRunLogModel(LogModel):
    id = column_uuid_primary_key()
    attempt = column_integer()
    retry_max = column_integer(default=0)
    retry_delay = column_integer(default=0)
    status = column_small_string()

    ref_id = column_uuid_string()
    ref_flow_run = ForeignKeyField(
        FlowRunLogModel,
        to_field=FlowRunLogModel.ref_id,
        backref='task_runs',
        on_delete='CASCADE'
    )
    ref_task = ForeignKeyField(
        TaskLogModel,
        to_field=TaskLogModel.ref_id,
        backref='task_runs',
        on_delete='CASCADE'
    )

    scheduler_session = ForeignKeyField(
        SchedulerSessionModel,
        backref='task_runs',
        on_delete='CASCADE',
        null=True
    )
    created_datetime = column_current_datetime()

    class Meta:
        table_name = LogTableName.TASK_RUN.value
