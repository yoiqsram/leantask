from ...enum import LogTableName
from ..base import LogModel
from ..common import (
    ForeignKeyField,
    column_boolean, column_integer,
    column_small_string, column_medium_string, column_big_string, column_text,
    column_md5_string, column_uuid_string, column_uuid_primary_key,
    column_datetime, column_current_datetime
)
from .session import SchedulerSessionModel


class FlowLogModel(LogModel):
    id = column_uuid_primary_key()
    path = column_big_string()
    name = column_medium_string()
    description = column_text(null=True)
    cron_schedules = column_medium_string(null=True)
    start_datetime = column_datetime(null=True)
    end_datetime = column_datetime(null=True)
    max_delay = column_integer(null=True)
    checksum = column_md5_string()
    active = column_boolean(default=False)

    ref_id = column_uuid_string()

    scheduler_session = ForeignKeyField(
        SchedulerSessionModel,
        backref='flow_runs',
        on_delete='CASCADE',
        null=True
    )
    created_datetime = column_current_datetime()

    class Meta:
        table_name = LogTableName.FLOW.value


class FlowRunLogModel(LogModel):
    id = column_uuid_primary_key()
    schedule_datetime = column_datetime(null=True)
    max_delay = column_integer(null=True)
    is_manual = column_boolean(default=False)
    params = column_text(null=True)
    status = column_small_string()

    ref_id = column_uuid_string()
    ref_flow_schedule_id = column_uuid_string(null=True)
    ref_flow = ForeignKeyField(
        FlowLogModel,
        field=FlowLogModel.ref_id,
        backref='flow_runs',
        on_delete='CASCADE'
    )

    scheduler_session = ForeignKeyField(
        SchedulerSessionModel,
        backref='flow_runs',
        on_delete='CASCADE',
        null=True
    )
    created_datetime = column_current_datetime()
    started_datetime = column_datetime(null=True)

    class Meta:
        table_name = LogTableName.FLOW_RUN.value
