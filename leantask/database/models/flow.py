from ...enum import TableName
from ..base import BaseModel
from ..common import (
    ForeignKeyField,
    column_boolean, column_integer,
    column_small_string, column_medium_string, column_big_string, column_text,
    column_md5_string, column_uuid_string, column_uuid_primary_key,
    column_datetime, column_current_datetime, column_modified_datetime
)
from ..log_models import FlowLogModel, FlowRunLogModel


class FlowModel(BaseModel):
    id = column_uuid_primary_key()
    path = column_big_string(null=True)
    name = column_medium_string(unique=True)
    description = column_text(null=True)
    cron_schedules = column_medium_string(null=True)
    start_datetime = column_datetime(null=True)
    end_datetime = column_datetime(null=True)
    max_delay = column_integer(null=True)
    checksum = column_md5_string(null=True)
    active = column_boolean(default=False)

    created_datetime = column_current_datetime()
    modified_datetime = column_modified_datetime()

    class Meta:
        table_name = TableName.FLOW.value
        log_model = FlowLogModel


class FlowScheduleModel(BaseModel):
    id = column_uuid_primary_key()
    flow = ForeignKeyField(
        FlowModel,
        backref='flow_schedules',
        on_delete='CASCADE'
    )
    schedule_datetime = column_datetime()
    max_delay = column_integer(null=True)
    is_manual = column_boolean(default=True)
    params = column_text(null=True)

    created_datetime = column_current_datetime()

    class Meta:
        table_name = TableName.FLOW_SCHEDULE.value


class FlowRunModel(BaseModel):
    id = column_uuid_primary_key()
    flow = ForeignKeyField(
        FlowModel,
        backref='flow_runs',
        on_delete='CASCADE'
    )
    schedule_datetime = column_datetime(null=True)
    max_delay = column_integer(null=True)
    is_manual = column_boolean(default=False)
    params = column_text(null=True)
    status = column_small_string()

    flow_schedule_id = column_uuid_string(null=True)

    created_datetime = column_current_datetime()
    modified_datetime = column_modified_datetime()
    started_datetime = column_datetime(null=True)

    class Meta:
        table_name = TableName.FLOW_RUN.value
        log_model = FlowRunLogModel
