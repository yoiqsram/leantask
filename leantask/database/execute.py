import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, List, Union

from ..enum import FlowRunStatus
from ..context import GlobalContext
from .base import database
from ..database import (
    FlowModel, FlowScheduleModel, FlowRunModel,
    MetadataModel,
    TaskModel, TaskDownstreamModel, TaskRunModel,
    FlowLogModel, FlowRunLogModel,
    TaskLogModel, TaskDownstreamLogModel, TaskRunLogModel,
    SchedulerSessionModel,
    database, log_database
)


def create_scheduler_session(
        session_id: str,
        heartbeat: int,
        worker: int,
        log_file_path: Path
    ) -> None:
    scheduler_session_record = SchedulerSessionModel(
        id=session_id,
        heartbeat=heartbeat,
        worker=worker,
        log_path=str(log_file_path)
    )
    scheduler_session_record.save()

    GlobalContext.set_scheduler_session(
        scheduler_session_record.id,
        scheduler_session_record.created_datetime
    )
    return session_id


def create_metadata_database(
        project_name: str,
        project_description: str = None
    ) -> None:
    try:
        database.create_tables([
            FlowModel, FlowScheduleModel, FlowRunModel,
            TaskModel, TaskDownstreamModel, TaskRunModel,
            MetadataModel
        ])
        log_database.create_tables([
            FlowLogModel, FlowRunLogModel,
            TaskLogModel, TaskDownstreamLogModel, TaskRunLogModel,
            SchedulerSessionModel
        ])

        project_metadata = {
            'name': project_name,
            'description': project_description,
            'is_active': True
        }

        for name, value in project_metadata.items():
            MetadataModel.create(name=name, value=str(value))

    except Exception as exc:
        GlobalContext.database_path().unlink(missing_ok=True)
        GlobalContext.log_database_path().unlink(missing_ok=True)
        raise exc


def get_scheduled_run_tasks(
        __datetime: datetime = None
    ) -> Dict[Path, Dict[str, Union[str, List[str]]]]:
    if __datetime is None:
        __datetime = datetime.now()

    select_fields = (
        FlowModel.id,
        FlowModel.path,
        FlowModel.name,
        FlowModel.checksum,
        FlowScheduleModel.id,
        FlowRunModel.id,
        FlowRunModel.status,
        FlowRunModel.schedule_datetime,
        FlowRunModel.max_delay,
        TaskModel.id,
        TaskModel.name,
        TaskRunModel.id,
        TaskRunModel.status,
        TaskRunModel.attempt,
        TaskRunModel.retry_max,
        TaskRunModel.retry_delay
    )
    schedule_records = (
        TaskRunModel.select(*select_fields)
        .join(FlowRunModel)
        .join(FlowScheduleModel)
        .join(TaskModel)
        .join(FlowModel)
        .where(
            FlowModel.active == True,
            FlowRunModel.schedule_datetime <= __datetime,
            FlowRunModel.status.in_((
                FlowRunStatus.SCHEDULED.name,
                FlowRunStatus.SCHEDULED_BY_USER.name,
                FlowRunStatus.RUNNING.name
            ))
        )
        .execute()
    )

    flow_task_schedules = dict()
    for (flow_id, path, name, checksum, schedule_id, run_id, run_status, schedule_datetime, run_max_delay,
            task_id, task_name, task_run_id, task_run_status, task_run_attempt, task_run_retry_max, task_run_retry_delay
            ) in schedule_records:
        path = Path(path)
        if path not in flow_task_schedules:
            flow_task_schedules[path] = {
                'id': flow_id,
                'name': name,
                'checksum': checksum,
                'schedule_id': schedule_id,
                'schedule_datetime': schedule_datetime,
                'run_id': run_id,
                'status': run_status,
                'max_delay': run_max_delay,
                'tasks': dict()
            }

        flow_task_schedules[path]['tasks'][task_name] = {
            'id': task_id,
            'run_id': task_run_id,
            'attempt': task_run_attempt,
            'retry_max': task_run_retry_max,
            'retry_delay': task_run_retry_delay,
            'status': task_run_status
        }

    return flow_task_schedules


def copy_records_to_log(
        records: List[Union[
            FlowModel, FlowRunModel, TaskModel,
            TaskDownstreamModel, TaskRunModel
        ]]
    ) -> None:
    log_records = []
    for record in records:
        kwargs = dict()
        columns = [column.name for column in record._meta.fields]
        for column in columns:
            if column == 'id':
                kwargs['ref_id'] = getattr(record, column)

            elif column.endswith('id'):
                kwargs[f'ref_{column}'] = getattr(record, column)

            elif column == 'modified_datetime':
                kwargs['created_datetime'] = getattr(record, column)

            elif column == 'created_datetime' and 'modified_datetime' in columns:
                continue

            else:
                kwargs[column] = getattr(record, column)

        log_model_name = 'log_models.' + record.__class__.__name__.split('Model')[0] + 'LogModel'
        log_record = eval(log_model_name)(**kwargs)
        log_records.append(log_record)


def iter_log_run_records(
        flow_name: str,
        start_datetime: datetime,
        end_datetime: datetime
    ) -> Generator:
    select_columns = [
        FlowLogModel.name,
        FlowLogModel.path,
        FlowLogModel.max_delay,
        FlowLogModel.created_datetime,
        FlowRunLogModel.schedule_datetime,
        TaskLogModel.name,
    ]
    log_run_record_proxy = (
        session.query(select_columns)
        .join(FlowRunLogModel)
        .join(TaskLogModel)
        .join(TaskLogModel)
        .filter(
            FlowLogModel.name == flow_name,
            FlowRunLogModel.schedule_datetime >= start_datetime,
            FlowRunLogModel.schedule_datetime < end_datetime,
        )
        .execute()
    )

    for log_run_record in log_run_record_proxy:
        yield log_run_record

    log_run_record_proxy.close()
