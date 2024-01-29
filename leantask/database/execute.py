from datetime import datetime
from pathlib import Path
from sqlalchemy import desc, func
from typing import Dict, Generator, List, Union

from ..enum import TaskRunStatus
from ..context import GlobalContext
from .models import (
    FlowModel, FlowScheduleModel, FlowRunModel,
    TaskModel, TaskDownstreamModel, TaskScheduleModel, TaskRunModel,
    log as log_models
)
from .models.log import (
    FlowLogModel, FlowRunLogModel,
    TaskLogModel,
    SchedulerSessionModel
)
from .orm import db_session, Session


@db_session(GlobalContext.database_path())
def get_flow_record(
        name: str,
        session: Session = None
    ) -> FlowModel:
    flow_record = (
        session.query(FlowModel)
        .filter(FlowModel.name == name)
        .one()
    )
    return flow_record


@db_session(GlobalContext.database_path())
def get_flow_records(session: Session = None) -> List[FlowModel]:
    return session.query(FlowModel).all()


@db_session(GlobalContext.database_path())
def get_task_records_by_flow_id(
        flow_id: str,
        session: Session = None
    ) -> List[TaskModel]:
    task_records = (
        session.query(TaskModel)
        .filter(TaskModel.flow_id == flow_id)
        .all()
    )
    return task_records


@db_session(GlobalContext.database_path())
def create_scheduler_session(
        heartbeat: int,
        worker: int,
        session: Session = None
    ) -> None:
    scheduler_session_log_record = SchedulerSessionModel(worker=worker)
    session.add(scheduler_session_log_record)


@db_session(GlobalContext.database_path())
def get_schedules(
        __datetime: datetime = None,
        session: Session = None
    ) -> Dict[Path, Dict[str, Union[str, List[str]]]]:
    if __datetime is None:
        __datetime = datetime.now()

    select_columns = (
        FlowModel.path,
        FlowModel.name,
        TaskModel.name,
        TaskScheduleModel.id,
    )
    schedule_record_proxy = (
        session.query(*select_columns)
        .join(FlowModel)
        .join(TaskModel)
        .join(FlowScheduleModel)
        .join(TaskRunModel)
        .filter(
            FlowModel.active == True,
            FlowScheduleModel.schedule_datetime <= __datetime,
            TaskRunModel.status == TaskRunStatus.PENDING.name
        )
        .execute()
    )

    flow_task_schedules = dict()
    for flow_path, flow_name, task_name, _ in schedule_record_proxy:
        if not flow_path in flow_task_schedules:
            flow_task_schedules[flow_path] = {
                'flow_name': flow_name,
                'task_names': []
            }

        flow_task_schedules[flow_path]['task_names'].append(task_name)

    schedule_record_proxy.close()

    return flow_task_schedules


@db_session(GlobalContext.log_database_path())
def copy_records_to_log(
        records: List[Union[
            FlowModel, FlowRunModel, TaskModel,
            TaskDownstreamModel, TaskScheduleModel, TaskRunModel
        ]],
        session: Session
    ) -> None:
    log_records = []
    for record in records:
        kwargs = dict()
        columns = [column.name for column in record.__table__.columns]
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

    session.add_all(log_records)
    session.commit()


@db_session(GlobalContext.log_database_path())
def iter_log_run_records(
        flow_name: str,
        start_datetime: datetime,
        end_datetime: datetime,
        session: Session = None
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
