from datetime import datetime
from pathlib import Path
from sqlalchemy import desc, func
from typing import Dict, Generator, List, Union

from ..enum import TaskRunStatus
from ..context import GlobalContext
from .models import (
    FlowModel, FlowScheduleModel, FlowRunModel,
    TaskModel, TaskDownstreamModel, TaskScheduleModel, TaskRunModel
)
from .models.log import (
    FlowLogModel, FlowRunLogModel,
    TaskLogModel, TaskDownstreamLogModel, TaskRunLogModel,
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
def add_records_to_log(
        records: List[Union[
            FlowModel, FlowRunModel, TaskModel,
            TaskDownstreamModel, TaskScheduleModel, TaskRunModel
        ]],
        session: Session
    ) -> None:
    for record in records:
        attrs = dict()
        record_attrs = dir(record)
        for attr in record_attrs:
            if attr == 'id':
                attrs[f'main_{record.__tablename__[:-1]}_id'] = getattr(record, attr)

            elif attr == 'modified_datetime':
                attrs['created_datetime'] = getattr(record, attr)

            elif attr == 'created_datetime':
                if 'modified_datetime' not in record_attrs:
                    attrs[attr] = getattr(record, attr)

            elif not attr.startswith('_') or not attr == 'registry':
                attrs[attr] = getattr(record, attr)

        log_model_name = record.__class__.__name__.split('Model')[0] + 'LogModel'
        log_flow_record = eval(log_model_name)(**attrs)
        session.add(log_flow_record)

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
