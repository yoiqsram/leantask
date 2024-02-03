from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, List, Union

from ..enum import TaskRunStatus
from ..context import GlobalContext
from ..utils.string import generate_uuid
from .models import (
    FlowModel, FlowScheduleModel, FlowRunModel,
    TaskModel, TaskDownstreamModel, TaskRunModel,
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


@db_session(GlobalContext.log_database_path())
def create_scheduler_session(
        heartbeat: int,
        worker: int,
        session: Session = None
    ) -> None:
    session_id = generate_uuid()

    log_dir = GlobalContext.log_dir()
    if not log_dir.is_dir():
        log_dir.mkdir(parents=True)

    scheduler_session_record = SchedulerSessionModel(
        id=session_id,
        heartbeat=heartbeat,
        worker=worker,
        log_path=str(log_dir / (session_id + '.log'))
    )
    session.add(scheduler_session_record)
    session.commit()

    GlobalContext.set_scheduler_session_id(scheduler_session_record.id)


@db_session(GlobalContext.database_path())
def get_scheduled_run_tasks(
        __datetime: datetime = None,
        session: Session = None
    ) -> Dict[Path, Dict[str, Union[str, List[str]]]]:
    if __datetime is None:
        __datetime = datetime.now()

    select_columns = (
        FlowModel.path,
        FlowModel.name,
        FlowModel.checksum,
        FlowScheduleModel.id,
        FlowScheduleModel.schedule_datetime,
        FlowRunModel.id,
        FlowRunModel.status,
        TaskModel.name,
        # TaskScheduleModel.id,
        TaskRunModel.id,
        TaskRunModel.status,
        TaskRunModel.attempt
    )
    schedule_records = (
        session.query(*select_columns)
        .select_from(FlowModel)
        .join(FlowModel.flow_schedule)
        .join(FlowModel.flow_runs)
        .join(FlowModel.tasks)
        .join(TaskModel.task_schedules)
        .join(TaskModel.task_runs)
        .filter(
            FlowModel.active == True,
            FlowScheduleModel.schedule_datetime <= __datetime,
            TaskRunModel.status == TaskRunStatus.PENDING.name
        )
        .all()
    )

    flow_task_schedules = dict()
    for (path, name, checksum, schedule_id, schedule_datetime, run_id, run_status,
            task_name, task_schedule_id, task_run_id, task_run_status, task_attempt
            ) in schedule_records:
        path = Path(path)
        if path not in flow_task_schedules:
            flow_task_schedules[path] = {
                'name': name,
                'checksum': checksum,
                'schedule_id': schedule_id,
                'schedule_datetime': schedule_datetime,
                'id': run_id,
                'status': run_status,
                'tasks': dict()
            }

        flow_task_schedules[path]['tasks'][task_name] = {
            'schedule_id': task_schedule_id,
            'id': task_run_id,
            'attempt': task_attempt,
            'status': task_run_status
        }

    return flow_task_schedules


@db_session(GlobalContext.log_database_path())
def copy_records_to_log(
        records: List[Union[
            FlowModel, FlowRunModel, TaskModel,
            TaskDownstreamModel, TaskRunModel
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
