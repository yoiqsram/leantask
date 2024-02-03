from argparse import Namespace
from datetime import datetime, timedelta
from typing import Callable

from ...context import GlobalContext
from ...enum import FlowScheduleStatus
from ...utils.string import generate_uuid


def add_schedule_parser(subparsers) -> Callable:
    parser = subparsers.add_parser(
        'schedule',
        help='schedule to queue system',
        description='schedule to queue system'
    )
    parser.add_argument(
        '--now', '-N',
        action='store_true',
        help='schedule task to run now'
    )

    return schedule_flow


def schedule_flow(args: Namespace, flow) -> None:
    from ...database.execute import get_flow_record
    from ...database.orm import open_db_session, NoResultFound

    if not flow.active:
        print('Failed to set the new schedule. Flow is inactive.')
        raise SystemExit(FlowScheduleStatus.FAILED_SCHEDULE_EXISTS.value)

    try:
        with open_db_session(GlobalContext.database_path()) as session:
            try:
                flow_record = get_flow_record(flow.name, session=session)

            except NoResultFound:
                print('Flow has not been indexed. Please index the flow using this command:', end='\n\n')
                print('python', flow.path.resolve(), 'index', '--project-dir', GlobalContext.PROJECT_DIR)
                raise SystemExit(FlowScheduleStatus.FAILED.value)

            if args.now:
                schedule_datetime = datetime.now()
            else:
                schedule_datetime = flow.next_schedule_datetime()

            if schedule_datetime is None:
                print('Flow has no schedule.')
                raise SystemExit(FlowScheduleStatus.NO_SCHEDULE.value)

            update_schedule_to_db(
                session,
                flow_record=flow_record,
                schedule_datetime=schedule_datetime,
                is_manual=args.now
            )

    except Exception as exc:
        print(f'{exc.__class__.__name__}: {exc}')
        raise SystemExit(FlowScheduleStatus.FAILED.value)


def update_schedule_to_db(
        session,
        flow_record,
        schedule_datetime: datetime,
        is_manual: bool
    ) -> None:
    from ...database.execute import copy_records_to_log, get_task_records_by_flow_id
    from ...database.models import FlowScheduleModel, FlowRunModel, TaskRunModel
    from ...database.orm import NoResultFound
    from ...enum import FlowRunStatus, TaskRunStatus

    try:
        flow_schedule_record, flow_run_record = (
            session.query(FlowScheduleModel, FlowRunModel)
            .join(FlowRunModel, FlowRunModel.flow_schedule_id == FlowScheduleModel.id)
            .filter(FlowScheduleModel.flow_id == flow_record.id)
            .one()
        )

        max_delay = timedelta(seconds=flow_record.max_delay if flow_record.max_delay is not None else 0)
        if (flow_schedule_record.schedule_datetime + max_delay) <= datetime.now():
            if flow_run_record.status in (
                    FlowRunStatus.SCHEDULED.name, FlowRunStatus.SCHEDULED_BY_USER.name,
                    FlowRunStatus.RUNNING.name, FlowRunStatus.UNKNOWN.name
                    ):
                print(
                    'Failed to set the new schedule. The flow has been scheduled',
                    'by scheduler'
                        if flow_run_record.status == FlowRunStatus.SCHEDULED.name
                        else 'manually',
                    'at',
                    repr(flow_schedule_record.schedule_datetime.isoformat(sep=' ', timespec='minutes')),
                    f'and has not been passing its max delay of {flow_record.max_delay} s.'
                        if flow_record.max_delay is not None \
                        else 'and new schedule only can be set when it finish.'
                )

                raise SystemExit(FlowScheduleStatus.FAILED_SCHEDULE_EXISTS.value)

            print('Delete existing schedule.')
            session.delete(flow_schedule_record)
            session.commit()

    except NoResultFound:
        print('No result')

    flow_schedule_record = FlowScheduleModel(
        id=generate_uuid(),
        flow_id=flow_record.id,
        schedule_datetime=schedule_datetime,
        is_manual=is_manual
    )
    session.add(flow_schedule_record)

    task_run_records = []
    for task_record in get_task_records_by_flow_id(flow_record.id, session=session):
        task_run_records.append(TaskRunModel(task_id=task_record.id, attempt=1, status=TaskRunStatus.PENDING.name))

    flow_run_record = FlowRunModel(
        flow_id=flow_record.id,
        schedule_datetime=schedule_datetime,
        status=FlowRunStatus.SCHEDULED_BY_USER.name if is_manual else FlowRunStatus.SCHEDULED.name,
        flow_schedule_id=flow_schedule_record.id,
        task_runs=task_run_records
    )
    session.add(flow_run_record)
    session.commit()

    copy_records_to_log([flow_run_record])

    print(
        'Successfully added a schedule at',
        schedule_datetime.isoformat(sep=' ', timespec='minutes'),
        end='.\n'
    )
