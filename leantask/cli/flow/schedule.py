from argparse import Namespace
from datetime import datetime, timedelta
from typing import Callable

from ...context import GlobalContext
from ...enum import FlowScheduleStatus


def add_schedule_parser(subparsers) -> Callable:
    parser = subparsers.add_parser(
        'schedule',
        help='Schedule to queue system.',
        description='Schedule to queue system.'
    )
    parser.add_argument(
        '--now', '-N',
        action='store_true',
        help='Schedule task to run now.'
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
                flow_record = get_flow_record(flow.name, session)

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
                schedule_datetime=flow.next_schedule_datetime()
            )

    except Exception as exc:
        print(type(exc), exc)
        raise SystemExit(FlowScheduleStatus.FAILED.value)


def update_schedule_to_db(
        session,
        flow_record,
        schedule_datetime: datetime
    ) -> None:
    from sqlalchemy import func
    from ...database.execute import add_records_to_log, get_task_records_by_flow_id
    from ...database.models import FlowScheduleModel, TaskScheduleModel

    old_schedule_datetime = (
        session.query(func.min(FlowScheduleModel.schedule_datetime))
        .filter(FlowScheduleModel.id == flow_record._id)
        .scalar()
    )
    if old_schedule_datetime is not None:
        max_delay = timedelta(seconds=flow_record.max_delay if flow_record.max_delay is not None else 0)
        if (old_schedule_datetime + max_delay) <= schedule_datetime:
            print(
                'Failed to set the new schedule. The flow has been scheduled at',
                repr(old_schedule_datetime.isoformat(sep=' ', timespec='minutes')),
                f'and has not been passing its max delay of {flow_record.max_delay} s.' \
                    if flow_record.max_delay is not None \
                    else 'and new schedule only can be set when it finish.',
                end='.\n'
            )
            raise SystemExit(FlowScheduleStatus.FAILED_SCHEDULE_EXISTS.value)

        flow_schedule_query = (
            session.query(FlowScheduleModel)
            .filter(FlowScheduleModel.flow_id == flow_record.id)
        )
        flow_schedule_query.delete()

    task_schedule_records = [
        TaskScheduleModel(task_id=task_record.id)
        for task_record in get_task_records_by_flow_id(flow_record.id, session)
    ]
    flow_schedule_record = FlowScheduleModel(
        flow_id=flow_record.id,
        schedule_datetime=schedule_datetime,
        task_schedules=task_schedule_records
    )
    session.add(flow_schedule_record)
    session.commit()

    add_records_to_log([flow_schedule_record] + task_schedule_records)

    print(
        'Successfully added a schedule at',
        schedule_datetime.isoformat(sep=' ', timespec='minutes'),
        end='.\n'
    )
