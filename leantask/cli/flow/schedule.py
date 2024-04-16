from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from typing import Callable, TYPE_CHECKING

from ...context import GlobalContext
from ...enum import FlowRunStatus, FlowScheduleStatus, TaskRunStatus
from ...logging import get_local_logger, get_logger
from ...utils.string import quote

if TYPE_CHECKING:
    from ...database import FlowScheduleModel
    from ...flow import Flow

logger = None


def add_schedule_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'schedule',
        help='schedule to queue system',
        description='schedule to queue system'
    )
    parser.add_argument(
        '--datetime', '-D',
        help='Schedule datetime.'
    )
    parser.add_argument(
        '--now', '-N',
        action='store_true',
        help='Schedule task to run now.'
    )
    parser.add_argument(
        '--force', '-F',
        action='store_true',
        help='Force add schedule even if it exists.'
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )
    parser.add_argument(
        '--log',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '--scheduler-session-id',
        help=argparse.SUPPRESS
    )

    return schedule_flow


def schedule_flow(args: argparse.Namespace, flow) -> None:
    global logger
    if args.log is not None:
        logger = get_logger('flow.schedule', args.log)
    else:
        logger = get_local_logger('flow.schedule')

    logger.info(f"Run command: {' '.join([quote(sys.executable)] + sys.argv)}")

    GlobalContext.SCHEDULER_SESSION_ID = args.scheduler_session_id

    if not flow.active:
        logger.error('Failed to set the new schedule. Flow is inactive.')
        raise SystemExit(FlowScheduleStatus.NO_SCHEDULE.value)

    if not flow._model_exists:
        logger.error(
            'Flow has not been indexed. Please index the flow using this command:\n'
            f'{sys.executable} "{flow.path}" index'
        )
        raise SystemExit(FlowScheduleStatus.FAILED.value)

    elif flow.checksum != flow._model.checksum:
        logger.error(
            'Flow has unindexed changes. Please reindex the flow using this command:\n'
            f'{sys.executable} "{flow.path}" index'
        )
        raise SystemExit(FlowScheduleStatus.FAILED.value)

    try:
        if args.now:
            logger.debug('Set schedule datetime to now.')
            schedule_datetime = datetime.now()

        elif args.datetime is not None:
            logger.debug(f"Set schedule datetime to {schedule_datetime.isoformat()}.")
            schedule_datetime = datetime.fromisoformat(args.datetime)

        else:
            logger.debug('Get next flow schedule datetime')
            schedule_datetime = flow.next_schedule_datetime()
            if schedule_datetime is None:
                logger.error('No run schedule in the future.')
                raise SystemExit(FlowScheduleStatus.NO_SCHEDULE.value)

            logger.debug(f"Flow's next schedule at {schedule_datetime.isoformat(sep=' ', timespec='minutes')}")

        if schedule_datetime is None:
            logger.error('Flow has no schedule.')
            raise SystemExit(FlowScheduleStatus.NO_SCHEDULE.value)

        with flow._model._meta.database.atomic():
            update_schedule(
                flow=flow,
                schedule_datetime=schedule_datetime,
                is_manual=args.now or args.datetime is not None,
                force=args.force
            )

    except Exception as exc:
        logger.error(f'{exc.__class__.__name__}: {exc}', exc_info=True)
        raise SystemExit(FlowScheduleStatus.FAILED.value)


def update_schedule(
        flow,
        schedule_datetime: datetime,
        is_manual: bool,
        force: bool
    ) -> None:
    from ...database import FlowRunModel, FlowScheduleModel

    logger.debug("Get flow's current schedule if exists.")
    flow_schedule_models = list(
        flow._model.flow_schedules
        .order_by(schedule_datetime)
    )

    if len(flow_schedule_models) == 0:
        logger.debug('No schedule was found.')

    for flow_schedule_model in flow_schedule_models:
        max_delay = None
        if flow_schedule_model.max_delay is not None:
            max_delay = timedelta(seconds=flow_schedule_model.max_delay)

        try:
            flow_run_model = (
                FlowRunModel.select()
                .where(FlowRunModel.flow_schedule_id == flow_schedule_model.id)
                .limit(1)
                [0]
            )

            if flow_run_model.status in (
                    FlowRunStatus.CANCELED.name, FlowRunStatus.CANCELED_BY_USER,
                    FlowRunStatus.DONE, FlowRunStatus.FAILED
                    ):
                logger.info('There was old schedule that has been executed and has not been removed.')
                logger.info('Clear current schedule.')
                flow_schedule_model.delete_instance()

            elif max_delay is None:
                logger.warning(
                    'Flow has been scheduled'
                    + (' by scheduler'
                        if flow_run_model.status == FlowRunStatus.SCHEDULED.name
                        else ' manually')
                    + ' at '
                    + repr(flow_run_model.schedule_datetime.isoformat(sep=' ', timespec='minutes'))
                    + ' and currently running. New schedule can only be set when it finish.'
                )
                continue

            elif (flow_run_model.schedule_datetime + max_delay) <= datetime.now():
                logger.warning(
                    'Flow has been scheduled'
                    + (' by scheduler'
                        if flow_run_model.status == FlowRunStatus.SCHEDULED.name
                        else ' manually')
                    + ' at '
                    + repr(flow_run_model.schedule_datetime.isoformat(sep=' ', timespec='minutes'))
                    + f' and currently running. '
                    + f'New schedule can only be set after passing its max delay of {flow_run_model.max_delay}s.'
                )
                for task_run_model in flow_run_model.task_runs:
                    task_run_model.status = TaskRunStatus.FAILED_TIMEOUT_DELAY
                    task_run_model.save()
                flow_run_model.status == FlowRunStatus.FAILED_TIMEOUT_DELAY
                flow_run_model.save()
                flow_schedule_model.delete_instance()

        except IndexError:
            if not force:
                logger.warning(
                    'Flow has been scheduled at '
                    + repr(flow_schedule_model.schedule_datetime.isoformat(sep=' ', timespec='minutes'))
                    + f' and has not been running'
                    + ('nor passing its max delay of {flow_schedule_model.max_delay}s.'
                        if flow_schedule_model.max_delay is not None
                        else '.')
                )
                continue

            logger.info('Remove existing schedule.')
            flow_schedule_model.delete_instance()

    if not force and len(flow_schedule_models) > 0:
        logger.error(
            f"Failed to set the new schedule. There's already {len(flow_schedule_models)} schedule(s) exists."
        )
        raise SystemExit(FlowScheduleStatus.FAILED_SCHEDULE_EXISTS.value)

    logger.debug('Add new schedule to database.')
    flow_schedule_model = FlowScheduleModel(
        flow=flow.id,
        schedule_datetime=schedule_datetime,
        max_delay=flow.max_delay,
        is_manual=is_manual
    )
    flow_schedule_model.save(force_insert=True)

    create_new_flow_run(flow, flow_schedule_model)

    logger.info(
        f"Successfully added a schedule at {schedule_datetime.isoformat(sep=' ', timespec='minutes')}."
    )


def create_new_flow_run(
        flow: Flow,
        flow_schedule_model: FlowScheduleModel,
    ) -> None:
    from ...flow import FlowRun

    logger.debug('Create new scheduled flow run.')
    is_manual = flow_schedule_model.is_manual

    flow_run = FlowRun(
        flow,
        is_manual=is_manual,
        status=FlowRunStatus.SCHEDULED if not is_manual else FlowRunStatus.SCHEDULED_BY_USER,
        schedule_id=flow_schedule_model.id,
        schedule_datetime=flow_schedule_model.schedule_datetime
    )
    flow_run.create_task_runs(TaskRunStatus.SCHEDULED)

    flow_run.save()
