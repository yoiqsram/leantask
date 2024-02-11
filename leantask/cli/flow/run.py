import argparse
import sys
from typing import Callable

from ...context import GlobalContext
from ...enum import FlowRunStatus
from ...logging import get_local_logger, get_logger
from ...utils.cache import load_cache
from ...utils.script import get_confirmation
from ...utils.string import quote

logger = None


def add_run_parser(subparsers) -> Callable:
    parser = subparsers.add_parser(
        'run',
        help='run flow',
        description='run flow'
    )
    parser.add_argument(
        '--cache',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '--local', '-L',
        action='store_true',
        help=(
            'NOT RECOMMENDED. Run locally without using scheduler thus will not be logged. '
            'Please use this only for testing purposes.'
        )
    )
    parser.add_argument(
        '--force', '-F',
        action='store_true',
        help='NOT RECOMMENDED. Bypass any confirmation before run.'
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )
    parser.add_argument(
        '--log-file',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '--scheduler-session-id',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help=argparse.SUPPRESS
    )

    return run_flow


def run_flow(args: argparse.Namespace, flow) -> None:
    global logger
    if args.log_file is not None:
        logger = get_logger('flow.run', args.log_file)
    else:
        logger = get_local_logger('flow.run')

    logger.info(f'''Run command: {' '.join([quote(sys.executable)] + sys.argv)}''')

    GlobalContext.LOCAL_RUN = args.local
    GlobalContext.SCHEDULER_SESSION_ID = args.scheduler_session_id

    if args.cache is not None:
        logger.debug('Prepare flow run using the provided cache.')
        if not args.force and not flow.active:
            logger.error('Flow is currently inactive.')
            raise SystemExit(FlowRunStatus.CANCELED.value)

        flow_run_cache = load_cache(args.cache)

        if flow_run_cache['name'] != flow.name \
                or flow_run_cache['checksum'] != flow.checksum:
            logger.error('Flow from cache is different with the current flow.')
            raise SystemExit(FlowRunStatus.UNKNOWN.value)

        logger.debug('Add flow run using cache.')
        flow.add_run_from_cache(flow_run_cache)

    elif not args.force \
            and not args.local:
        if not get_confirmation(
                "It's always be better to schedule flow and let the scheduler to run flow.\n"
                'Are you sure you want to run it manually?',
                default=False
            ):
            logger.debug('User reject to run the flow manually.')
            raise SystemExit(FlowRunStatus.UNKNOWN.value)

        logger.debug('User confirmed to run the flow manually.')

        if not flow.active:
            if get_confirmation(
                    'Flow is currently inactive.\n'
                    'Are you sure you want to run it?',
                    default=False
                ):
                logger.debug('User reject to run the flow that is currently inactive.')
                raise SystemExit(FlowRunStatus.UNKNOWN.value)

            logger.debug('User confirmed to run the flow that is currently inactive.')

    if args.cache is None:
        prepare_flow_for_manual_run(flow)

    try:
        flow_run = flow.run()
        raise SystemExit(flow_run.status.value)

    except Exception as exc:
        logger.error(f'{exc.__class__.__name__}: {exc}', exc_info=True)
        raise SystemExit(FlowRunStatus.FAILED.value)


def prepare_flow_for_manual_run(flow):
    from ...database.execute import get_flow_record_by_name, get_task_records_by_flow_id
    from ...database.orm import NoResultFound

    logger.debug('Prepare flow run for manual run.')

    flow_record = None
    try:
        logger.debug('Get flow record from database.')
        flow_record = get_flow_record_by_name(flow.name)
    except NoResultFound:
        logger.error(
            'Flow has not been indexed. Use this command to index the flow:\n'
            f'"{sys.executable}" "{flow.path}" index'
        )
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    if flow.checksum != flow_record.checksum:
        logger.error(
            'Flow has been changed from the last time indexed at '
            + flow_record.modified_datetime.isoformat(sep=' ', timespec='minutes') + '.\n'
            + 'Use this command to reindex the flow:\n'
            + f'{sys.executable} "{flow.path}" index'
        )
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    logger.debug('Add flow and task reference from database.')
    flow.id = flow_record.id
    task_ids = {
        task_record.name: task_record.id
        for task_record in get_task_records_by_flow_id(flow.id)
    }
    for task in flow.tasks:
        task.id = task_ids[task.name]
