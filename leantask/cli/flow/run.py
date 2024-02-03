import argparse
import sys
from typing import Callable

from ...context import GlobalContext
from ...enum import FlowRunStatus
from ...logging import get_logger
from ...utils.script import get_confirmation

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
        '--verbose', '-V',
        action='store_true',
        help='show run log'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help=argparse.SUPPRESS
    )

    return run_flow


def run_flow(args: argparse.Namespace, flow) -> None:
    global logger
    logger = get_logger('cli.flow.run')

    GlobalContext.LOCAL_RUN = args.local

    if args.cache is not None:
        logger.debug('Prepare flow run using the provided cache.')
        if not args.force and not flow.active:
            logger.error('Flow is currently inactive.')
            raise SystemExit(FlowRunStatus.CANCELED.value)

        flow_run_cache = args.cache['flow']

        if flow_run_cache['name'] != flow.name \
                or flow_run_cache['checksum'] != flow.checksum:
            logger.error('Flow from cache is different with the current flow.')
            raise SystemExit(FlowRunStatus.UNKNOWN.value)

        logger.debug('Add flow run using cache.')
        flow.add_run_from_cache(flow_run_cache)

    elif not args.force \
            and not args.local \
            and not get_confirmation(
                "It's always be better to schedule flow and let the scheduler to run flow.\n"
                'Are you sure you want to run it manually?',
                default=False
            ):
        logger.debug('User reject to run the flow manually.')
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    elif not args.force \
            and not args.local \
            and not flow.active \
            and not get_confirmation(
                'Flow is currently inactive.\n'
                'Are you sure you want to run it?',
                default=False
            ):
        logger.debug('User reject to run the flow that is currently inactive.')
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    elif not args.local:
        prepare_flow_for_manual_run(flow)

    try:
        flow_run = flow.run(verbose=args.verbose)
        raise SystemExit(flow_run.status.value)

    except Exception as exc:
        logger.error(f'{exc.__class__.__name__}: {exc}')
        raise SystemExit(FlowRunStatus.FAILED.value)


def prepare_flow_for_manual_run(flow):
    from ...database.execute import get_flow_record, get_task_records_by_flow_id
    from ...database.orm import NoResultFound

    logger.debug('Prepare flow run for manual run.')

    flow_record = None
    try:
        logger.debug('Get flow record from database.')
        flow_record = get_flow_record(flow.name)
    except NoResultFound:
        logger.error(
            'Flow has not been indexed. Use this command to index the flow:\n'
            f'{sys.executable} "{flow.path}" index'
        )
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    if flow.checksum != flow_record.checksum:
        logger.error(
            'Flow has been changed from the last time indexed at',
            flow.modified_datetime.isoformat(sep=' ', timespec='minutes') + '.',
            'Use this command to reindex the flow:\n',
            f'{sys.executable} "{flow.path}" index'
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
