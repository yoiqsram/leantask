import argparse
import sys
from typing import Callable

from ...context import GlobalContext
from ...enum import FlowRunStatus
from ...flow import FlowRun
from ...utils.cache import load_cache
from ...utils.script import get_confirmation


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

    return run_flow


def run_flow(args: argparse.Namespace, flow) -> None:
    GlobalContext.LOCAL_RUN = args.local

    flow_run = None

    if args.cache is not None:
        if not args.force and not flow.active:
            print('Flow is currently inactive.')
            raise SystemExit(FlowRunStatus.CANCELED)

        cache = load_cache(args.cache)
        flow_run = create_flow_run_for_cache_run(flow, cache)

    elif not args.force \
            and not args.local \
            and not get_confirmation(
                "It's always be better to schedule flow and let the scheduler to run flow.\n"
                'Are you sure you want to run it manually?',
                default=False
            ):
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    elif not args.force \
            and not args.local \
            and not flow.active \
            and not get_confirmation(
                'Flow is currently inactive.\n'
                'Are you sure you want to run it?',
                default=False
            ):
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    elif not args.local:
        prepare_flow_for_manual_run(flow)

    try:
        flow_run = flow.run(flow_run, verbose=args.verbose)
        raise SystemExit(flow_run.status.value)

    except Exception as exc:
        print(f'{exc.__class__.__name__}: {exc}')
        raise SystemExit(FlowRunStatus.FAILED.value)


def create_flow_run_for_cache_run(flow, cache):
    flow_run = FlowRun(
        flow,
        schedule_datetime=cache['flow']['schedule_datetime'],
        __schedule_id=cache['flow']['schedule_id'],
        status=cache['flow_run']['status']
    )
    for task in flow_run.tasks_ordered:
        flow_run.create_task_run(
            task,
            status=cache['task_runs'][task]['status']
        )

    return flow_run


def prepare_flow_for_manual_run(flow):
    from ...database.execute import get_flow_record, get_task_records_by_flow_id
    from ...database.orm import NoResultFound

    flow_record = None
    try:
        flow_record = get_flow_record(flow.name)
    except NoResultFound:
        print(
            'Flow has not been indexed. Use this command to index the flow:\n',
            f'{sys.executable} "{flow.path}" index'
        )
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    if flow.checksum != flow_record.checksum:
        print(
            'Flow has been changed from the last time indexed at',
            flow.modified_datetime.isoformat(sep=' ', timespec='minutes') + '.',
            'Use this command to reindex the flow:\n',
            f'{sys.executable} "{flow.path}" index'
        )
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    flow.id = flow_record.id

    task_ids = {
        task_record.name: task_record.id
        for task_record in get_task_records_by_flow_id(flow.id)
    }
    for task in flow.tasks:
        task.id = task_ids[task.name]
