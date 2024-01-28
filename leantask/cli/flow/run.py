import argparse
from typing import Callable

from ...context import GlobalContext
from ...enum import FlowRunStatus
from ...flow import FlowRun
from ...utils.cache import load_cache
from ...utils.script import get_confirmation


def add_run_parser(subparsers) -> Callable:
    parser = subparsers.add_parser(
        'run',
        help='Run flow now.',
        description='Run flow now.'
    )
    parser.add_argument(
        '--cache',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '--local', '-L',
        action='store_true',
        help=(
            'Run locally without using scheduler and will not be logged. '
            'Not Recommended. Please use this for testing purposes only.'
        )
    )
    parser.add_argument(
        '--force', '-F',
        action='store_true',
        help='Bypass any confirmation before run. Not recommended.'
    )
    parser.add_argument(
        '--verbose', '-V',
        action='store_true',
        help='Show run log.'
    )

    return run_flow


def run_flow(args: argparse.Namespace, flow) -> None:
    GlobalContext.LOCAL_RUN = args.local

    flow_run = None
    if args.cache is not None:
        cache = load_cache(args.cache)

        if not args.force and not flow.active:
            print('Flow is currently inactive.')
            raise SystemExit(FlowRunStatus.CANCELED)

        flow_run = FlowRun(
            flow,
            schedule_datetime=cache['flow']['schedule_datetime'],
            schedule_id=cache['flow']['schedule_id'],
            status=cache['flow_run']['status']
        )
        for task in flow_run.tasks_ordered:
            flow_run.create_task_run(
                task,
                status=cache['task_runs'][task]['status']
            )

    elif not args.force \
            and not get_confirmation(
                "It's always be better to schedule flow and let the scheduler to run flow.\n"
                'Are you sure you want to run it manually?',
                default=False
            ):
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    elif not args.force \
            and not flow.active \
            and not get_confirmation(
                'Flow is currently inactive.\n'
                'Are you sure you want to run it?',
                default=False
            ):
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    try:
        flow_run = flow.run(flow_run, verbose=args.verbose)
        raise SystemExit(flow_run.status.value)

    except Exception as exc:
        raise exc
        print(f'{exc.__class__.__name__}: {exc}')
        raise SystemExit(FlowRunStatus.FAILED.value)
