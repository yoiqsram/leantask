import argparse
from typing import Callable


def add_schedule_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'schedule',
        help='schedule to queue system',
        description='schedule to queue system'
    )
    parser.add_argument(
        'flow_name',
        help='Flow name'
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


def schedule_flow(args: argparse.Namespace) -> None:
    from ....flow import get_flow
    from ...flow.schedule import schedule_flow

    flow = get_flow(args.flow_name)
    schedule_flow(args, flow)
