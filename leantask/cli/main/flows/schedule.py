import argparse
from typing import Callable


def add_schedule_parser(subparsers) -> Callable:
    from ...flow.schedule import add_schedule_arguments

    parser: argparse.ArgumentParser = subparsers.add_parser(
        'schedule',
        help='schedule to queue system',
        description='schedule to queue system'
    )
    parser.add_argument(
        'flow_name',
        help='Flow name'
    )
    add_schedule_arguments(parser)

    return schedule_flow


def schedule_flow(args: argparse.Namespace) -> None:
    from ....flow import get_flow
    from ...flow.schedule import schedule_flow

    flow = get_flow(args.flow_name)
    schedule_flow(args, flow)
