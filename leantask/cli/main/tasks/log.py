import argparse
from typing import Callable


def add_log_parser(subparsers) -> Callable:
    from ...flow.tasks.log import add_task_log_arguments

    parser: argparse.ArgumentParser = subparsers.add_parser(
        'log',
        help='Show log of a run.',
        description='Show log of a run.'
    )
    parser.add_argument(
        'flow_name',
        help='Flow name'
    )
    parser.add_argument(
        'task_name',
        help='Flow name'
    )
    add_task_log_arguments(parser)

    return show_task_log


def show_task_log(args: argparse.Namespace) -> None:
    from ....flow import get_flow
    from ...flow.tasks.log import show_task_log

    flow = get_flow(args.flow_name)
    show_task_log(args, flow)
