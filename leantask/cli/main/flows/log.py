import argparse
from typing import Callable


def add_log_parser(subparsers) -> Callable:
    from ...flow.log import add_log_arguments

    parser: argparse.ArgumentParser = subparsers.add_parser(
        'log',
        help='Show log of a run.',
        description='Show log of a run.'
    )
    parser.add_argument(
        'flow_name',
        help='Flow name'
    )
    add_log_arguments(parser)

    return logs_flow


def logs_flow(args: argparse.Namespace) -> None:
    from ....flow import get_flow
    from ...flow.log import show_log

    flow = get_flow(args.flow_name)
    show_log(args, flow)
