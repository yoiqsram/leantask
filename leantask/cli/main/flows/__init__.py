import argparse
from typing import Callable

from .discover import add_discover_parser
from .list import add_list_parser
from .log import add_log_parser
from .run import add_run_parser
from .schedule import add_schedule_parser
from .status import add_status_parser


def add_flows_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'flows',
        help='Flows command.',
        description='Flows command.'
    )
    subparsers = parser.add_subparsers(
        dest='flows_command',
        required=True,
        help='Command to run.'
    )

    command_runners = {
        'discover': add_discover_parser(subparsers),
        'list': add_list_parser(subparsers),
        'log': add_log_parser(subparsers),
        'run': add_run_parser(subparsers),
        'schedule': add_schedule_parser(subparsers),
        'status': add_status_parser(subparsers)
    }

    return command_runners
