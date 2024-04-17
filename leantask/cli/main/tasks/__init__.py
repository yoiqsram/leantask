import argparse
from typing import Callable

from .log import add_log_parser


def add_tasks_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'tasks',
        help='Tasks command.',
        description='Tasks command.'
    )
    subparsers = parser.add_subparsers(
        dest='tasks_command',
        required=True,
        help='Command to run.'
    )

    command_runners = {
        'log': add_log_parser(subparsers)
    }
    return command_runners
