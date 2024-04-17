import argparse
from typing import Callable


def add_log_parser(subparsers) -> Callable:
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
        '--run-id', '-I',
        help='Filter by run id.'
    )
    parser.add_argument(
        '--datetime', '-D',
        help=(
            'Filter by run datetime (ISO format). '
            'Example: 2024-02-22 or 2024-02-22T10:00'
        )
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )

    return logs_flow


def logs_flow(args: argparse.Namespace) -> None:
    from ....flow import get_flow
    from ...flow.log import show_log

    flow = get_flow(args.flow_name)
    show_log(args, flow)
