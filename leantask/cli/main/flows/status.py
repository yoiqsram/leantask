import argparse
from typing import Callable


def add_status_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'status',
        help='Show latest runs info.',
        description='Show latest runs info.'
    )
    parser.add_argument(
        'flow_name',
        help='Flow name'
    )
    parser.add_argument(
        '--limit', '-l',
        default=15,
        type=int,
        help='Maximum number of runs info to be shown.'
    )
    parser.add_argument(
        '--tasks', '-T',
        action='store_true',
        help='Show all task stasuses.'
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
        '--status', '-S',
        help='Filter by run status.'
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )

    return status_flow


def status_flow(args: argparse.Namespace) -> None:
    from ....flow import get_flow
    from ...flow.status import show_run_statuses

    flow = get_flow(args.flow_name)
    show_run_statuses(args, flow)
