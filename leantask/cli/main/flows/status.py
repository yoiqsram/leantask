import argparse
from typing import Callable


def add_status_parser(subparsers) -> Callable:
    from ...flow.status import add_status_arguments

    parser: argparse.ArgumentParser = subparsers.add_parser(
        'status',
        help='Show latest runs info.',
        description='Show latest runs info.'
    )
    parser.add_argument(
        'flow_name',
        help='Flow name'
    )
    add_status_arguments(parser)

    return status_flow


def status_flow(args: argparse.Namespace) -> None:
    from ....flow import get_flow
    from ...flow.status import show_run_statuses

    flow = get_flow(args.flow_name)
    show_run_statuses(args, flow)
