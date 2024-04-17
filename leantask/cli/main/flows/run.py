import argparse
from typing import Callable


def add_run_parser(subparsers) -> Callable:
    from ...flow.run import add_run_arguments

    parser: argparse.ArgumentParser = subparsers.add_parser(
        'run',
        help='Run workflow.',
        description='Run workflow.'
    )
    parser.add_argument(
        'flow_name',
        help='Flow name'
    )
    add_run_arguments(parser)

    return run_flow


def run_flow(args: argparse.Namespace) -> None:
    from ....flow import get_flow
    from ...flow.run import run_flow

    flow = get_flow(args.flow_name)
    run_flow(args, flow)
