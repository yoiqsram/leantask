import argparse
from typing import Callable

from ....utils.script import import_lib


def add_run_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'run',
        help='Run workflow.',
        description='Run workflow.'
    )
    parser.add_argument(
        'flow_name',
        help='Flow name'
    )
    parser.add_argument(
        '--run-id',
        help='Continue run based on flow run id.'
    )
    parser.add_argument(
        '--local', '-L',
        action='store_true',
        help=(
            'NOT RECOMMENDED. Run locally without using scheduler thus will not be logged. '
            'Please use this only for testing purposes.'
        )
    )
    parser.add_argument(
        '--rerun', '-R',
        action='store_true',
        help='Rerun failed or canceled run.'
    )
    parser.add_argument(
        '--force', '-F',
        action='store_true',
        help='NOT RECOMMENDED. Bypass any confirmation before run.'
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )
    parser.add_argument(
        '--log-file',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '--scheduler-session-id',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help=argparse.SUPPRESS
    )

    return run_flow


def run_flow(args: argparse.Namespace) -> None:
    from ....database import FlowModel
    from ...flow.run import run_flow

    flow_model = (
        FlowModel.select()
        .where(FlowModel.name == args.flow_name)
        .get()
    )
    flow_module = import_lib('flow', flow_model.path)
    flow = flow_module.Flow.__context__.__defined__

    run_flow(args, flow)
