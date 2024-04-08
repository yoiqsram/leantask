import argparse
from typing import Callable

from ....utils.script import import_lib


def add_schedule_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'schedule',
        help='schedule to queue system',
        description='schedule to queue system'
    )
    parser.add_argument(
        'flow_name',
        help='Flow name'
    )
    parser.add_argument(
        '--datetime', '-D',
        help='Schedule datetime.'
    )
    parser.add_argument(
        '--now', '-N',
        action='store_true',
        help='Schedule task to run now.'
    )
    parser.add_argument(
        '--force', '-F',
        action='store_true',
        help='Force add schedule even if it exists.'
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

    return schedule_flow


def schedule_flow(args: argparse.Namespace) -> None:
    from ....database import FlowModel
    from ...flow.schedule import schedule_flow

    flow_model = (
        FlowModel.select()
        .where(FlowModel.name == args.flow_name)
        .get()
    )
    flow_module = import_lib('flow', flow_model.path)
    flow = flow_module.Flow.__context__.__defined__

    schedule_flow(args, flow)
