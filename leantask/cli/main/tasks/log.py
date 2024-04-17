import argparse
from typing import Callable

from ....utils.script import import_lib


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
        'task_name',
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
        '--attempt', '-A',
        type=int,
        default=1,
        help='Filter by attempt.'
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )

    return show_task_log


def show_task_log(args: argparse.Namespace) -> None:
    from ....database import FlowModel
    from ...flow.tasks.log import show_task_log

    flow_model = (
        FlowModel.select()
        .where(FlowModel.name == args.flow_name)
        .get()
    )
    flow_module = import_lib('flow', flow_model.path)
    flow = flow_module.Flow.__context__.__defined__

    show_task_log(args, flow)
