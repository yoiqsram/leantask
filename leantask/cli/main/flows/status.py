import argparse
from typing import Callable

from ....utils.script import import_lib

LIMIT = 10

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
    option = parser.add_subparsers(
        dest='option',
        help='runs options'
    )

    list_option = option.add_parser(
        'list',
        help='Show list of latest runs info.'
    )
    list_option.add_argument(
        '--limit', '-l',
        default=LIMIT,
        type=int,
        help='Maximum number of runs info to be shown.'
    )
    list_option.add_argument(
        '--status', '-S',
        help='Filter run status.'
    )
    list_option.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )

    search_option = option.add_parser(
        'search',
        help='Search run info.'
    )
    search_option.add_argument(
        'keyword',
        help='Search keyword. Default to flow run id.'
    )
    search_option.add_argument(
        '--by-task',
        action='store_true',
        help='Enable to search by task run id.'
    )
    search_option.add_argument(
        '--by-datetime',
        action='store_true',
        help=(
            'Enable to search using datetime (ISO format). '
            'Example: 2024-02-22 or 2024-02-22T10:00'
        )
    )

    return status_flow


def status_flow(args: argparse.Namespace) -> None:
    from ....database import FlowModel
    from ...flow.status import show_runs

    flow_model = (
        FlowModel.select()
        .where(FlowModel.name == args.flow_name)
        .get()
    )
    flow_module = import_lib('flow', flow_model.path)
    flow = flow_module.Flow.__context__.__defined__

    show_runs(args, flow)
