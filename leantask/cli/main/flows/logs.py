import argparse
from typing import Callable

from ....utils.script import import_lib


def add_logs_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'logs',
        help='Show run logs.',
        description='Show run logs.'
    )
    parser.add_argument(
        'flow_name',
        help='Flow name'
    )
    option = parser.add_subparsers(
        dest='option',
        required=True,
        help='log options'
    )

    list_option = option.add_parser(
        'list',
        help='Show list of log files from the latest runs.'
    )
    list_option.add_argument(
        '--limit', '-l',
        default=10,
        type=int,
        help='Maximum number of logs to be shown.'
    )
    list_option.add_argument(
        '--full',
        action='store_true',
        help='Show full absolute path instead of relative path from project dir.'
    )
    list_option.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )

    search_option = option.add_parser(
        'search',
        help='Search run log.'
    )
    search_option.add_argument(
        'keyword',
        help='Search keyword. Default for flow run id.'
    )
    search_option.add_argument(
        '--by-task',
        action='store_true',
        help='Enable to search by task run id.'
    )
    search_option.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )

    inspect_option = option.add_parser(
        'inspect',
        help='Show list of log files from the latest runs.'
    )
    inspect_option.add_argument(
        '--last', '-L',
        default=1,
        type=int,
        help="Open the latest 'N' log files."
    )
    inspect_option.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )

    return logs_flow


def logs_flow(args: argparse.Namespace) -> None:
    from ....database import FlowModel
    from ...flow.logs import show_logs

    flow_model = (
        FlowModel.select()
        .where(FlowModel.name == args.flow_name)
        .get()
    )
    flow_module = import_lib('flow', flow_model.path)
    flow = flow_module.Flow.__context__.__defined__

    show_logs(args, flow)
