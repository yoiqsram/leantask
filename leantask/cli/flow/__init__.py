import argparse
from pathlib import Path
from typing import Callable, Dict, Tuple

from ...context import GlobalContext
from ...flow import Flow
from ...utils.script import is_main_script
from .index import add_index_parser
from .info import add_info_parser
from .logs import add_logs_parser
from .run import add_run_parser
from .runs import add_runs_parser
from .schedule import add_schedule_parser


def parse_args(
        description: str
    ) -> Tuple[argparse.Namespace, Dict[str, Callable]]:
    parser = argparse.ArgumentParser(description=description)
    subparsers = parser.add_subparsers(
        dest='command',
        required=True,
        help='Command to run.'
    )

    command_runners = {
        'info': add_info_parser(subparsers),
        'run': add_run_parser(subparsers),
        'index': add_index_parser(subparsers),
        'schedule': add_schedule_parser(subparsers),
        'runs': add_runs_parser(subparsers),
        'logs': add_logs_parser(subparsers)
    }
    return parser.parse_known_args(), command_runners


def run_cli(flow: Flow) -> None:
    if is_main_script():
        return

    if len(flow.tasks) == 0:
        raise ValueError('Flow must contain at least one task.')

    (args, _), command_runners = parse_args(description=flow.description)

    if 'project_dir' in args and args.project_dir is not None:
        GlobalContext.set_project_dir(Path(args.project_dir).resolve())

    if 'debug' in args and args.debug:
        GlobalContext.LOG_DEBUG = True

    command_runners[args.command](args, flow)
