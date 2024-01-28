from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Callable, Dict, Tuple

from ...context import GlobalContext
from ...utils.script import is_main_script
from .index import add_index_parser
from .info import add_info_parser
from .run import add_run_parser
from .schedule import add_schedule_parser


def parse_args(
        description: str
    ) -> Tuple[Namespace, Dict[str, Callable]]:
    parser = ArgumentParser(description=description)
    subparsers = parser.add_subparsers(
        dest='command',
        required=True,
        help='Command to run.'
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )

    command_runners = {
        'info': add_info_parser(subparsers),
        'run': add_run_parser(subparsers),
        'index': add_index_parser(subparsers),
        'schedule': add_schedule_parser(subparsers)
    }
    return parser.parse_args(), command_runners


def run_cli(flow) -> None:
    if is_main_script():
        return

    if len(flow.tasks) == 0:
        raise ValueError('Flow must contain at least one task.')

    args, command_runners = parse_args(description=flow.description)

    if 'project_dir' in args:
        if args.project_dir is not None:
            GlobalContext.set_project_dir(Path(args.project_dir).resolve())

    command_runners[args.command](args, flow)