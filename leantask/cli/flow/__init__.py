from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable, Dict, Tuple, TYPE_CHECKING

from ...context import GlobalContext
from .index import add_index_parser
from .info import add_info_parser
from .log import add_log_parser
from .run import add_run_parser
from .schedule import add_schedule_parser
from .status import add_status_parser

if TYPE_CHECKING:
    from ...flow import Flow


def parse_args(
        description: str
    ) -> Tuple[argparse.Namespace, Dict[str, Callable]]:
    parser = argparse.ArgumentParser(description=description)
    subparsers = parser.add_subparsers(
        dest='command',
        required=True,
        help='Command to run'
    )

    command_runners = {
        'info': add_info_parser(subparsers),
        'run': add_run_parser(subparsers),
        'index': add_index_parser(subparsers),
        'schedule': add_schedule_parser(subparsers),
        'status': add_status_parser(subparsers),
        'log': add_log_parser(subparsers)
    }
    return parser.parse_known_args(), command_runners


def run_cli(flow: Flow) -> None:
    if len(flow.tasks) == 0:
        raise ValueError('Flow must contain at least one task.')

    (args, _), command_runners = parse_args(description=flow.description)

    if 'project_dir' in args and args.project_dir is not None:
        GlobalContext.set_project_dir(Path(args.project_dir).resolve())
    command_runners[args.command](args, flow)
