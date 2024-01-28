from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Callable, Dict, Tuple

from ...context import GlobalContext
from .init import add_init_parser
from .info import add_info_parser
# from .scheduler import add_scheduler_parser


def parse_args() -> Tuple[Namespace, Dict[str, Callable]]:
    parser = ArgumentParser(
        description='Leantask: Simple and lean workflow scheduler for Python.'
    )
    subparsers = parser.add_subparsers(
        dest='command',
        required=True,
        help='Command to run.'
    )

    command_runners = {
        'init': add_init_parser(subparsers),
        'info': add_info_parser(subparsers),
        # 'scheduler': add_scheduler_parser(subparsers)
    }

    return parser.parse_args(), command_runners


def cli():
    args, command_runners = parse_args()
    command_runners[args.command](args)
