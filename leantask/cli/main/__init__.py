import argparse
from typing import Callable, Dict, Tuple

from ...context import GlobalContext
from .discover import add_discover_parser
from .init import add_init_parser
from .info import add_info_parser
from .scheduler import add_scheduler_parser


def parse_args() -> Tuple[argparse.Namespace, Dict[str, Callable]]:
    parser = argparse.ArgumentParser(
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
        'discover': add_discover_parser(subparsers),
        'scheduler': add_scheduler_parser(subparsers)
    }

    return parser.parse_args(), command_runners


def cli():
    args, command_runners = parse_args()

    if 'debug' in args and args.debug:
        GlobalContext.LOG_DEBUG = True

    command_runners[args.command](args)
