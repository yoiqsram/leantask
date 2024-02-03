from argparse import ArgumentParser, Namespace
from typing import Callable, Dict, Tuple

from .discover import add_discover_parser
from .init import add_init_parser
from .info import add_info_parser


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
        'discover': add_discover_parser(subparsers)
    }

    return parser.parse_args(), command_runners


def cli():
    args, command_runners = parse_args()
    command_runners[args.command](args)
