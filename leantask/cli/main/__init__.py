import argparse
from typing import Callable, Dict, Tuple

from .discover import add_discover_parser
from .init import add_init_parser
from .info import add_info_parser
from .flows import add_flows_parser
from .scheduler import add_scheduler_parser
from .tasks import add_tasks_parser


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
        'flows': add_flows_parser(subparsers),
        'tasks': add_tasks_parser(subparsers),
        'scheduler': add_scheduler_parser(subparsers)
    }

    return parser.parse_known_args(), command_runners


def cli():
    (args, _), command_runners = parse_args()

    if args.command == 'flows': 
        command_runners[args.command][args.flows_command](args)

    elif args.command == 'tasks': 
        command_runners[args.command][args.tasks_command](args)

    else:
        command_runners[args.command](args)
