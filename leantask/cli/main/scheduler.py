import argparse
from typing import Callable

from ...scheduler import Scheduler


def add_scheduler_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'scheduler',
        help='Leantask scheduler',
        description='Leantask scheduler'
    )
    add_scheduler_arguments(parser)

    return run_scheduler


def add_scheduler_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        '--worker', '-W',
        type=int,
        help='Number of worker.'
    )
    parser.add_argument(
        '--heartbeat', '-H',
        type=int,
        help='Heartbeat interval.'
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )


def run_scheduler(args: argparse.Namespace) -> None:
    scheduler = Scheduler(
        worker=args.worker,
        heartbeat=args.heartbeat
    )
    scheduler.run_loop()
