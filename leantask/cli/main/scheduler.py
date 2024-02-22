import argparse
from typing import Callable


def add_scheduler_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'scheduler',
        help='Leantask scheduler',
        description='Leantask scheduler'
    )
    parser.add_argument(
        '--worker', '-W',
        default=1,
        help='Number of worker.'
    )
    parser.add_argument(
        '--heartbeat', '-H',
        default=30,
        help='Heartbeat interval.'
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help=argparse.SUPPRESS
    )

    return run_scheduler


def run_scheduler(args: argparse.Namespace) -> None:
    import asyncio
    from ...scheduler import Scheduler

    scheduler = Scheduler(
        worker=args.worker,
        heartbeat=args.heartbeat,
        debug=args.debug
    )

    asyncio.run(scheduler.run_loop())
