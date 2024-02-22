import argparse
from argparse import Namespace
from datetime import datetime
from pathlib import Path
import subprocess
from typing import Callable, List, Tuple

from ...context import GlobalContext
from ...utils.path import get_file_created_datetime


def add_logs_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'logs',
        help='Show run logs.',
        description='Show run logs.'
    )
    option = parser.add_subparsers(
        dest='option',
        required=True,
        help='log options'
    )

    list_option: argparse.ArgumentParser = option.add_parser(
        'list',
        help='Show list of log files from the latest runs.'
    )
    list_option.add_argument(
        '--max', '-M',
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

    inspect_option: argparse.ArgumentParser = option.add_parser(
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

    return show_logs


def show_logs(args: Namespace, flow) -> None:
    log_dir = GlobalContext.log_dir() / 'flow_runs' / str(flow.id)
    log_file_paths = {
        get_file_created_datetime(log_file_path): log_file_path
        for log_file_path in log_dir.rglob('*.log')
    }

    if len(log_file_paths) == 0:
        print('No log file was found.')
        return

    log_file_created_datetime_sorted = sorted(list(log_file_paths.keys()))

    log_file_paths_sorted: List[Tuple[datetime, Path]] = []
    for created_datetime in log_file_created_datetime_sorted:
        log_file_paths_sorted.append((
            created_datetime,
            log_file_paths[created_datetime]
        ))

    if args.option == 'list':
        show_log_list(
            log_file_paths_sorted,
            max=args.max,
            full=args.full
        )

    elif args.option == 'inspect':
        inspect_log(
            log_file_paths_sorted,
            last=args.last
        )


def show_log_list(
        log_file_paths_sorted: List[Tuple[datetime, Path]],
        max: int,
        full: bool
    ) -> None:
    log_file_path_count = len(log_file_paths_sorted)
    if log_file_path_count == 0:
        print('No log file was found.')
        return

    print(
        f'Found {log_file_path_count} run logs.'
        + (' Show the last {max} logs.' if log_file_path_count > max else '')
    )
    for created_datetime, log_file_path in log_file_paths_sorted[:max]:
        if not full:
            log_file_path = GlobalContext.relative_path(log_file_path)

        print(
            f"[{created_datetime.isoformat(sep=' ', timespec='seconds')}]",
            str(log_file_path)
        )


def inspect_log(
        log_file_paths_sorted: List[Tuple[datetime, Path]],
        last: int
    ) -> None:
    log_file_path_count = len(log_file_paths_sorted)
    if last > log_file_path_count:
        print(f'Failed to open the latest {last} log. There are only {log_file_path_count} logs.')
        return

    created_datetime, log_file_path = log_file_paths_sorted[-last]
    print(
        f"Inspect latest log '{log_file_path}'",
        f"({created_datetime.isoformat(sep=' ', timespec='seconds')})."
    )
    subprocess.run(
        f'nano "{log_file_path}"',
        shell=True
    )
