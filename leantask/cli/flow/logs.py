import argparse
from datetime import datetime
from pathlib import Path
import subprocess
from typing import Callable, List, Tuple

from ...context import GlobalContext
from ...flow import Flow
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

    list_option = option.add_parser(
        'list',
        help='Show list of log files from the latest runs.'
    )
    list_option.add_argument(
        '--limit', '-l',
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

    search_option = option.add_parser(
        'search',
        help='Search run log.'
    )
    search_option.add_argument(
        'keyword',
        help='Search keyword. Default for flow run id.'
    )
    search_option.add_argument(
        '--by-task',
        action='store_true',
        help='Enable to search by task run id.'
    )
    search_option.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )

    inspect_option = option.add_parser(
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


def show_logs(
        args: argparse.Namespace,
        flow: Flow
    ) -> None:
    log_dir = (
        GlobalContext.log_dir()
        / 'flow_runs'
        / str(flow.id)
    )

    if args.option == 'search':
        if args.by_task:
            log_dir = (
                GlobalContext.log_dir()
                / 'task_runs'
                / str(flow.id)
            )

        keyword = args.keyword.replace('.', '') + '*.log'
        for log_file_path in log_dir.rglob(keyword):
            return edit_log(log_file_path)

    log_file_paths_sorted = get_all_log_file_paths_sorted(log_dir)

    if args.option == 'list':
        show_log_list(
            log_file_paths_sorted,
            limit=args.limit,
            full=args.full
        )

    elif args.option == 'inspect':
        if args.last > len(log_file_paths_sorted):
            print(
                f'Failed to open the latest {args.last} log.',
                f'There are only {len(log_file_paths_sorted)} logs.'
            )
            return

        return edit_log(*log_file_paths_sorted[-args.last])


def get_all_log_file_paths_sorted(log_dir: Path) -> List[Tuple[datetime, Path]]:
    log_file_paths = {
        get_file_created_datetime(log_file_path): log_file_path
        for log_file_path in log_dir.rglob('*.log')
    }

    log_file_path_count = len(log_file_paths)
    if log_file_path_count == 0:
        print('No log file was found.')
        raise SystemExit()

    log_file_created_datetime_sorted = sorted(list(log_file_paths.keys()))

    log_file_paths_sorted: List[Tuple[datetime, Path]] = []
    for created_datetime in log_file_created_datetime_sorted:
        log_file_paths_sorted.append((
            log_file_paths[created_datetime],
            created_datetime
        ))

    return log_file_paths_sorted


def show_log_list(
        log_file_paths_sorted: List[Tuple[datetime, Path]],
        limit: int,
        full: bool
    ) -> None:
    log_file_path_count = len(log_file_paths_sorted)
    if log_file_path_count == 0:
        print('No log file was found.')
        return

    print(
        f'Found {log_file_path_count} run logs.'
        + (' Show the last {max} logs.' if log_file_path_count > limit else '')
    )
    for log_file_path, created_datetime in log_file_paths_sorted[:limit]:
        if not full:
            log_file_path = GlobalContext.relative_path(log_file_path)

        print(
            f"[{created_datetime.isoformat(sep=' ', timespec='seconds')}]",
            str(log_file_path)
        )


def edit_log(
        log_file_path: Path,
        created_datetime: datetime = None
    ) -> None:
    print(
        f"Inspect log '{log_file_path}'"
        + (
            f"(created at {created_datetime.isoformat(sep=' ', timespec='seconds')})."
            if created_datetime is not None else ''
        )
    )
    subprocess.run(
        f'nano "{log_file_path}"',
        shell=True
    )
