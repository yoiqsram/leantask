import argparse
import sys
from pathlib import Path
from typing import Callable

from ...context import GlobalContext
from ...logging import get_logger
from ...utils.string import quote


def add_discover_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'discover',
        help='Discover workflows and indexed them.',
        description='Discover workflows and indexed them.'
    )
    parser.add_argument(
        '--log',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '--scheduler_session_id',
        help=argparse.SUPPRESS
    )

    return discover_flows


def discover_flows(args: argparse.Namespace):
    from ...discover import index_all_flows

    global logger
    if args.log is not None:
        log_file_path = Path(args.log)
    else:
        log_file_path = GlobalContext.get_local_log_file_path()

    logger = get_logger('discover', log_file_path)
    logger.info(f'''Run command: {' '.join([quote(sys.executable)] + sys.argv)}''')

    GlobalContext.SCHEDULER_SESSION_ID = args.scheduler_session_id

    logger.debug('Searching for workflows...')
    flow_records = index_all_flows(log_file_path=log_file_path)
    logger.info(f'Total flow(s) found: {len(flow_records)}.')
