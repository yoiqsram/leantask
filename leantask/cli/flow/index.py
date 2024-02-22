import argparse
import sys
from typing import Callable

from ...context import GlobalContext
from ...enum import FlowIndexStatus
from ...logging import get_local_logger, get_logger
from ...utils.string import quote

logger = None


def add_index_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'index',
        help='index flow to database',
        description='index flow to database'
    )
    parser.add_argument(
        '--force', '-F',
        action='store_true',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )
    parser.add_argument(
        '--log-file',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '--scheduler-session-id',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help=argparse.SUPPRESS
    )

    return index_flow


def index_flow(args: argparse.Namespace, flow) -> None:
    global logger
    if args.log_file is not None:
        logger = get_logger('flow.index', args.log_file)
    else:
        logger = get_local_logger('flow.index')

    logger.info(f"Run command: {' '.join([quote(sys.executable)] + sys.argv)}")

    GlobalContext.SCHEDULER_SESSION_ID = args.scheduler_session_id

    try:
        index_status: FlowIndexStatus = flow.index()
        logger.info(f'Flow index status: {index_status.name}')
        raise SystemExit(index_status.value)

    except Exception as exc:
        logger.info(f"Failed to index flow '{flow.path}'")
        logger.error(f'{exc.__class__.__name__}: {exc}', exc_info=True)
        raise SystemExit(FlowIndexStatus.FAILED.value)
