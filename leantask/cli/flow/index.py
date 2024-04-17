from __future__ import annotations

import argparse
import sys
from typing import Callable, TYPE_CHECKING

from ...context import GlobalContext
from ...enum import FlowIndexStatus
from ...logging import get_local_logger, get_logger
from ...utils.string import quote

if TYPE_CHECKING:
    from ...flow import Flow

logger = None


def add_index_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'index',
        help='Index flow to database',
        description='Index flow to database'
    )
    add_index_arguments(parser)

    return index_flow


def add_index_arguments(parser: argparse.ArgumentParser) -> None:
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
        '--log',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '--scheduler-session-id',
        help=argparse.SUPPRESS
    )


def index_flow(
        args: argparse.Namespace,
        flow: Flow
    ) -> None:
    global logger
    if args.log is not None:
        logger = get_logger('flow.index', args.log)
    else:
        logger = get_local_logger('flow.index')

    logger.info(f"Run command: {' '.join([quote(sys.executable)] + sys.argv)}")

    GlobalContext.SCHEDULER_SESSION_ID = args.scheduler_session_id

    try:
        index_status = flow.index()
        logger.info(f'Flow index status: {index_status.name}')
        raise SystemExit(index_status.value)

    except Exception as exc:
        logger.info(f"Failed to index flow '{flow.path}'")
        logger.error(f'{exc.__class__.__name__}: {exc}', exc_info=True)
        raise SystemExit(FlowIndexStatus.FAILED.value)
