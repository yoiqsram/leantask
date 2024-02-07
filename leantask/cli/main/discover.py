import argparse
from typing import Callable

from ...logging import get_logger


def add_discover_parser(subparsers) -> Callable:
    parser = subparsers.add_parser(
        'discover',
        help='Discover workflows and indexed them.',
        description='Discover workflows and indexed them.'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help=argparse.SUPPRESS
    )

    return discover_flows


def discover_flows(args: argparse.Namespace):
    from ...discover import update_flow_records

    logger = get_logger('discover')

    logger.debug('Searching for workflows...')
    flow_records = update_flow_records()

    logger.info(f'Total flow(s) found: {len(flow_records)}.')
