import argparse
from typing import Callable


def add_discover_parser(subparsers) -> Callable:
    parser = subparsers.add_parser(
        'discover',
        help='Discover workflows and indexed them.',
        description='Discover workflows and indexed them.'
    )

    return discover_flows


def discover_flows(args: argparse.Namespace):
    from ...discover import update_flow_records

    print('Searching for workflows...')
    flow_records = update_flow_records()

    print(f'Found {len(flow_records)} flow(s).')
