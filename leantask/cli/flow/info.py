from __future__ import annotations

import argparse
import sys
from tabulate import tabulate
from typing import Callable, TYPE_CHECKING

from ...utils.string import quote

if TYPE_CHECKING:
    from ...flow import Flow


def add_info_parser(subparsers) -> Callable:
    parser = subparsers.add_parser(
        'info',
        help='Show flow information.',
        description='Show flow information.'
    )

    return show_info


def show_info(
        args: argparse.Namespace,
        flow: Flow
    ) -> None:
    # Show flow name and description
    print(tabulate(
        [
            ('Name', flow.name),
            ('Description', flow.description),
        ],
        tablefmt='simple_outline'
    ))

    # Show index status
    if not flow._model_exists:
        print(
            'Flow has not been indexed yet. Please index the flow using this command:\n'
            f'{quote(sys.executable)} {quote(flow.path)} index'
        )
    elif flow.checksum != flow._model.checksum:
        print('Flow has unindexed changes. Please reindex the flow using this command:\n'
            f'{quote(sys.executable)} {quote(flow.path)} index'
        )
    else:
        print('Flow has been indexed.')

    # Show list of tasks in the flow
    print()
    print(f'Contains {len(flow.tasks)} task(s).')
    print(tabulate(
        [
            [
                task.name,
                task.retry_max,
                task.retry_delay
            ]
            for task in flow.tasks_sorted
        ],
        headers=['Task Name', 'Max Retries', 'Retry Delay (s)'],
        showindex=range(1, len(flow.tasks) + 1),
        tablefmt='simple_outline'
    ))
