import sys
from argparse import Namespace
from typing import Callable

from ...utils.string import quote


def add_info_parser(subparsers) -> Callable:
    parser = subparsers.add_parser(
        'info',
        help='show flow information',
        description='show flow information'
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )
    return show_info


def show_info(args: Namespace, flow) -> None:
    # Show flow name and description
    print(flow.name)
    print(' ' * 3, flow.description)

    # Show list of tasks in the flow
    print()
    print(f'{len(flow.tasks)} tasks (sorted):')
    for task in flow.tasks_sorted:
        print(' ' * 3, '-', task.name)

    # Show index status
    print()
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
