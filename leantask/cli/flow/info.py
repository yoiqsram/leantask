from argparse import Namespace
from typing import Callable


def add_info_parser(subparsers) -> Callable:
    subparsers.add_parser(
        'info',
        help='show flow information',
        description='show flow information'
    )
    return show_info


def show_info(args: Namespace, flow) -> None:
    # Show flow name and description
    print(flow.name)
    print(' ' * 3, flow.description, end='\n\n')

    # Show list of tasks in the flow
    print(f'{len(flow.tasks)} tasks (unordered):')
    for task in flow.tasks:
        print(' ' * 3, '-', task.name)
