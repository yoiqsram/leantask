import argparse
from tabulate import tabulate
from typing import Callable


def add_info_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'info',
        help='Show info about the project.',
        description='Show info about the project.'
    )

    return show_info


def show_info(args: argparse.Namespace):
    from ...database import MetadataModel

    info = []
    for metadata in MetadataModel.select():
        if metadata.name == 'name':
            info.append(('Name', metadata.value))

        elif metadata.name == 'is_active':
            info.append(('Active', metadata.value))

        else:
            info.append((metadata.name, metadata.value))

    print(tabulate(info, tablefmt='simple_outline'))
