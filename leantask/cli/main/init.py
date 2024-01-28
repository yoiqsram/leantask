import argparse
from pathlib import Path

from ...context import GlobalContext


def add_init_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        'init',
        help='Initialize leantask project.',
        description='Initialize leantask project.'
    )
    parser.add_argument(
        '--name', '-N',
        default=GlobalContext.PROJECT_DIR.name,
        help='Project name. Default to project directory name.'
    )
    parser.add_argument(
        '--description', '-D',
        help='Project description.'
    )
    parser.add_argument(
        '--replace', '-R',
        action='store_true',
        help='Replace project if it already exists.'
    )

    return init_project


def init_project(args: argparse.Namespace) -> None:
    from ...database.orm import create_metadata_database

    database_path = GlobalContext.database_path()

    if database_path.exists() and not args.replace:
        raise SystemExit(
            'Failed to initialize the project.\n'
            f"There is already a project exists in '{GlobalContext.PROJECT_DIR}'."
        )

    if not GlobalContext.PROJECT_DIR.is_dir():
        raise SystemExit(f"Project directory '{GlobalContext.PROJECT_DIR}' does not exists.")

    create_metadata_database(
        project_name=args.name,
        project_description=args.description,
        replace=args.replace
    )

    print(f"Project created successfully on '{GlobalContext.PROJECT_DIR}'.")
