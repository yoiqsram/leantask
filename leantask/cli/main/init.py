import argparse

from ...context import GlobalContext
from ...logging import get_logger


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
    parser.add_argument(
        '--debug',
        action='store_true',
        help=argparse.SUPPRESS
    )

    return init_project


def init_project(args: argparse.Namespace) -> None:
    from ...database.orm import create_metadata_database

    logger = get_logger('cli.main.init')

    database_path = GlobalContext.database_path()

    if database_path.exists() and not args.replace:
        logger.error(
            'Failed to initialize the project.'
            f" There is already a project exists in '{GlobalContext.PROJECT_DIR}'."
        )
        raise SystemExit(1)

    create_metadata_database(
        project_name=args.name,
        project_description=args.description,
        replace=args.replace
    )

    logger.info(f"Project created successfully on '{GlobalContext.PROJECT_DIR}'.")
