import argparse

from ...context import GlobalContext
from ...logging import get_logger
from ...utils.script import has_sudo_access, sync_server_time


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

    if has_sudo_access():
        try:
            logger.debug('Sync server time.')
            sync_server_time()
            logger.debug('Successfully sync time.')
        except LookupError:
            logger.warning('Failed to sync time to NTP server.')
    else:
        logger.warning(
            'Failed to sync time due to lack of permission. Please sync your time using this command: '
            'sudo ntpdate -s ntp.ubuntu.com'
        )

    database_path = GlobalContext.database_path()
    if database_path.exists():
        logger.warning(f"There is already a project exists in '{GlobalContext.PROJECT_DIR}'.")
        if not args.replace:
            logger.error('Failed to initialize the project.')
            raise SystemExit(1)

        logger.debug('Project will be replaced.')

    create_metadata_database(
        project_name=args.name,
        project_description=args.description,
        replace=args.replace
    )

    logger.info(f"Project created successfully on '{GlobalContext.PROJECT_DIR}'.")
