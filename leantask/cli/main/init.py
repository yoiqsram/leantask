import argparse
import os
import sys
from datetime import datetime

from ...context import GlobalContext
from ...database import (
    FlowModel, FlowScheduleModel, FlowRunModel,
    MetadataModel,
    TaskModel, TaskDownstreamModel, TaskRunModel,
    FlowLogModel, FlowRunLogModel,
    TaskLogModel, TaskDownstreamLogModel, TaskRunLogModel,
    SchedulerSessionModel,
    database, log_database
)
from ...logging import get_local_logger
from ...utils.script import has_sudo_access, sync_server_time
from ...utils.string import quote


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
    metadata_dir = GlobalContext.metadata_dir()
    metadata_dir_backup = (
        metadata_dir.parent
        / (metadata_dir.name + f".backup_{datetime.now().isoformat(timespec='seconds')}")
    )

    if args.replace and metadata_dir.is_dir():
        os.rename(metadata_dir, metadata_dir_backup)

    metadata_dir.mkdir(parents=True, exist_ok=True)

    global logger
    logger = get_local_logger('init')
    logger.info(f"Run command: {' '.join([quote(sys.executable)] + sys.argv)}")

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
    log_database_path = GlobalContext.log_database_path()
    if (database_path.exists() and os.path.getsize(database_path) > 0) \
            or (log_database_path.exists() and os.path.getsize(log_database_path) > 0):
        logger.warning(f"There is already a project exists in '{GlobalContext.PROJECT_DIR}'.")
        if not args.replace:
            logger.error('Failed to initialize the project.')
            raise SystemExit(1)

        logger.debug('Project will be replaced.')

    try:
        create_metadata_database(
            project_name=args.name,
            project_description=args.description
        )

    except Exception as exc:
        logger.error(f'{exc.__class__.__name__}: {exc}', exc_info=True)
        if metadata_dir_backup.is_dir():
            os.rename(metadata_dir_backup, metadata_dir)
        raise SystemExit(1)

    logger.info(f"Project created successfully on '{GlobalContext.PROJECT_DIR}'.")


def create_metadata_database(
        project_name: str,
        project_description: str = None
    ) -> None:
    try:
        database.create_tables([
            FlowModel, FlowScheduleModel, FlowRunModel,
            TaskModel, TaskDownstreamModel, TaskRunModel,
            MetadataModel
        ])
        log_database.create_tables([
            FlowLogModel, FlowRunLogModel,
            TaskLogModel, TaskDownstreamLogModel, TaskRunLogModel,
            SchedulerSessionModel
        ])

        project_metadata = {
            'name': project_name,
            'description': project_description,
            'is_active': True
        }

        for name, value in project_metadata.items():
            MetadataModel.create(name=name, value=str(value))

    except Exception as exc:
        GlobalContext.database_path().unlink(missing_ok=True)
        GlobalContext.log_database_path().unlink(missing_ok=True)
        raise exc
