import argparse
from typing import Callable

from ...context import GlobalContext
from ...enum import FlowIndexStatus
from ...logging import get_logger

logger = None


def add_index_parser(subparsers) -> Callable:
    parser = subparsers.add_parser(
        'index',
        help='index flow to database',
        description='index flow to database'
    )
    parser.add_argument(
        '--force', '-F',
        action='store_true',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help=argparse.SUPPRESS
    )

    return index_flow


def index_flow(args: argparse.Namespace, flow) -> None:
    from ...database.orm import open_db_session

    global logger
    logger = get_logger('flow.index')

    try:
        with open_db_session(GlobalContext.database_path()) as session:
            index_metadata_to_db(flow, session, args.force)

    except Exception as exc:
        logger.info(f"Failed to index flow '{flow.path}'")
        logger.error(f'{exc.__class__.__name__}: {exc}', exc_info=True)
        raise SystemExit(FlowIndexStatus.FAILED)


def index_metadata_to_db(flow, session, force: bool = False) -> None:
    from ...database.execute import get_flow_record, copy_records_to_log
    from ...database.models import FlowModel, TaskModel, TaskDownstreamModel
    from ...database.orm import NoResultFound

    try:
        logger.debug(f"Get flow record from database.")
        flow_record = get_flow_record(flow.name, session=session)

        tasks_query = (
            session.query(TaskModel)
            .filter(TaskModel.flow_id == flow_record.id)
        )

        if flow_record.checksum == flow.checksum:
            logger.info(f"Flow '{flow.name}' is already indexed.")
            if not force:
                raise SystemExit(FlowIndexStatus.UNCHANGED.value)

        if not force:
            logger.debug(f"Flow '{flow.name}' has changed. Update new value from the changes.")

        flow_record.name = flow.name
        flow_record.checksum = flow.checksum
        flow_record.max_delay = flow.max_delay
        flow_record.active = flow.active

        logger.debug(f"Remove all old {tasks_query.count()} task(s) that might change.")
        tasks_query.delete()

    except NoResultFound:
        logger.debug(f"Flow '{flow.name}' has not been indexed. Indexing the flow...")
        flow_record = FlowModel(
            name=flow.name,
            path=str(GlobalContext.relative_path(flow.path)),
            checksum=flow.checksum,
            max_delay=flow.max_delay,
            active=flow.active
        )
        logger.debug(f"Add flow to the database.")
        session.add(flow_record)

    logger.debug(f"Prepare Task models for all tasks.")
    task_record_map = {
        task.name: TaskModel(name=task.name, flow=flow_record)
        for task in flow.tasks
    }
    task_records = list(task_record_map.values())
    logger.debug(f"Add {len(task_records)} task(s) to the database.")
    session.add_all(task_records)

    logger.debug(f"Prepare all task relationships.")
    tasks_relationship = {
        task.name: [task.name for task in task.downstreams]
        for task in flow.tasks
    }
    task_downstream_records = [
        TaskDownstreamModel(
            task=task_record,
            downstream_task=task_record_map[downstream_name]
        )
        for name, task_record in task_record_map.items()
        for downstream_name in tasks_relationship[name]
    ]
    logger.debug(f"Add {len(task_records)} task relations to the database.")
    session.add_all(task_downstream_records)

    session.commit()

    copy_records_to_log([flow_record] + task_records + task_downstream_records)

    logger.info(
        f"Successfully indexing '{flow.name}' workflow"
        f" and {len(task_record_map)} task(s)."
    )
