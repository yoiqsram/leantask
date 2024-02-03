import argparse
from typing import Callable, Dict, List

from ...context import GlobalContext
from ...enum import FlowIndexStatus


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

    return index_flow


def index_flow(args: argparse.Namespace, flow) -> None:
    from ...database.orm import open_db_session

    try:
        with open_db_session(GlobalContext.database_path()) as session:
            index_metadata_to_db(flow, session, args.force)
    
    except Exception as exc:
        print(f'{exc.__class__.__name__}: {exc}')
        raise SystemExit(FlowIndexStatus.FAILED)


def index_metadata_to_db(flow, session, force: bool = False) -> None:
    from ...database.execute import get_flow_record, copy_records_to_log
    from ...database.models import FlowModel, TaskModel, TaskDownstreamModel
    from ...database.orm import NoResultFound

    tasks_relationship = {
        task.name: [task.name for task in task.downstreams]
        for task in flow.tasks
    }

    try:
        # Reindex existing flow record
        flow_record = get_flow_record(flow.name, session=session)

        tasks_query = (
            session.query(TaskModel)
            .filter(TaskModel.flow_id == flow_record.id)
        )

        if flow_record.checksum == flow.checksum and not force:
            print(f"Flow '{flow_record.name}' is already indexed.")
            raise SystemExit(FlowIndexStatus.UNCHANGED.value)

        flow_record.name = flow.name
        flow_record.checksum = flow.checksum
        flow_record.max_delay = flow.max_delay
        flow_record.active = flow.active

        tasks_query.delete()

    except NoResultFound:
        # Index new flow record
        flow_record = FlowModel(
            name=flow.name,
            path=str(GlobalContext.relative_path(flow.path)),
            checksum=flow.checksum,
            max_delay=flow.max_delay,
            active=flow.active
        )
        session.add(flow_record)

    # Index tasks and its relationship
    task_record_map = {
        name: TaskModel(name=name, flow=flow_record)
        for name in tasks_relationship.keys()
    }
    task_records = list(task_record_map.values())
    session.add_all(task_records)

    task_downstream_records = [
        TaskDownstreamModel(
            task=task_record,
            downstream_task=task_record_map[downstream_name]
        )
        for name, task_record in task_record_map.items()
        for downstream_name in tasks_relationship[name]
    ]
    session.add_all(task_downstream_records)

    session.commit()

    copy_records_to_log([flow_record] + task_records + task_downstream_records)

    print(
        f"Successfully indexing '{flow.name}' workflow",
        f"and {len(task_record_map)} task(s)."
    )
