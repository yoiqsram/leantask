import argparse
from typing import Callable

from ...context import GlobalContext


def add_info_parser(subparsers) -> Callable:
    parser = subparsers.add_parser(
        'info',
        help='Show info about leantask project.',
        description='Show info about leantask project.'
    )
    options_subparser = parser.add_subparsers(
        dest='options',
        help='Options to show. Default to show project info.',
        description='Options to show. Default to show project info.',
    )

    project_parser = options_subparser.add_parser(
        'project',
        help='Show project info.'
    )

    flow_parser = options_subparser.add_parser(
        'flow',
        help='Show flow info.'
    )

    return show_info


def show_info(args: argparse.Namespace):
    if args.options == 'flow':
        show_flow_info()
    else:
        show_project_info()


def show_project_info() -> None:
    from ...database.orm import open_db_session, MetadataModel

    print('Leantask Project Information.')
    with open_db_session(GlobalContext.database_path()) as session:
        for metadata in session.query(MetadataModel).all():
            print(f"{metadata.name}: {metadata.value}")


def show_flow_info() -> None:
    from ...database.models import FlowModel, FlowScheduleModel
    from ...database.orm import open_db_session, NoResultFound

    with open_db_session(GlobalContext.database_path()) as session:
        print('Found', session.query(FlowModel).count(), 'workflow(s) in the project.')
        for flow_record in session.query(FlowModel).all():
            try:
                schedule_record = (
                    session.query(FlowScheduleModel)
                    .filter(FlowScheduleModel.flow_id == flow_record.id)
                    .one()
                )
                print(
                    f"- {flow_record.name} (path='{flow_record.path}'",
                    f"next_schedule='{schedule_record.schedule_datetime.isoformat(sep=' ', timespec='minutes')}')"
                )

            except NoResultFound:
                print('-', f"{flow_record.name} (path='{flow_record.path}' no_schedule")
