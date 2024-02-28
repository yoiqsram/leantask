import argparse
from typing import Callable, List


def add_info_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'info',
        help='Show info about leantask project.',
        description='Show info about leantask project.'
    )
    option_subparser = parser.add_subparsers(
        dest='option',
        help='Option to show. Default to show project info.',
        description='Options to show. Default to show project info.',
    )

    option_subparser.add_parser(
        'project',
        help='Show project info.'
    )
    option_subparser.add_parser(
        'flows',
        help='Show flow info.'
    )

    return show_info


def show_info(args: argparse.Namespace):
    if args.option == 'flows':
        show_flow_info()
    else:
        show_project_info()


def show_project_info() -> None:
    from ...database import MetadataModel

    print('Leantask Project Information.')
    for metadata in MetadataModel.select():
        print(f"{metadata.name}: {metadata.value}")


def show_flow_info() -> None:
    from ...database import FlowModel, FlowScheduleModel

    flow_models: List[FlowModel] = list(
        FlowModel.select()
        .order_by(FlowModel.name)
    )
    print('Found', len(flow_models), 'flow(s) in the project.')
    for flow_model in flow_models:
        print(
            ( '[Disabled]  ' if not flow_model.active else '' )
            + f"{flow_model.name}  {str(flow_model).split('-')[0]}..."
        )

        print(
            ' ', 'Path          :',
            flow_model.path
        )

        print(
            ' ', 'Description   :',
            flow_model.description
        )

        if flow_model.active:
            schedule_models = list(
                FlowScheduleModel.select()
                .where(FlowScheduleModel.flow_id == flow_model.id)
                .order_by(FlowScheduleModel.schedule_datetime)
                .limit(1)
            )
            if len(schedule_models) > 0:
                print(
                    ' ', 'Next schedule :',
                    schedule_models[0].schedule_datetime.isoformat(sep=' ', timespec='minutes')
                )

        print()
