import argparse
from typing import Callable

from ...utils.string import quote


def add_info_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'info',
        help='Show info about leantask project.',
        description='Show info about leantask project.'
    )
    options_subparser = parser.add_subparsers(
        dest='options',
        help='Options to show. Default to show project info.',
        description='Options to show. Default to show project info.',
    )

    options_subparser.add_parser(
        'project',
        help='Show project info.'
    )
    options_subparser.add_parser(
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
    from ...database import MetadataModel

    print('Leantask Project Information.')
    for metadata in MetadataModel.select():
        print(f"{metadata.name}: {metadata.value}")


def show_flow_info() -> None:
    from ...database import FlowModel, FlowScheduleModel

    flow_models = list(
        FlowModel.select()
        .order_by(FlowModel.name)
    )
    print('Found', len(flow_models), 'workflow(s) in the project.')
    for flow_model in flow_models:
        if not flow_model.active:
            print(
                f"- {flow_model.name} (path={flow_model.path} inactive)"
            )
            continue

        schedule_models = list(
            FlowScheduleModel.select()
            .where(FlowScheduleModel.flow_id == flow_model.id)
            .order_by(FlowScheduleModel.schedule_datetime)
        )

        if len(schedule_models) == 0:
            print(' ' * 3, '-', f"{flow_model.name} (path='{flow_model.path}' no_schedule)")
            continue

        print(
            ' ' * 3, 
            f'- {flow_model.name} (path={flow_model.path}',
            f"next_schedule={quote(schedule_models[0].schedule_datetime.isoformat(sep=' ', timespec='minutes'))}",
            f'max_delay={schedule_models[0].max_delay})',
        )
