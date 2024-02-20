import argparse
from typing import Callable


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

    flow_models = list(FlowModel.select())
    print('Found', len(flow_models), 'workflow(s) in the project.')
    for flow_model in flow_models:
        if not flow_model.active:
            print(
                f"- {flow_model.name} (path={flow_model.path} inactive)"
            )
            continue

        schedule_model = (
            FlowScheduleModel.select()
            .where(FlowScheduleModel.flow_id == flow_model.id)
        )

        if len(schedule_model) == 0:
            print('-', f"{flow_model.name} (path='{flow_model.path}' no_schedule)")
            continue

        print(
            f"- {flow_model.name} (path={flow_model.path}",
            f"next_schedule={flow_model.schedule_datetime.isoformat(sep=' ', timespec='minutes')}",
            f"max_delay={flow_model.max_delay})",
        )
