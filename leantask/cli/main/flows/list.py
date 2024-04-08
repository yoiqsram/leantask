import argparse
from typing import Callable, List


def add_list_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'list',
        help='List workflows.',
        description='List workflows.'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help=argparse.SUPPRESS
    )

    return list_flows


def list_flows(args: argparse.Namespace) -> None:
    from ....database import FlowModel, FlowScheduleModel

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
