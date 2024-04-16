import argparse
from collections import OrderedDict
from datetime import datetime
from tabulate import tabulate
from typing import Callable, List


def add_list_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'list',
        help='List workflows.',
        description='List workflows.'
    )

    return list_flows


def list_flows(args: argparse.Namespace) -> None:
    from ....database import FlowModel, FlowRunModel, FlowScheduleModel
    from ....enum import FlowRunStatus

    flow_models: List[FlowModel] = list(
        FlowModel.select()
        .order_by(
            FlowModel.active,
            FlowModel.name,
            FlowModel.path
        )
    )

    flow_infos = []
    for i, model in enumerate(flow_models):
        try:
            last_run = (
                model.flow_runs.select()
                .where(~FlowRunModel.status.startswith(FlowRunStatus.SCHEDULED.name))
                .order_by(
                    FlowRunModel.modified_datetime.desc(),
                    FlowRunModel.started_datetime.desc(),
                    FlowRunModel.created_datetime.desc()
                )
                .get()
            )
            if last_run.status == FlowRunStatus.DONE.name \
                    or last_run.status.startswith(FlowRunStatus.FAILED.name):
                total_time_elapsed = (last_run.modified_datetime - last_run.started_datetime).seconds
                completed_datetime = last_run.modified_datetime.isoformat(sep=' ', timespec='minutes')
                last_run_status = f"{last_run.status} {total_time_elapsed:d}s ({completed_datetime})"

            elif last_run.status == FlowRunStatus.RUNNING.name:
                total_time_elapsed = (datetime.now() - last_run.started_datetime).seconds
                last_run_status = f'{last_run.status} {total_time_elapsed:d}s'

            else:
                last_run_status = last_run.status

        except:
            last_run_status = None

        try:
            next_schedule = (
                model.flow_schedules.select()
                .order_by(FlowScheduleModel.schedule_datetime)
                .get()
            )
            next_schedule_datetime = next_schedule.schedule_datetime.isoformat(sep=' ', timespec='minutes')

        except:
            next_schedule_datetime = None

        flow_infos.append(OrderedDict({
            'Short Id': model.id.split('-')[0],
            'Name': model.name,
            'Path': model.path,
            'Active': model.active,
            'Last Status': last_run_status if last_run_status is not None else None,
            'Next Schedule': next_schedule_datetime if next_schedule_datetime is not None else None,
        }))

    print('Found', len(flow_models), 'flow(s) in the project.')
    if len(flow_models) == 0:
        return

    print(tabulate(
        flow_infos,
        headers='keys',
        tablefmt='simple_outline'
    ))
