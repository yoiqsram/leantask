from __future__ import annotations

import argparse
from collections import OrderedDict
from datetime import datetime
from tabulate import tabulate
from typing import Callable, List, TYPE_CHECKING

from ...enum import FlowRunStatus, TaskRunStatus

if TYPE_CHECKING:
    from ...database import FlowRunModel
    from ...flow import Flow


def add_status_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'status',
        help='Show latest run statuses.',
        description='Show latest run statuses.'
    )
    parser.add_argument(
        '--limit', '-l',
        default=15,
        type=int,
        help='Maximum number of runs info to be shown.'
    )
    parser.add_argument(
        '--tasks', '-T',
        action='store_true',
        help='Show all task stasuses.'
    )
    parser.add_argument(
        '--run-id', '-I',
        help='Filter by run id.'
    )
    parser.add_argument(
        '--datetime', '-D',
        help=(
            'Filter by run datetime (ISO format). '
            'Example: 2024-02-22 or 2024-02-22T10:00'
        )
    )
    parser.add_argument(
        '--status', '-S',
        help='Filter by run status.'
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )

    return show_run_statuses


def show_run_statuses(
        args: argparse.Namespace,
        flow: Flow
    ) -> None:
    from ...database import FlowRunModel, TaskRunModel

    if args.run_id is not None:
        keyword = args.run_id.replace('.', '') + '*'
        flow_run_models = list(
            flow._model.flow_runs
            .where(FlowRunModel.id.like(keyword))
            .order_by(FlowRunModel.modified_datetime.desc())
            .limit(1)
        )

    elif args.datetime is not None:
        schedule_datetime = datetime.fromisoformat(args.datetime)
        flow_run_models = list(
            flow._model.flow_runs
            .where(FlowRunModel.schedule_datetime == schedule_datetime)
            .order_by(FlowRunModel.modified_datetime.desc())
            .limit(1)
        )

    elif args.status is not None:
        if hasattr(FlowRunStatus, args.status.upper()):
            flow_run_models: List[FlowRunModel] = list(
                flow._model.flow_runs
                .where(FlowRunModel.status == args.status)
                .order_by(FlowRunModel.modified_datetime.desc())
                .limit(args.limit)
            )

        else:
            raise ValueError(f"Unknown status of '{args.status}'.")

    else:
        flow_run_models: List[FlowRunModel] = list(
            flow._model.flow_runs
            .order_by(FlowRunModel.modified_datetime.desc())
            .limit(args.limit)
        )

    if len(flow_run_models) == 0:
        print('No run history.')
        return

    run_statuses = []
    for flow_run_model in flow_run_models:
        schedule_datetime = None
        if flow_run_model.schedule_datetime is not None:
            schedule_datetime = flow_run_model.schedule_datetime.isoformat(sep=' ', timespec='minutes')

        started_datetime = None
        if flow_run_model.started_datetime is not None:
            started_datetime = flow_run_model.started_datetime.isoformat(sep=' ', timespec='minutes')

        total_time_elapsed = None
        if flow_run_model.status == FlowRunStatus.DONE.name \
                or flow_run_model.status.startswith(FlowRunStatus.FAILED.name):
            total_time_elapsed = float((flow_run_model.modified_datetime - flow_run_model.started_datetime).seconds)
        elif flow_run_model.started_datetime is not None:
            total_time_elapsed = float((datetime.now() - flow_run_model.started_datetime).seconds)

        if args.tasks:
            run_statuses.append(OrderedDict({
                'Short Run Id': flow_run_model.id.split('-')[0],
                'Run/Schedule Datetime': schedule_datetime,
                'Execution datetime': started_datetime,
                'Task Name': '-- Flow --',
                'Attempt': None,
                'Status': flow_run_model.status,
                'Time Elapsed (s)': total_time_elapsed
            }))

            task_run_models: List[TaskRunModel] = (
                flow_run_model.task_runs
                .order_by(TaskRunModel.modified_datetime)
            )
            for task_run_model in task_run_models:
                task_started_datetime = None
                if flow_run_model.started_datetime is not None:
                    task_started_datetime = flow_run_model.started_datetime.isoformat(sep=' ', timespec='minutes')

                task_total_time_elapsed = None
                if task_run_model.status == TaskRunStatus.DONE.name \
                        or task_run_model.status.startswith(TaskRunStatus.FAILED.name):
                    task_total_time_elapsed = float((task_run_model.modified_datetime - task_run_model.started_datetime).seconds)
                elif task_run_model.started_datetime is not None:
                    task_total_time_elapsed = float((datetime.now() - task_run_model.started_datetime).seconds)

                run_statuses.append(OrderedDict({
                    'Short Run Id': task_run_model.id.split('-')[0],
                    'Run/Schedule Datetime': None,
                    'Execution datetime': task_started_datetime,
                    'Task Name': task_run_model.task.name,
                    'Attempt': task_run_model.attempt,
                    'Status': task_run_model.status,
                    'Time Elapsed (s)': task_total_time_elapsed
                }))

            print(tabulate(
                run_statuses,
                headers='keys',
                tablefmt='simple_outline',
                floatfmt='.1f'
            ))
            run_statuses = []

        else:
            run_statuses.append(OrderedDict({
                'Short Run Id': flow_run_model.id.split('-')[0],
                'Run/Schedule Datetime': schedule_datetime,
                'Execution datetime': started_datetime,
                'Status': flow_run_model.status,
                'Time Elapsed (s)': total_time_elapsed
            }))

    if not args.tasks:
        print(tabulate(
            run_statuses,
            headers='keys',
            tablefmt='simple_outline',
            floatfmt='.1f'
        ))
