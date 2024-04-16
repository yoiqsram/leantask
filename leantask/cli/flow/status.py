from __future__ import annotations

import argparse
from collections import OrderedDict
from datetime import datetime
from tabulate import tabulate
from typing import Callable, List, TYPE_CHECKING

from ...enum import FlowRunStatus

if TYPE_CHECKING:
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
    from ...database import FlowRunModel

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
    for model in flow_run_models:
        schedule_datetime = None
        if model.schedule_datetime is not None:
            schedule_datetime = model.schedule_datetime.isoformat(sep=' ', timespec='minutes')

        started_datetime = None
        if model.started_datetime is not None:
            started_datetime = model.started_datetime.isoformat(sep=' ', timespec='minutes')

        total_time_elapsed = None
        if model.status == FlowRunStatus.DONE.name \
                or model.status.startswith(FlowRunStatus.FAILED.name):
            total_time_elapsed = float((model.modified_datetime - model.started_datetime).seconds)
        elif model.started_datetime is not None:
            total_time_elapsed = float((datetime.now() - model.started_datetime).seconds)

        run_statuses.append(OrderedDict({
            'Short Run Id': model.id.split('-')[0],
            'Run/Schedule Datetime': schedule_datetime,
            'Execution datetime': started_datetime,
            'Status': model.status,
            'Time Elapsed (s)': total_time_elapsed
        }))

    print(tabulate(
        run_statuses,
        headers='keys',
        tablefmt='simple_outline',
        floatfmt='.1f'
    ))
