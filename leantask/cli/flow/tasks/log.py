from __future__ import annotations

import argparse
from datetime import datetime
from typing import Callable, TYPE_CHECKING

from ....context import GlobalContext
from ....utils.script import display_scrollable_text

if TYPE_CHECKING:
    from ...flow import Flow


def add_log_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'log',
        help='Show log of a run.',
        description='Show log of a run.'
    )
    parser.add_argument(
        'task_name',
        help='Flow name'
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
        '--attempt', '-A',
        type=int,
        default=1,
        help='Filter by attempt.'
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )

    return show_task_log


def show_task_log(
        args: argparse.Namespace,
        flow: Flow
    ) -> None:
    from ....database import FlowRunModel, TaskRunModel

    task = flow.get_task(args.task_name)

    log_dir = (
        GlobalContext.log_dir()
        / 'task_runs'
        / str(flow.id)
        / str(task.id)
    )

    if args.run_id is not None:
        try:
            keyword = args.run_id.replace('.', '') + '*'
            task_run_model = (
                task._model.task_runs
                .where(
                    TaskRunModel.id.like(keyword)
                    & TaskRunModel.attempt == args.attempt
                )
                .get()
            )
        except:
            raise IndexError(f"No run with id of '{args.run_id}'.")

    elif args.datetime is not None:
        try:
            schedule_datetime = datetime.fromisoformat(args.datetime)
            flow_run_model = (
                flow._model.flow_runs
                .where(FlowRunModel.schedule_datetime == schedule_datetime)
                .get()
            )
            task_run_model = (
                flow_run_model.task_runs
                .where(
                    TaskRunModel.task == task._model.id
                    & TaskRunModel.attempt == args.attempt
                )
                .get()
            )
        except:
            raise IndexError(f"No run with schedule datetime of '{schedule_datetime}'")

    else:
        try:
            task_run_model = (
                task._model.task_runs
                .where(TaskRunModel.attempt == args.attempt)
                .order_by(TaskRunModel.modified_datetime.desc())
                .get()
            )
        except:
            raise IndexError('No run history was found.')

    log_run_ids = [
        path.name[:-4]
        for path in log_dir.iterdir() if path.name.endswith('.log')
    ]
    if task_run_model.id not in log_run_ids:
        raise FileNotFoundError(f"Log of run with id '{task_run_model.id}' is missing in log directory.")

    with open(log_dir / (task_run_model.id + '.log')) as f:
        log_text = f.read()

    display_scrollable_text(log_text)
