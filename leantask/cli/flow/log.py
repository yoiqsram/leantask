from __future__ import annotations

import argparse
from datetime import datetime
from typing import Callable, TYPE_CHECKING

from ...context import GlobalContext
from ...utils.script import display_scrollable_text

if TYPE_CHECKING:
    from ...flow import Flow


def add_log_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'log',
        help='Show log of a run.',
        description='Show log of a run.'
    )
    add_log_arguments(parser)

    return show_log


def add_log_arguments(parser: argparse.ArgumentParser) -> None:
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
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )


def show_log(
        args: argparse.Namespace,
        flow: Flow
    ) -> None:
    from ...database import FlowRunModel

    log_dir = (
        GlobalContext.log_dir()
        / 'flow_runs'
        / str(flow.id)
    )

    if args.run_id is not None:
        try:
            keyword = args.run_id.replace('.', '') + '*'
            run_model = (
                flow._model.flow_runs
                .where(FlowRunModel.id.like(keyword))
                .get()
            )
        except:
            raise IndexError(f"No run with id of '{args.run_id}'.")

    elif args.datetime is not None:
        try:
            schedule_datetime = datetime.fromisoformat(args.datetime)
            run_model = (
                flow._model.flow_runs
                .where(FlowRunModel.schedule_datetime == schedule_datetime)
                .get()
            )
        except:
            raise IndexError(f"No run with schedule datetime of '{schedule_datetime}'")

    else:
        try:
            run_model = (
                flow._model.flow_runs
                .order_by(FlowRunModel.modified_datetime.desc())
                .get()
            )
        except:
            raise IndexError('No run history was found.')

    log_run_ids = [
        path.name[:-4]
        for path in log_dir.iterdir() if path.name.endswith('.log')
    ]
    if run_model.id not in log_run_ids:
        raise FileNotFoundError(f"Log of run with id '{run_model.id}' is missing in log directory.")

    with open(log_dir / (run_model.id + '.log')) as f:
        log_text = f.read()

    display_scrollable_text(log_text)
