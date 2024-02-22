import argparse
from datetime import datetime
from typing import Callable, List

from ...database import FlowRunModel, TaskRunModel
from ...enum import FlowRunStatus, TaskRunStatus
from ...flow import Flow


def add_runs_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'runs',
        help='Show latest runs info.',
        description='Show latest runs info.'
    )
    option = parser.add_subparsers(
        dest='option',
        required=True,
        help='runs options'
    )

    list_option = option.add_parser(
        'list',
        help='Show list of latest runs info.'
    )
    list_option.add_argument(
        '--limit', '-l',
        default=10,
        type=int,
        help='Maximum number of runs info to be shown.'
    )
    list_option.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )

    search_option = option.add_parser(
        'search',
        help='Search run info.'
    )
    search_option.add_argument(
        'keyword',
        help='Search keyword. Default to flow run id.'
    )
    search_option.add_argument(
        '--by-task',
        action='store_true',
        help='Enable to search by task run id.'
    )
    search_option.add_argument(
        '--by-datetime',
        action='store_true',
        help=(
            'Enable to search using datetime (ISO format). '
            'Example: 2024-02-22 or 2024-02-22T10:00'
        )
    )

    return show_runs


def show_runs(
        args: argparse.Namespace,
        flow: Flow
    ) -> None:
    if args.option == 'list':
        flow_run_models: List[FlowRunModel] = list(
            flow._model.flow_runs
            .order_by(FlowRunModel.modified_datetime.desc())
            .limit(args.limit)
        )

        if len(flow_run_models) == 0:
            print('No run history.')
            return

        for flow_run_model in flow_run_models:
            show_flow_run_info(flow, flow_run_model)
            print()

    elif args.option == 'search':
        if args.by_datetime:
            schedule_datetime = datetime.fromisoformat(args.keyword)
            flow_run_models = list(
                flow._model.flow_runs
                .where(FlowRunModel.schedule_datetime == schedule_datetime)
                .order_by(FlowRunModel.modified_datetime.desc())
                .limit(1)
            )

        elif args.by_task:
            keyword = args.keyword.replace('.', '') + '*'
            flow_run_models = list(
                flow._model.flow_runs
                .join(TaskRunModel)
                .where(TaskRunModel.id.like(keyword))
                .order_by(TaskRunModel.modified_datetime.desc())
                .limit(1)
            )

        else:
            keyword = args.keyword.replace('.', '') + '*'
            flow_run_models = list(
                flow._model.flow_runs
                .where(FlowRunModel.id.like(keyword))
                .order_by(FlowRunModel.modified_datetime.desc())
                .limit(1)
            )

        if len(flow_run_models) == 0:
            print('No run history.')
            return

        show_flow_run_info(
            flow,
            flow_run_models[0],
            task_info=True
        )


def show_flow_run_info(
        flow: Flow,
        flow_run_model: FlowRunModel,
        task_info: bool = False
    ) -> None:
    print(
        f'{flow.name}',
        f" {str(flow_run_model.id).split('-')[0]}..."
    )

    if flow_run_model.schedule_datetime is not None:
        print(
            ' ', 'Schedule datetime :',
            flow_run_model.schedule_datetime.isoformat(sep=' ', timespec='minutes')
        )

    if flow_run_model.started_datetime is not None:
        print(
            ' ', 'Started datetime  :',
            flow_run_model.started_datetime.isoformat(sep=' ', timespec='milliseconds')
            + (
                ' (Manual run)'
                if flow_run_model.is_manual
                else ''
            )
        )

    print(
        ' ', 'Status            :',
        flow_run_model.status
    )

    if flow_run_model.status in (
            FlowRunStatus.DONE.name,
            FlowRunStatus.FAILED.name
        ):
        print(
            ' ', 'Time elapsed      :',
            f'{(flow_run_model.modified_datetime - flow_run_model.started_datetime).total_seconds():.2f}s'
        )

    elif flow_run_model.status == FlowRunStatus.RUNNING.name:
        print(
            ' ', 'Time elapsed      :',
            f'{(datetime.now() - flow_run_model.started_datetime).total_seconds():.2f}s'
        )

    if task_info:
        task_run_models: List[TaskRunModel] = list(
            flow_run_model.task_runs
            .order_by(TaskRunModel.created_datetime)
        )
        print(' ', 'Tasks             :', end=' ')
        if len(task_run_models) == 0:
            print('No task run was found')
            return

        done_task_run_count = len([
            1 for task_run_model in task_run_models
            if task_run_model.status == TaskRunStatus.DONE.name
        ])
        failed_task_run_count = len([
            1 for task_run_model in task_run_models
            if task_run_model.status in (
                TaskRunStatus.FAILED.name,
                TaskRunStatus.FAILED_BY_USER.name,
                TaskRunStatus.FAILED_UPSTREAM.name,
            )
        ])
        running_task_run_count = len([
            1 for task_run_model in task_run_models
            if task_run_model.status in (
                TaskRunStatus.PENDING.name,
                TaskRunStatus.RUNNING.name
            )
        ])
        print(
            f'{len(task_run_models)} task run(s).',
            f' {done_task_run_count} DONE',
            f'| {failed_task_run_count} FAILED',
            f'| {running_task_run_count} PENDING/RUNNING.'
        )

        print()
        for task_run_model in task_run_models:
            show_task_run_info(task_run_model)
            print()


def show_task_run_info(task_run_model: TaskRunModel) -> None:
    print(
        f'  {task_run_model.task.name} ',
        f"{str(task_run_model.id).split('-')[0]}..."
    )

    print(
        '   ', 'Status            :',
        task_run_model.status
    )

    if task_run_model.status in (
            FlowRunStatus.DONE.name,
            FlowRunStatus.FAILED.name
        ):
        print(
            '   ', 'Time elapsed      :',
            f'{(task_run_model.modified_datetime - task_run_model.started_datetime).total_seconds():.2f}s'
        )

    elif task_run_model.status == FlowRunStatus.RUNNING.name:
        print(
            '   ', 'Time elapsed      :',
            f'{(datetime.now() - task_run_model.started_datetime).total_seconds():.2f}s'
        )
