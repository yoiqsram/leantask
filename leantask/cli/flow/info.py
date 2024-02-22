import argparse
import sys
from typing import Callable, List

from ...database import FlowRunModel, FlowScheduleModel
from ...enum import FlowRunStatus
from ...flow import Flow
from ...utils.string import quote


def add_info_parser(subparsers) -> Callable:
    parser = subparsers.add_parser(
        'info',
        help='show flow information',
        description='show flow information'
    )
    parser.add_argument(
        '--all', '-a',
        action='store_true',
        help='Show more detailed info.'
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )

    return show_info


def show_info(
        args: argparse.Namespace,
        flow: Flow
    ) -> None:
    # Show flow name and description
    print(flow.name)
    print(' ' * 3, flow.description)

    # Show index status
    print()
    if not flow._model_exists:
        print(
            'Flow has not been indexed yet. Please index the flow using this command:\n'
            f'{quote(sys.executable)} {quote(flow.path)} index'
        )
    elif flow.checksum != flow._model.checksum:
        print('Flow has unindexed changes. Please reindex the flow using this command:\n'
            f'{quote(sys.executable)} {quote(flow.path)} index'
        )
    else:
        print('Flow has been indexed.')

    # Show list of tasks in the flow
    print()
    print(f'{len(flow.tasks)} tasks (sorted):')
    for task in flow.tasks_sorted:
        print(' ' * 3, '-', task.name)

    if not args.all:
        return

    # Show run statistics
    
    flow_run_models: List[FlowRunModel] = list(
        flow._model.flow_runs
        .where(
            FlowRunModel.status.in_([
                FlowRunStatus.DONE.name,
                FlowRunStatus.FAILED.name
            ])
        )
        .order_by(FlowRunModel.created_datetime)
    )
    print()
    if len(flow_run_models) > 0:
        print('Run statistics:')
        print(
            ' ' * 3,
            '- Last run status          :',
            flow_run_models[-1].status,
            f" (for {(flow_run_models[-1].modified_datetime - flow_run_models[-1].created_datetime).total_seconds():.2f}s",
            f"at {flow_run_models[-1].created_datetime.isoformat(sep=' ', timespec='seconds')})"
        )

        done_flow_run_models = [
            flow_run_model
            for flow_run_model in flow_run_models
            if flow_run_model.status == FlowRunStatus.DONE.name
        ]
        print(' ' * 3, '- Success runs             :', end=' ')
        if len(done_flow_run_models) > 0:
            print(
                len(done_flow_run_models),
                f" (latest run at {done_flow_run_models[-1].created_datetime.isoformat(sep=' ', timespec='seconds')})"
            )
        else:
            print(0)

        failed_flow_run_models = [
            flow_run_model
            for flow_run_model in flow_run_models
            if flow_run_model.status == FlowRunStatus.FAILED.name
        ]
        print(' ' * 3, '- Failed runs              :', end=' ')
        if len(failed_flow_run_models) > 0:
            print(
                len(failed_flow_run_models),
                f" (latest run at {failed_flow_run_models[-1].created_datetime.isoformat(sep=' ')})"
            )
        else:
            print(0)

        running_flow_run_models = [
            flow_run_model
            for flow_run_model in flow_run_models
            if flow_run_model.status == FlowRunStatus.RUNNING.name
        ]
        print(' ' * 3, '- Still running            :', end=' ')
        if len(running_flow_run_models) > 0:
            print(
                len(running_flow_run_models),
                f" (latest run at {running_flow_run_models[-1].created_datetime.isoformat(sep=' ')})"
            )
        else:
            print(0)

        print(' ' * 3, '- Avg. running time (DONE) :', end=' ')
        if len(done_flow_run_models) > 0:
            avg_running_time = sum([
                (flow_run_model.modified_datetime - flow_run_model.created_datetime).total_seconds()
                for flow_run_model in done_flow_run_models
            ]) / len(done_flow_run_models)
            print(f'{avg_running_time:.2f}s')

    else:
        print('No run history.')

    # Show flow schedules
    flow_schedule_models: List[FlowScheduleModel] = list(flow._model.flow_schedules)
    print()
    if len(flow_schedule_models) > 0:
        max_items = 10
        print(f'Scheduled run datetime - {len(flow_schedule_models)} schedule(s):')
        for i, flow_schedule_model in enumerate(flow_schedule_models):
            if i == max_items:
                print(' ' * 3, '- etc')
                break

            print(
                ' ' * 3,
                f"- {flow_schedule_model.schedule_datetime.isoformat(sep=' ', timespec='minutes')}"
            )

    else:
        print('No scheduled run.')
