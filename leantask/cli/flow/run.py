import argparse
import sys
from typing import Callable

from ...context import GlobalContext
from ...database import FlowRunModel
from ...enum import FlowRunStatus
from ...flow import Flow, FlowRun
from ...logging import get_local_logger, get_logger
from ...utils.script import get_confirmation
from ...utils.string import quote

logger = None


def add_run_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'run',
        help='run flow',
        description='run flow'
    )
    parser.add_argument(
        '--run-id',
        help='Continue run based on flow run id.'
    )
    parser.add_argument(
        '--local', '-L',
        action='store_true',
        help=(
            'NOT RECOMMENDED. Run locally without using scheduler thus will not be logged. '
            'Please use this only for testing purposes.'
        )
    )
    parser.add_argument(
        '--force', '-F',
        action='store_true',
        help='NOT RECOMMENDED. Bypass any confirmation before run.'
    )
    parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )
    parser.add_argument(
        '--log-file',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '--scheduler-session-id',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help=argparse.SUPPRESS
    )

    return run_flow


def run_flow(
        args: argparse.Namespace,
        flow: Flow
    ) -> None:
    global logger
    if args.log_file is not None:
        logger = get_logger('flow.run', args.log_file)
    else:
        logger = get_local_logger('flow.run')

    logger.info(f"Run command: {' '.join([quote(sys.executable)] + sys.argv)}")

    GlobalContext.LOCAL_RUN = args.local
    GlobalContext.SCHEDULER_SESSION_ID = args.scheduler_session_id

    if args.run_id is not None:
        prepare_flow_from_database(
            flow,
            run_id=args.run_id,
            force=args.force
        )

    elif not args.force \
            and not args.local:
        if not get_confirmation(
                "It's always be better to schedule flow and let the scheduler to run flow.\n"
                'Are you sure you want to run it manually?',
                default=False
            ):
            logger.debug('User reject to run the flow manually.')
            raise SystemExit(FlowRunStatus.UNKNOWN.value)

        logger.debug('User confirmed to run the flow manually.')

        if not flow.active:
            if get_confirmation(
                    'Flow is currently inactive.\n'
                    'Are you sure you want to run it?',
                    default=False
                ):
                logger.debug('User reject to run the flow that is currently inactive.')
                raise SystemExit(FlowRunStatus.UNKNOWN.value)

            logger.debug('User confirmed to run the flow that is currently inactive.')

    if not flow._model_exists:
        logger.error(
            'Flow has not been indexed. Use this command to index the flow:\n'
            f'{quote(sys.executable)} {quote(flow.path)} index'
        )
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    if flow.checksum != flow._model.checksum:
        logger.error(
            'Flow has been changed from the last time indexed at '
            + flow._model.modified_datetime.isoformat(sep=' ', timespec='minutes') + '.\n'
            + 'Use this command to reindex the flow:\n'
            + f'{quote(sys.executable)} {quote(flow.path)} index'
        )
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    try:
        flow_run = flow.run()
        raise SystemExit(flow_run.status.value)

    except Exception as exc:
        logger.error(f'{exc.__class__.__name__}: {exc}', exc_info=True)
        raise SystemExit(FlowRunStatus.UNKNOWN.value)


def prepare_flow_from_database(
        flow: Flow,
        run_id: str,
        force: bool
    ):
    if not force and not flow.active:
        logger.error('Flow is currently inactive.')
        raise SystemExit(FlowRunStatus.CANCELED.value)

    try:
        keyword = run_id.replace('.', '') + '*'
        flow_run_model = (
            FlowRunModel.select()
            .where(FlowRunModel.id.like(keyword))
            .order_by(FlowRunModel.created_datetime)
            .limit(1)
            [0]
        )
    except IndexError:
        logger.error('No run was found.')
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    if flow.name != flow_run_model.flow.name:
        logger.error(
            f"Current flow '{flow.name}' is different to "
            f"flow run which from using '{flow_run_model.flow.name}'."
        )
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    if flow_run_model.flow.checksum != flow.checksum:
        logger.error(
            'Flow has been changed from the last time indexed at '
            + flow_run_model.flow.modified_datetime.isoformat(sep=' ', timespec='minutes') + '.\n'
            + 'Use this command to reindex the flow:\n'
            + f'{quote(sys.executable)} {quote(flow.path)} index'
        )
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    logger.debug('Prepare flow run from database.')
    try:
        flow_run = FlowRun(
            flow,
            run_id=flow_run_model.id
        )
        flow_run.create_task_runs()

    except:
        logger.error('An error occured while preparing flow run.', exc_info=True)
        raise SystemExit(FlowRunStatus.UNKNOWN.value)

    if flow_run.status in (
            FlowRunStatus.DONE,
            FlowRunStatus.FAILED,
            FlowRunStatus.FAILED_TIMEOUT_DELAY,
            FlowRunStatus.FAILED_TIMEOUT_RUN,
            FlowRunStatus.CANCELED,
            FlowRunStatus.CANCELED_BY_USER
        ):
        logger.error(f"Flow run has been already over with latest status '{flow_run.status.name}'.")
        raise SystemExit(FlowRunStatus.UNKNOWN.value)
