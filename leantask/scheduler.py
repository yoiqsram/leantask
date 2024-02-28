import asyncio
import os
import subprocess
import sys
from concurrent import futures
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

from .context import GlobalContext
from .database import FlowModel, FlowRunModel, FlowScheduleModel, SchedulerSessionModel
from .discover import index_all_flows
from .enum import FlowIndexStatus, FlowRunStatus, FlowScheduleStatus
from .logging import get_logger
from .utils.script import has_sudo_access, sync_server_time
from .utils.string import generate_uuid, obj_repr, quote

logger = None


def execute_flow(
        flow_run_model: FlowRunModel,
        debug: bool = False
    ) -> FlowRunStatus:
    run_command = ' '.join((
        quote(sys.executable),
        quote(Path(flow_run_model.flow.path).resolve()),
        'run',
        '--run-id', str(flow_run_model.id),
        '--project-dir', quote(GlobalContext.PROJECT_DIR),
        '--scheduler-session-id', GlobalContext.SCHEDULER_SESSION_ID,
        '--debug' if debug else ''
    ))
    try:
        logger.info(f"Execute flow '{flow_run_model.flow.path}'.")
        run_process = subprocess.run(
            run_command,
            shell=True,
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        flow_run_status = FlowRunStatus(run_process.returncode)

    except Exception as exc:
        logger.error(f'{exc.__class__.__name__}: {exc}', exc_info=True)
        flow_run_status = FlowRunStatus.FAILED

    logger.info(f"Flow run status of '{flow_run_model.flow.path}': {flow_run_status.name}")
    return flow_run_status


def schedule_flow(
        flow_model: FlowModel,
        log_file_path: Path,
        debug: bool = False
    ) -> FlowScheduleStatus:
    schedule_command = ' '.join((
        quote(sys.executable),
        quote(flow_model.path),
        'schedule',
        '--project-dir', quote(GlobalContext.PROJECT_DIR),
        '--log-file', quote(log_file_path),
        '--scheduler-session-id', GlobalContext.SCHEDULER_SESSION_ID,
        '--debug' if debug else ''
    ))
    try:
        logger.info(f"Update schedule flow '{flow_model.name}'.")
        schedule_process = subprocess.run(
            schedule_command,
            shell=True,
            env=os.environ.copy()
        )
        flow_schedule_status = FlowScheduleStatus(schedule_process.returncode)

    except Exception as exc:
        logger.error(f'{exc.__class__.__name__}: {exc}', exc_info=True)
        flow_schedule_status = FlowScheduleStatus.FAILED

    logger.debug(f"Flow schedule status of '{flow_model.name}': {flow_schedule_status.name}")
    return flow_schedule_status


def execute_and_reschedule_flow(
        flow_run_model: FlowRunModel,
        log_file_path: Path,
        debug: bool = False
    ) -> Tuple[FlowRunStatus, FlowScheduleStatus]:
    flow_run_status = execute_flow(
        flow_run_model,
        debug=debug
    )
    flow_schedule_status = schedule_flow(
        flow_run_model.flow,
        log_file_path=log_file_path,
        debug=debug
    )
    return flow_run_status, flow_schedule_status


def get_unfinished_flow_run_models():
    unfinished_flow_run_status = (
        FlowRunStatus.SCHEDULED.name,
        FlowRunStatus.SCHEDULED_BY_USER.name,
        FlowRunStatus.RUNNING.name
    )
    unfinished_flow_run_models = []

    logger.debug('Get scheduled flow run.')
    flow_schedule_models = list(
        FlowScheduleModel().select()
        .where(FlowScheduleModel.schedule_datetime <= datetime.now())
    )
    for flow_schedule_model in flow_schedule_models:
        scheduled_flow_run_models = list(
            FlowRunModel.select()
            .where(
                (FlowRunModel.flow_schedule_id == flow_schedule_model.id)
                & FlowRunModel.status.in_(unfinished_flow_run_status)
            )
        )

        if len(scheduled_flow_run_models) == 0:
            logger.debug('Clean unknown schedule.')
            flow_schedule_model.delete_instance()
            continue

        for flow_run_model in scheduled_flow_run_models:
            logger.info(f"Add flow run of '{flow_run_model.flow.name}'.")
            unfinished_flow_run_models.append(flow_run_model)
    if len(unfinished_flow_run_models):
        logger.info(
            f'Found {len(unfinished_flow_run_models)} unfinished scheduled flow run(s).'
        )

    logger.debug('Get unscheduled flow run.')
    unscheduled_flow_run_models = list(
        FlowRunModel.select()
        .where(
            (FlowRunModel.flow_schedule_id >> None)
            & FlowRunModel.status.in_(unfinished_flow_run_status)
        )
    )
    if len(unscheduled_flow_run_models):
        logger.info(
            f'Found {len(unscheduled_flow_run_models)} unscheduled flow run(s).'
        )

    unfinished_flow_run_models += unscheduled_flow_run_models
    return unfinished_flow_run_models


class Scheduler:
    def __init__(
            self,
            worker: int = None,
            heartbeat: int = None,
            debug: bool = False
        ) -> None:
        self.id = generate_uuid()
        self.heartbeat = heartbeat if heartbeat is not None else GlobalContext.HEARTBEAT
        self.worker = worker if worker is not None else GlobalContext.WORKER
        self.log_path = GlobalContext.get_scheduler_session_log_file_path()

        global logger
        self.debug = debug
        GlobalContext.LOG_DEBUG = self.debug
        logger = get_logger('scheduler', self.log_path)
        logger.debug(repr(self))

        self._sync_server_time()
        self._create_scheduler_session()

        self._flow_models: List[FlowModel] = None

    def _sync_server_time(self) -> None:
        if has_sudo_access():
            try:
                logger.debug('Sync server time.')
                sync_server_time()
                logger.debug('Successfully sync time.')
            except LookupError:
                logger.warning('Failed to sync time to NTP server.')
        else:
            logger.warning(
                'Failed to sync time due to lack of permission. Please sync your time using this command: '
                'sudo ntpdate -s ntp.ubuntu.com'
            )

    def _create_scheduler_session(self) -> None:
        self._model = SchedulerSessionModel(
            id=self.id,
            heartbeat=self.heartbeat,
            worker=self.worker,
            log_path=str(self.log_path)
        )
        self._model.save(force_insert=True)

        GlobalContext.set_scheduler_session(self.id)

    async def run_routine(
            self,
            executor: futures.ThreadPoolExecutor = None
        ) -> None:
        logger.debug('Start run routine by updating flow indexes from database.')
        updated_flow_models = index_all_flows(self._flow_models, self.log_path)
        self._flow_models = list(updated_flow_models.keys())
        for flow_model in self._flow_models:
            if not flow_model.active \
                    or updated_flow_models[flow_model] in (
                        FlowIndexStatus.UNCHANGED, FlowIndexStatus.FAILED
                    ):
                continue

            schedule_flow(
                flow_model,
                log_file_path=self.log_path,
                debug=self.debug
            )

        for flow_run_model in get_unfinished_flow_run_models():
            flow_run_model.status = FlowRunStatus.PENDING.name
            flow_run_model.save()

            if executor is not None:
                logger.info(f"Submit flow run of '{flow_run_model.flow.path}' to the executor.")
                executor.submit(
                    execute_and_reschedule_flow,
                    flow_run_model,
                    self.log_path,
                    self.debug
                )
            else:
                execute_and_reschedule_flow(
                    flow_run_model,
                    self.log_path,
                    self.debug
                )

        logger.debug('Run routine has been completed.')

    async def run_loop(self) -> None:
        logger.info('Initialize update and schedule flow indexes from database.')
        updated_flow_models = index_all_flows(self._flow_models, self.log_path)
        self._flow_models = list(updated_flow_models.keys())
        for flow_model in self._flow_models:
            schedule_flow(
                flow_model,
                log_file_path=self.log_path,
                debug=self.debug
            )

        if self.worker > 0:
            with futures.ThreadPoolExecutor(max_workers=self.worker) as executor:
                while True:
                    logger.info('ALIVE')
                    delay_task = asyncio.create_task(asyncio.sleep(self.heartbeat))
                    routine_task = asyncio.create_task(self.run_routine(executor))
                    await routine_task
                    await delay_task

        else:
            while True:
                logger.info('ALIVE')
                delay_task = asyncio.create_task(asyncio.sleep(self.heartbeat))
                routine_task = asyncio.create_task(self.run_routine())
                await routine_task
                await delay_task

    def __repr__(self) -> str:
        return obj_repr(self, 'id', 'heartbeat', 'worker', 'log_path')
