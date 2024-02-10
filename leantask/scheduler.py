import asyncio
import os
import subprocess
import sys
from concurrent import futures
from pathlib import Path
from typing import Dict, Tuple, Union

from .context import GlobalContext
from .database.execute import create_scheduler_session, get_scheduled_run_tasks
from .discover import update_flow_records
from .enum import FlowRunStatus, FlowScheduleStatus
from .logging import get_scheduler_session_logger
from .utils.cache import save_cache, clear_cache
from .utils.script import has_sudo_access, sync_server_time
from .utils.string import generate_uuid, obj_repr, quote

logger = None


def execute_and_reschedule_flow(
        flow_path: Path,
        flow_run: Dict[str, Union[str, Dict[str, str]]],
        log_file_path: Path,
        scheduler_session_id: str,
        debug: bool = False
    ) -> Tuple[FlowRunStatus, FlowScheduleStatus]:
    cache_key = save_cache(flow_run)

    run_command = ' '.join((
        quote(sys.executable),
        quote(flow_path),
        'run',
        '--project-dir', quote(GlobalContext.PROJECT_DIR),
        '--cache', cache_key,
        '--log-file', quote(log_file_path),
        '--scheduler-session-id', scheduler_session_id,
        '--debug' if debug else ''
    ))
    try:
        logger.info(f"Execute flow '{flow_run['name']}'.")
        run_process = subprocess.run(
            run_command,
            shell=True,
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        flow_run_status = FlowRunStatus(run_process.returncode)
        clear_cache(cache_key)

    except:
        flow_run_status = FlowRunStatus.FAILED

    logger.info(f"Flow run status of '{flow_run['name']}': {flow_run_status.name}")

    schedule_command = ' '.join((
        quote(sys.executable),
        quote(flow_path),
        'schedule',
        '--project-dir', quote(GlobalContext.PROJECT_DIR),
        '--log-file', quote(log_file_path),
        '--scheduler-session-id', scheduler_session_id,
        '--debug' if debug else ''
    ))
    try:
        logger.info(f"Update schedule flow '{flow_run['name']}'.")
        schedule_process = subprocess.run(
            schedule_command,
            shell=True,
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        flow_schedule_status = FlowScheduleStatus(schedule_process.returncode)

    except:
        flow_schedule_status = FlowScheduleStatus.FAILED

    logger.debug(f"Flow '{flow_run['name']}' schedule status: {flow_schedule_status.name}")

    return flow_run_status, flow_schedule_status


class Scheduler:
    def __init__(
            self,
            worker: int = 1,
            heartbeat: int = 30,
            debug: bool = False
        ) -> None:
        self.id = generate_uuid()
        self.heartbeat = heartbeat
        self.worker = worker
        self.log_path = GlobalContext.get_scheduler_session_log_file_path(self.id)
        self.flow_records = None

        create_scheduler_session(
            self.id,
            self.heartbeat,
            self.worker,
            self.log_path
        )

        global logger
        self.debug = debug
        GlobalContext.LOG_DEBUG = self.debug
        logger = get_scheduler_session_logger(self.id)
        logger.debug(repr(self))
        logger.info(f'New scheduler session has been initialized ({self.id}).')

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

    def __repr__(self) -> str:
        return obj_repr(self, 'heartbeat', 'worker', 'log_path')

    def run_routine(
            self,
            executor: futures.ThreadPoolExecutor = None
        ) -> None:
        logger.debug('Start run routine by updating flow indexes on database.')
        self.flow_records = update_flow_records(self.flow_records)

        logger.debug('Get flow and task run active schedules on database.')
        schedules = get_scheduled_run_tasks()
        logger.debug(
            f"Total flow schedules found: {len(schedules)}"
            f" ({sum(len(flow_run['tasks']) for flow_run in schedules.values())} tasks)."
        )
        for flow_path, flow_run in schedules.items():
            if executor is not None:
                logger.info(f"Submit flow run of '{flow_path}' to the executor.")
                executor.submit(execute_and_reschedule_flow, flow_path, flow_run, self.log_path, self.id, self.debug)
            else:
                execute_and_reschedule_flow(flow_path, flow_run, self.log_path, self.id, self.debug)

        logger.debug('Run routine has been completed.')

    async def run_loop(self) -> None:
        with futures.ThreadPoolExecutor(max_workers=self.worker) as executor:
            while True:
                logger.info('ALIVE')
                delay_task = asyncio.create_task(asyncio.sleep(self.heartbeat))
                self.run_routine(executor)
                await delay_task