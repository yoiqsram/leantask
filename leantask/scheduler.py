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
from .logging import create_log_file, get_logger
from .utils.cache import save_cache, clear_cache
from .utils.script import has_sudo_access, sync_server_time
from .utils.string import obj_repr

logger = None


def execute_and_reschedule_flow(
        flow_path: Path,
        flow_run: Dict[str, Union[str, Dict[str, str]]],
        debug: bool = False
    ) -> Tuple[FlowRunStatus, FlowScheduleStatus]:
    cache_key = save_cache(flow_run)

    run_command = ' '.join((
        sys.executable,
        str(flow_path),
        'run',
        '--project-dir', str(GlobalContext.PROJECT_DIR),
        '--cache', cache_key,
        '--debug' if debug else ''
    ))
    try:
        logger.debug(f"Execute command to run flow: {run_command}")
        run_process = subprocess.run(
            run_command,
            shell=True,
            env=os.environ.copy()
        )
        flow_run_status = FlowRunStatus(run_process.returncode)
        clear_cache(cache_key)

    except:
        flow_run_status = FlowRunStatus.FAILED

    logger.info(f"Flow run status of '{flow_run['name']}': {flow_run_status.name}")

    schedule_command = ' '.join((
        sys.executable,
        str(flow_path),
        'schedule',
        '--project-dir', str(GlobalContext.PROJECT_DIR),
        '--debug' if debug else ''
    ))
    try:
        logger.debug(f"Execute command to schedule flow: {schedule_command}")
        schedule_process = subprocess.run(
            schedule_command,
            shell=True,
            env=os.environ.copy()
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
        self.heartbeat = heartbeat
        self.worker = worker
        self.log_path = create_log_file()

        self.flow_records = None

        global logger
        self.debug = debug
        GlobalContext.LOG_DEBUG = self.debug
        logger = get_logger('scheduler')

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

        logger.debug(repr(self))
        self.id = create_scheduler_session(self.heartbeat, self.worker)
        logger.info(f'New scheduler session has been initialized ({self.id}).')

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
                executor.submit(execute_and_reschedule_flow, flow_path, flow_run, self.debug)
            else:
                execute_and_reschedule_flow(flow_path, flow_run, self.debug)

        logger.debug('Run routine has been completed.')

    async def run_loop(self) -> None:
        with futures.ThreadPoolExecutor(max_workers=self.worker) as executor:
            while True:
                logger.info('ALIVE')
                delay_task = asyncio.create_task(asyncio.sleep(self.heartbeat))
                self.run_routine(executor)
                await delay_task
