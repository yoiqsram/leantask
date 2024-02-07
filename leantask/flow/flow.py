from __future__ import annotations

import inspect
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Dict, List, Set, Union

from ..database.sqlite.execute import update_flow_run_status_to_db
from ..context import GlobalContext
from ..enum import FlowRunStatus, TaskRunStatus
from ..logging import get_logger
from ..utils.script import calculate_md5
from ..utils.string import generate_uuid, obj_repr, validate_use_safe_chars
from .context import FlowContext
from .schedule import Schedule
from .task import Task, TaskRun

logger = None


class FlowRun:
    def __init__(
            self,
            flow: Flow,
            status: FlowRunStatus = FlowRunStatus.UNKNOWN,
            max_delay: int = None,
            run_id: str = None,
            schedule_id: str = None,
            schedule_datetime: datetime = None
        ) -> None:
        self.id = run_id if run_id is not None else generate_uuid()
        self.schedule_id = schedule_id
        self.schedule_datetime = schedule_datetime
        self.max_delay = max_delay if max_delay is not None else flow.max_delay

        self.flow = flow
        self.created_datetime = datetime.now()
        self.modified_datetime = self.created_datetime

        self._tasks_ordered = flow.tasks_ordered
        self._task_runs: Dict[Task, TaskRun] = dict()
        self._status = status

    @property
    def flow_name(self) -> str:
        return self.flow.name

    @property
    def tasks_ordered(self) -> List[Task]:
        return self._tasks_ordered

    @property
    def status(self) -> FlowRunStatus:
        return self._status

    @status.setter
    def status(self, value: FlowRunStatus) -> None:
        if not isinstance(value, FlowRunStatus):
            raise TypeError(
                f"Run status of flow '{self.flow.name}' should be 'FlowRunStatus' not '{type(value)}'."
            )

        if self._status == FlowRunStatus.DONE or (
                value not in (FlowRunStatus.UNKNOWN, FlowRunStatus.DONE)
                and value.value <= self._status.value
            ):
            raise ValueError(
                f"Run status of flow '{self.flow.name}' cannot be set to similar or backward state from "
                f"'{self._status.name}' to '{value.name}'."
            )

        self._status = value
        self.modified_datetime = datetime.now()
        update_flow_run_status_to_db(self)

        if self._status in (
                FlowRunStatus.SCHEDULED,
                FlowRunStatus.SCHEDULED_BY_USER,
                FlowRunStatus.RUNNING
            ):
            for task in self.tasks_ordered:
                task_run = self.get_task_run(task)
                if task_run.status != TaskRunStatus.PENDING:
                    task_run.status = TaskRunStatus.PENDING

        elif self._status in (
                FlowRunStatus.CANCELED,
                FlowRunStatus.CANCELED_BY_USER
            ):
            for _, task_run in self._task_runs.values():
                if task_run.status == TaskRunStatus.PENDING:
                    task_run.status = TaskRunStatus.CANCELED

    def total_seconds(self) -> Union[float, None]:
        if self._status in (
                FlowRunStatus.FAILED,
                FlowRunStatus.DONE
            ):
            return (self.modified_datetime - self.created_datetime).total_seconds()

    def create_task_run(
            self,
            task: Task,
            attempt: int = 1,
            retry_max: int = None,
            retry_delay: int = None,
            status: TaskRunStatus = TaskRunStatus.UNKNOWN,
            run_id: str = None
        ) -> TaskRun:
        if task in self._task_runs:
            attempt = self._task_runs[task].attempt + 1 if attempt is None else attempt

        task_run = TaskRun(
            self,
            task=task,
            attempt=attempt,
            retry_max=retry_max,
            retry_delay=retry_delay,
            status=status,
            run_id=run_id
        )
        self._task_runs[task] = task_run
        return task_run

    def get_task_run(self, task: Task) -> TaskRun:
        return self._task_runs[task]

    def __repr__(self) -> str:
        return obj_repr(self, 'flow_name', 'status')


class Flow:
    '''Wrap task declaration using 'with Flow():' statement to make it runable.'''
    def __init__(
            self,
            name: str,
            description: str = None,
            cron_schedules: Union[str, List[str]] = None,
            start_datetime: datetime = None,
            end_datetime: datetime = None,
            max_delay: int = None,
            active: bool = True,
            flow_id: str = None
        ) -> None:
        if FlowContext.__defined__ is not None:
            raise RuntimeError('You can only define one flow.')

        self.id = flow_id if flow_id is not None else generate_uuid()
        self.name = name
        self.description = description
        self.schedule = Schedule(cron_schedules, start_datetime, end_datetime)
        self.max_delay = max_delay
        self.active = active

        self._path = Path(inspect.stack()[-1].filename).resolve()
        self._checksum = calculate_md5(self._path)

        self._tasks: Set[Task] = set()
        self._runs: List[FlowRun] = []

        FlowContext.__defined__ = self

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        validate_use_safe_chars(value)
        self._name = value

    @property
    def path(self) -> Path:
        return self._path

    @property
    def checksum(self) -> str:
        return self._checksum

    @property
    def tasks(self) -> Set[Task]:
        return self._tasks

    @property
    def tasks_ordered(self) -> List[Task]:
        ordered_tasks = []
        visited = set()

        def dfs(task):
            nonlocal visited
            if task in visited:
                return

            visited.add(task)
            for downstream_task in task.downstreams:
                dfs(downstream_task)

            ordered_tasks.append(task)

        for task in self._tasks:
            dfs(task)

        return ordered_tasks[::-1]

    @property
    def runs(self) -> List[FlowRun]:
        return self._runs

    def add_task(self, task: Task) -> None:
        '''Add task to the flow.'''
        task_names = set(task.name for task in self._tasks)
        for task in task.iter_downstream():
            if task.name in task_names:
                raise ValueError(f"'{task.name}' is already registered in the flow.")

            task_names.add(task.name)
            self._tasks.add(task)
            task._flow = self

    def add_run(self, flow_run: FlowRun) -> None:
        self._runs.append(flow_run)

    def add_run_from_cache(self, cache: Dict[str, Union[str, Dict[str, str]]]) -> None:
        flow_run = FlowRun(
            self,
            status=getattr(FlowRunStatus, cache['status']),
            max_delay=cache['max_delay'],
            run_id=cache['id'],
            schedule_id=cache.get('schedule_id'),
            schedule_datetime=cache.get('schedule_datetime')
        )

        tasks = {task.name: task for task in flow_run.tasks_ordered}
        for task_name, task_run in cache['tasks'].items():
            flow_run.create_task_run(
                tasks[task_name],
                attempt=task_run['attempt'],
                retry_max=task_run.get('retry_max'),
                retry_delay=task_run.get('retry_delay'),
                status=getattr(TaskRunStatus, task_run['status']),
                run_id=task_run['id']
            )

        self._runs.append(flow_run)

    def run(self) -> FlowRun:
        '''Run flow.'''
        global logger
        if logger is None:
            logger = get_logger(f'flow ({GlobalContext.relative_path(self.path)})')

        if len(self.runs) == 0 \
                or self.runs[-1].status in (
                    FlowRunStatus.CANCELED, FlowRunStatus.CANCELED_BY_USER,
                    FlowRunStatus.DONE, FlowRunStatus.FAILED
                ):
            logger.debug('Prepare for new run.')
            flow_run = FlowRun(self)
            for task in flow_run.tasks_ordered:
                flow_run.create_task_run(task)
            flow_run.status = FlowRunStatus.RUNNING

        else:
            logger.debug('Continue from last run.')
            flow_run = self.runs[-1]

            if flow_run.status in (
                    FlowRunStatus.SCHEDULED,
                    FlowRunStatus.SCHEDULED_BY_USER,
                ):
                flow_run.status = FlowRunStatus.RUNNING

        if flow_run.status != FlowRunStatus.RUNNING:
            logger.warning(
                f"Flow run is flagged as '{flow_run.status.name}' not '{FlowRunStatus.RUNNING.name}',"
                f" thus it will not be run."
            )
            return flow_run

        logger.debug('Prepare task run based on task order.')
        has_failed = False
        for task in flow_run.tasks_ordered:
            logger.debug(f"Prepare task run for '{task.name}'.")
            task_run = flow_run.get_task_run(task)

            if task_run.status != TaskRunStatus.PENDING:
                logger.debug(
                    f"Task '{task.name}' is flagged as '{task_run.status.name}' not '{TaskRunStatus.PENDING.name}',"
                    f" thus it will not be run."
                )
                continue

            while True:
                logger.debug(f"Execute task '{task.name}'.")
                task_run.execute()

                if task_run.status in (TaskRunStatus.DONE, TaskRunStatus.CANCELED):
                    logger.debug(f"Task '{task.name}' has been flagged as '{task_run.status.name}'.")
                    break

                if task_run.attempt > task_run.retry_max:
                    logger.debug(
                        f"Task '{task.name}' has run for {task_run.attempt} time(s)"
                        f" and reaching the maximum attempt of {task_run.retry_max}."
                    )
                    break

                logger.debug(f"Wait for {task_run.retry_delay} s before retrying.")
                sleep(task_run.retry_delay)
                logger.debug(f"Prepare new task run for '{task.name}' ({task_run.attempt + 1}).")
                task_run = flow_run.create_task_run(task, attempt=task_run.attempt + 1)

            if task_run.status in (TaskRunStatus.FAILED, TaskRunStatus.FAILED_BY_USER):
                logger.debug(f"Task '{task.name}' has failed on all of its attempts.")
                has_failed = True
                for downstream_task in task.iter_downstream():
                    if downstream_task == task:
                        continue

                    logger.debug(
                        f"Set task '{downstream_task.name}' status to '{TaskRunStatus.FAILED_UPSTREAM.name}'."
                    )
                    downstream_task_run = flow_run.get_task_run(downstream_task)
                    downstream_task_run.status = TaskRunStatus.FAILED_UPSTREAM

        if has_failed:
            logger.debug(
                f"Set flow status to '{FlowRunStatus.FAILED.name}' due to failure on at least a task."
            )
            flow_run.status = FlowRunStatus.FAILED
        else:
            logger.debug(f"Set flow status '{FlowRunStatus.DONE.name}'.")
            flow_run.status = FlowRunStatus.DONE

        return flow_run

    def next_schedule_datetime(self) -> Union[None, datetime]:
        # from datetime import timedelta
        # now = datetime.now() + timedelta(minutes=15)
        # return datetime(year=now.year, month=now.month, day=now.day, hour=now.hour, minute=now.minute)
        return

    def __repr__(self) -> str:
        return obj_repr(self, 'name', 'path', 'active')

    def __enter__(self) -> Flow:
        if FlowContext.__active__ is not None:
            raise RuntimeError(
                "There's already an active flow at the moment. "
                f' ({repr(FlowContext.__active__)})'
            )

        FlowContext.__active__ = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        FlowContext.__active__ = None

        from ..cli.flow import run_cli

        run_cli(self)
