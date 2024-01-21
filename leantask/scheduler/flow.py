from __future__ import annotations

import asyncio
import inspect
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Union

from leantask.cli.flow import run_flow_cli
from leantask.logging import logger, db_logger
from .context import FlowContext
from .schedule import Schedule
from .task import Task, TaskSkipped


class TaskRunStatus(Enum):
    DONE = 0
    RUNNING = 1
    PENDING = 2
    FAILED = 3
    FAILED_UPSTREAM = 4
    SKIPPED = 9


class FlowRunStatus(Enum):
    DONE = 0
    RUNNING = 1
    PENDING = 2
    FAILED = 3


class TaskRun:
    '''Track task run status.'''
    def __init__(
            self,
            task: Task
        ) -> None:
        self.task = task
        self.status = TaskRunStatus.PENDING
        self.scheduled_datetime = self.status_datetime
        self.run_datetime = None

    @property
    def status(self) -> TaskRunStatus:
        '''Return task run status.'''
        return self._status

    @status.setter
    def status(self, value: TaskRunStatus) -> None:
        if not isinstance(value, TaskRunStatus):
            raise TypeError(f"TaskRun'status' must be a TaskRunStatus, not '{type(value)}'.")

        self._status = value
        self.status_datetime = datetime.now()

        # TODO: Log task run status changes.
        # print(f"Task '{self.task.name}' run status changed to:", self._status)

    @property
    def total_seconds(self) -> float:
        '''Return total seconds from task run start to task run end.'''
        if self.status != TaskRunStatus.DONE:
            return

        return (self.status_datetime - self.run_datetime).total_seconds()

    def safe_run(self) -> None:
        '''Run task safely and record the status.'''
        try:
            self.run_datetime = datetime.now()
            self.status = TaskRunStatus.RUNNING
            self.task.run()
            self.status = TaskRunStatus.DONE

        except TaskSkipped:
            self.status = TaskRunStatus.SKIPPED
            return

        except Exception as exc:
            self.status = TaskRunStatus.FAILED
            # TODO: Log task run failure.
            print(f'{type(exc).__name__}:', exc)


class Flow:
    '''Wrap tasks into a flow to allow it to be executed.'''
    def __init__(
            self,
            name: str,
            description: str = None,
            cron_schedules: Union[str, List[str]] = None,
            start_datetime: datetime = None,
            end_datetime: datetime = None,
        ) -> None:
        self.name = self._validate_name(name)
        FlowContext.__names__ += (self.name,)

        self.description = description
        self.schedule = Schedule(cron_schedules, start_datetime, end_datetime)

        self.filepath = Path(inspect.stack()[-1].filename).resolve()

        self._tasks = set()
        self._task_run = []
        self._flow_run = []

    def _validate_name(self, name: str) -> str:
        '''Validate flow name whether it has already been registered.'''
        if not isinstance(name, str):
            raise TypeError(f"Flow name must be a string, not '{type(name)}'.")
        
        if name in FlowContext.__names__:
            raise TypeError(f"Flow name of '{name}' is already registered. Use different flow name.")

        return name

    def __enter__(self) -> Flow:
        if FlowContext.__active__ is not None:
            raise RuntimeError(
                "You can't have nested Flow definition. "
                "Make sure to exit from Flow before define another Flow."
            )

        FlowContext.__active__ = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        FlowContext.__active__ = None
        run_flow_cli(self)

    def add_task(self, task: Task) -> None:
        '''Add task to the flow.'''
        for _task in task.iter_downstream():
            self._tasks.add(_task)

    def _get_task_run_order(self) -> List[Task]:
        '''Get task run order.'''
        ordered_tasks = []
        visited = set()

        def dfs(task):
            nonlocal visited
            if task in visited:
                return

            visited.add(task)
            for downstream_task in task._downstream:
                dfs(downstream_task)

            ordered_tasks.append(task)

        for task in self._tasks:
            dfs(task)

        return ordered_tasks[::-1]

    def queue(self) -> None:
        '''Add flow run to scheduler queue.'''
        from .scheduler import Scheduler

    def run(self) -> None:
        '''Run flow.'''
        ordered_tasks = self._get_task_run_order()
        # TODO: Log flow run start.
        # print(f"Running flow '{self.name}'...")
        for task in ordered_tasks:
            task_retry = 0
            task_run = TaskRun(task)
            while task_retry <= task.retry:
                self._task_run.append(task_run)
                task_run.safe_run()

                if task_run.status != TaskRunStatus.FAILED:
                    break

                if task_retry < task.retry:
                    asyncio.run(asyncio.sleep(task.retry_delay))
                    task_run = TaskRun(task)

                task_retry += 1

            if task_run.status in (TaskRunStatus.FAILED, TaskRunStatus.SKIPPED):
                for downstream_task in task.iter_downstream():
                    if downstream_task == task:
                        continue

                    downstream_task_run = TaskRun(downstream_task)
                    self._task_run.append(downstream_task_run)

                    if task_run.status == TaskRunStatus.FAILED:
                        downstream_task_run.status = TaskRunStatus.FAILED_UPSTREAM
                    else:
                        downstream_task_run.status = TaskRunStatus.SKIPPED

                break
