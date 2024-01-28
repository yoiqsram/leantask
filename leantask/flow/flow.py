from __future__ import annotations

import inspect
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Dict, List, Set, Union

from ..database.sqlite.execute import update_flow_run_status_to_db
from ..enum import FlowRunStatus, TaskRunStatus
from ..utils.script import calculate_md5
from ..utils.string import generate_uuid, validate_use_safe_chars
from .context import FlowContext
from .schedule import Schedule
from .task import Task, TaskRun


class FlowRun:
    def __init__(
            self,
            flow: Flow,
            schedule_datetime: datetime = None,
            status: FlowRunStatus = FlowRunStatus.UNKNOWN,
            schedule_id: str = None,
            __id: str = None
        ) -> None:
        self.id = __id if __id is not None else generate_uuid()
        self.flow = flow
        self.flow.add_run(self)
        self.schedule_datetime = schedule_datetime
        self.schedule_id = schedule_id
        self.created_datetime = datetime.now()
        self.modified_datetime = self.created_datetime

        self._tasks_ordered = flow.tasks_ordered
        self._task_runs: Dict[Task, TaskRun] = dict()
        self._status = status

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
                f"Run status should be 'FlowRunStatus' not '{type(value)}'."
            )

        if self._status == FlowRunStatus.DONE or (
                value not in (FlowRunStatus.UNKNOWN, FlowRunStatus.DONE)
                and value.value <= self._status.value
            ):
            raise ValueError(
                'Run status cannot be set to similar or backward state from '
                f"'{self._status.name}' to '{value.name}'."
            )

        if value in (
                FlowRunStatus.SCHEDULED,
                FlowRunStatus.SCHEDULED_BY_USER,
                FlowRunStatus.RUNNING
            ):
            for task in self.tasks_ordered:
                task_run = self.get_task_run(task)
                task_run.status = TaskRunStatus.PENDING

        elif value in (
                FlowRunStatus.CANCELED,
                FlowRunStatus.CANCELED_BY_USER
            ):
            for _, task_run in self._task_runs.values():
                if task_run.status == TaskRunStatus.PENDING:
                    task_run.status = TaskRunStatus.CANCELED

        self._status = value
        self.modified_datetime = datetime.now()
        update_flow_run_status_to_db(self)

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
            status: TaskRunStatus = TaskRunStatus.UNKNOWN
        ) -> TaskRun:
        if task in self._task_runs:
            attempt = self._task_runs[task].attempt + 1 if attempt is None else attempt

        task_run = TaskRun(
            self,
            task,
            attempt=attempt,
            status=status
        )
        self._task_runs[task] = task_run
        return task_run

    def get_task_run(self, task: Task) -> TaskRun:
        return self._task_runs[task]


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
            __id: str = None,
        ) -> None:
        if FlowContext.__defined__ is not None:
            raise RuntimeError('You can only define one flow.')

        self.id = __id if __id is not None else generate_uuid()
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

    def add_run(self, flow_run: FlowRun) -> None:
        self._runs.append(flow_run)

    @property
    def runs(self) -> List[FlowRun]:
        return self._runs

    def __repr__(self) -> str:
        return f'<Flow name={self.name}>'

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
        self.cli()

    def cli(self) -> None:
        from ..cli.flow import run_cli

        run_cli(self)

    def add_task(self, task: Task) -> None:
        '''Add task to the flow.'''
        for task in task.iter_downstream():
            self._tasks.add(task)

    def run(
            self,
            flow_run: FlowRun = None,
            verbose: bool = False
        ) -> FlowRun:
        '''Run flow.'''
        if flow_run is None:
            flow_run = FlowRun(self)
            for task in flow_run.tasks_ordered:
                flow_run.create_task_run(task)

            flow_run.status = FlowRunStatus.RUNNING

        if flow_run.status in (
                FlowRunStatus.SCHEDULED,
                FlowRunStatus.SCHEDULED_BY_USER,
            ):
            flow_run.status = FlowRunStatus.RUNNING

        if flow_run.status != FlowRunStatus.RUNNING:
            return flow_run

        # TODO: Log flow run start.
        # print(f"Running flow '{self.name}'...")
        # print(ordered_tasks)
        has_failed = False
        for task in flow_run.tasks_ordered:
            task_run = flow_run.get_task_run(task)

            if task_run.status != TaskRunStatus.PENDING:
                continue

            while task_run.attempt <= task.retry_max + 1:
                task_run.execute()

                if task_run.status in (TaskRunStatus.DONE, TaskRunStatus.CANCELED):
                    break

                if task_run.attempt < task.retry_max:
                    sleep(task.retry_delay)

                task_run = flow_run.create_task_run(task, attempt=task_run.attempt + 1)

            if task_run.status in (TaskRunStatus.FAILED, TaskRunStatus.FAILED_BY_USER):
                has_failed = True
                for downstream_task in task.iter_downstream():
                    if downstream_task == task:
                        continue

                    downstream_task_run = flow_run.get_task_run(downstream_task)
                    downstream_task_run.status = TaskRunStatus.FAILED_UPSTREAM
        
        flow_run.status = FlowRunStatus.DONE if not has_failed else FlowRunStatus.FAILED
        return flow_run

    def next_schedule_datetime(self):
        from datetime import timedelta
        now = datetime.now() + timedelta(minutes=15)
        return datetime(year=now.year, month=now.month, day=now.day, hour=now.hour, minute=now.minute)