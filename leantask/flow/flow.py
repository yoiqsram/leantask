from __future__ import annotations

import inspect
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import List, Set, Union

from ..context import GlobalContext
from ..database import FlowModel, FlowRunModel, database
from ..enum import FlowIndexStatus, FlowRunStatus, TaskRunStatus
from ..logging import get_flow_run_logger
from ..utils.script import calculate_md5
from ..utils.string import obj_repr, validate_use_safe_chars
from ..utils.tree import sort_tree_nodes
from .base import ModelMixin
from .context import FlowContext
from .schedule import Schedule
from .task import Task, TaskRun


class Flow(ModelMixin):
    __model__ = FlowModel
    __refs__ = ('id', )

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
        FlowContext.__defined__ = self

        self.name = name
        self.description = description
        self.max_delay = max_delay
        self.active = active

        if cron_schedules is not None:
            self._schedule = Schedule(cron_schedules, start_datetime, end_datetime)
        else:
            self._schedule = None

        try:
            self._path = GlobalContext.relative_path(Path(inspect.stack()[-1].filename).resolve())
            self._checksum = calculate_md5(self._path)
        except FileNotFoundError:
            self._path = None
            self._checksum = None

        self._tasks: Set[Task] = set()
        self._runs: List[FlowRun] = []

        super(Flow, self).__init__(flow_id, path=self._path, name=self.name)

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
    def cron_schedules(self) -> Union[str, None]:
        if self._schedule is None:
            return None
        return ','.join(self._schedule.cron_schedules)

    @property
    def start_datetime(self) -> Union[datetime, None]:
        if self._schedule is None:
            return None
        return self._schedule.start_datetime

    @property
    def end_datetime(self) -> Union[datetime, None]:
        if self._schedule is None:
            return None
        return self._schedule.end_datetime

    @property
    def checksum(self) -> str:
        return self._checksum

    @property
    def tasks(self) -> Set[Task]:
        return self._tasks

    @property
    def tasks_sorted(self) -> List[Task]:
        return sort_tree_nodes(self._tasks, 'downstreams')

    @property
    def runs(self) -> List[FlowRun]:
        return self._runs

    def _setup_existing_model(
            self,
            __id: str = None,
            path: str = None,
            name: str = None
        ) -> None:
        if __id is not None:
            try:
                self._model = (
                    self.__model__.select()
                    .where(self.__model__.id == __id)
                    .limit(1)
                    [0]
                )

                self._model_exists = True

            except IndexError:
                pass

        elif path is not None and name is not None:
            try:
                self._model = (
                    self.__model__.select()
                    .where(
                        (self.__model__.path == path)
                        & (self.__model__.name == name)
                    )
                    .limit(1)
                    [0]
                )
                self._model_exists = True

            except IndexError:
                pass

    def add_task(self, task: Task) -> None:
        if not isinstance(task, Task):
            raise TypeError()

        registered_task_names = set(task.name for task in self._tasks)
        new_tasks = [task] + list(task.iter_downstream())
        for task in new_tasks:
            if task.name in registered_task_names:
                raise ValueError(f"'{task.name}' is already registered in the flow.")

            self._tasks.add(task)
            task._flow = self

    def add_run(self, flow_run: FlowRun) -> None:
        if not isinstance(flow_run, FlowRun):
            raise TypeError()

        self._runs.append(flow_run)

    def run(self) -> FlowRun:
        if len(self.runs) == 0 \
                or self.runs[-1].status in (
                    FlowRunStatus.CANCELED, FlowRunStatus.CANCELED_BY_USER,
                    FlowRunStatus.DONE, FlowRunStatus.FAILED
                ):
            flow_run = FlowRun(self, status=FlowRunStatus.RUNNING)
            flow_run.create_all_task_runs()

        else:
            flow_run = self.runs[-1]
            if flow_run.status in (
                    FlowRunStatus.SCHEDULED,
                    FlowRunStatus.SCHEDULED_BY_USER,
                ):
                flow_run.status = FlowRunStatus.RUNNING

        if flow_run.status != FlowRunStatus.RUNNING:
            flow_run.logger.warning(
                f"Flow run is flagged as '{flow_run.status.name}' not '{FlowRunStatus.RUNNING.name}',"
                f" thus it will not be run."
            )
            return flow_run

        flow_run.execute()
        return flow_run

    def next_schedule_datetime(self, anchor_datetime: datetime = None) -> datetime:
        if self._schedule is None:
            return None

        return self._schedule.next_datetime(anchor_datetime)

    def save(self) -> None:
        with database.atomic():
            super(Flow, self).save()

            for task in self.tasks:
                task.save()

    def index(self) -> FlowIndexStatus:
        if self._model_exists and self._model.checksum == self.checksum:
            return FlowIndexStatus.UNCHANGED

        self.save()
        return FlowIndexStatus.UPDATED

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
        from ..cli.flow import run_cli

        FlowContext.__active__ = None
        run_cli(self)


class FlowRun(ModelMixin):
    __model__ = FlowRunModel
    __refs__ = ('id', 'flow', 'flow_schedule')

    def __init__(
            self,
            flow: Flow,
            max_delay: int = None,
            is_manual: bool = True,
            status: FlowRunStatus = FlowRunStatus.UNKNOWN,
            run_id: str = None,
            schedule_id: str = None,
            schedule_datetime: datetime = None
        ) -> None:
        self.max_delay = max_delay if max_delay is not None else flow.max_delay
        self.is_manual = is_manual
        self.flow_schedule_id = schedule_id
        self.schedule_datetime = schedule_datetime

        self.flow = flow
        self.created_datetime = datetime.now()
        self.modified_datetime = self.created_datetime

        self._task_runs_sorted: OrderedDict[Task, TaskRun] = OrderedDict()
        self._status = status

        super(FlowRun, self).__init__(run_id)

        self.logger = get_flow_run_logger(self.flow.id, self.id)

    @property
    def flow_id(self) -> str:
        return self.flow.id

    @property
    def flow_name(self) -> str:
        return self.flow.name

    @property
    def task_runs_sorted(self) -> OrderedDict[Task, TaskRun]:
        return self._task_runs_sorted

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
        self._model.status = self._status
        self.modified_datetime = datetime.now()
        self._model.modified_datetime = self.modified_datetime
        self.save()

        if self._status in (
                FlowRunStatus.SCHEDULED,
                FlowRunStatus.SCHEDULED_BY_USER,
                FlowRunStatus.RUNNING
            ):
            for task_run in self.task_runs_sorted:
                if task_run.status != TaskRunStatus.PENDING:
                    task_run.status = TaskRunStatus.PENDING

        elif self._status in (
                FlowRunStatus.CANCELED,
                FlowRunStatus.CANCELED_BY_USER
            ):
            for _, task_run in self._task_runs.values():
                if task_run.status == TaskRunStatus.PENDING:
                    task_run.status = TaskRunStatus.CANCELED

    def add_task_run(self, task_run: TaskRun) -> None:
        if not isinstance(task_run, TaskRun):
            raise TypeError()

        self._task_runs_sorted[task_run.task] = task_run

    def execute(self):
        self.logger.info(f"Run flow '{self.flow.name}'.")

        has_failed = False
        for task_run in self._task_runs_sorted.values():
            self.logger.debug(f"Prepare task run for '{task_run.task.name}'.")

            if task_run.status != TaskRunStatus.PENDING:
                self.logger.debug(
                    f"Task '{task_run.task.name}' is flagged as '{task_run.status.name}' not '{TaskRunStatus.PENDING.name}',"
                    f" thus it will not be run."
                )
                continue

            while True:
                self.logger.debug(f"Execute task '{task_run.task.name}' on {task_run.attempt + 1} attempt(s).")
                task_run.execute()

                if task_run.status in (TaskRunStatus.DONE, TaskRunStatus.CANCELED):
                    self.logger.debug(f"Task '{task_run.task.name}' has been flagged as '{task_run.status.name}'.")
                    break

                if task_run.attempt > task_run.retry_max:
                    self.logger.debug(
                        f"Task '{task_run.task.name}' has run for {task_run.attempt} time(s)"
                        f" and reaching the maximum attempt of {task_run.retry_max}."
                    )
                    break

                self.logger.debug(f"Wait for {task_run.retry_delay} s before retrying.")
                sleep(task_run.retry_delay)
                task_run = task_run.next_attempt()

            if task_run.status in (TaskRunStatus.FAILED, TaskRunStatus.FAILED_BY_USER):
                self.logger.debug(f"Task '{task_run.task.name}' has failed on all of its attempts.")
                has_failed = True
                for downstream_task_run in task_run.iter_downstream():
                    self.logger.debug(
                        f"Set task '{downstream_task_run.task.name}' status to '{TaskRunStatus.FAILED_UPSTREAM.name}'."
                    )
                    downstream_task_run.status = TaskRunStatus.FAILED_UPSTREAM

        if has_failed:
            self.logger.debug(
                f"Set flow status to '{FlowRunStatus.FAILED.name}' due to failure on at least a task."
            )
            self.status = FlowRunStatus.FAILED
        else:
            self.logger.debug(f"Set flow status '{FlowRunStatus.DONE.name}'.")
            self.status = FlowRunStatus.DONE

        self.logger.info(f"Flow run status: '{self.status.name}'.")

    def total_seconds(self) -> Union[float, None]:
        if self._status in (
                FlowRunStatus.FAILED,
                FlowRunStatus.DONE
            ):
            return (self.modified_datetime - self.created_datetime).total_seconds()

    def create_all_task_runs(self):
        for task in self.flow.tasks_sorted:
            task_run = TaskRun(
                self,
                task=task,
                attempt=1,
                retry_max=task.retry_max,
                retry_delay=task.retry_delay
            )
            self._task_runs_sorted[task] = task_run

    def get_task_run(self, task: Task) -> TaskRun:
        return self._task_runs[task]

    def __repr__(self) -> str:
        return obj_repr(self, 'flow_id', 'flow_name', 'status')

    @classmethod
    def from_database(cls, __id: str):
        model = cls.get_model(__id)

        return cls(
            name=model.name,
            description=model.description,
            cron_schedules=model.cron_schedules.split(',') if model.cron_schedules is not None else None,
            start_datetime=model.start_datetime,
            end_datetime=model.end_datetime,
            max_delay=model.max_delay,
            active=model.active,
            flow_id=model.id
        )
