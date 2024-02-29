from __future__ import annotations

import inspect
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import List, Set, Union

from ..context import GlobalContext
from ..database import (
    FlowModel, FlowRunModel, FlowScheduleModel,
    TaskDownstreamModel, TaskDownstreamLogModel,
    database
)
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

        self._start_datetime = start_datetime
        self._end_datetime = end_datetime
        if cron_schedules is not None:
            self._cron_schedules = ','.join(cron_schedules)
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

        super(Flow, self).__init__(
            flow_id,
            path=self._path,
            name=self.name
        )

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
        return self._cron_schedules

    @property
    def start_datetime(self) -> Union[datetime, None]:
        return self._start_datetime

    @property
    def end_datetime(self) -> Union[datetime, None]:
        return self._end_datetime

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

    def _setup_model_from_fields(
            self,
            path: Path = None,
            name: str = None
        ) -> None:
        if path is not None:
            try:
                self._model = (
                    self.__model__.select()
                    .where(self.__model__.path == path)
                    .limit(1)
                    [0]
                )
                self._model_exists = True
                return

            except IndexError:
                pass

        if name is not None:
            try:
                self._model = (
                    self.__model__.select()
                    .where(self.__model__.name == name)
                    .limit(1)
                    [0]
                )
                self._model_exists = True
                return

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
                    FlowRunStatus.DONE, FlowRunStatus.FAILED,
                    FlowRunStatus.FAILED_TIMEOUT_DELAY, FlowRunStatus.FAILED_TIMEOUT_RUN
                ):
            flow_run = FlowRun(self, status=FlowRunStatus.RUNNING)
            flow_run.create_task_runs()

        else:
            flow_run = self.runs[-1]
            if flow_run.status in (
                    FlowRunStatus.SCHEDULED,
                    FlowRunStatus.SCHEDULED_BY_USER,
                    FlowRunStatus.PENDING
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

                for downstream_task in task.downstreams:
                    model = TaskDownstreamModel(
                        task=task.id,
                        downstream_task=downstream_task.id
                    )
                    model.save()

                    log_model = TaskDownstreamLogModel(
                        ref_id=model.id,
                        ref_task=task.id,
                        ref_downstream_task=downstream_task.id,
                        created_datetime=task._model.created_datetime
                    )
                    log_model.save(force_insert=True)

    def index(self) -> FlowIndexStatus:
        self._checksum = calculate_md5(self.path)
        if self._model_exists and self._model.checksum == self._checksum:
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
        import inspect
        import sys
        from ..cli.flow import run_cli

        FlowContext.__active__ = None

        main_script_path = Path(sys.argv[0]).resolve()
        main_caller_path = Path(inspect.stack()[1].filename).resolve()
        if main_caller_path == main_script_path:
            run_cli(self)

class FlowRun(ModelMixin):
    __model__ = FlowRunModel
    __refs__ = ('id', 'flow', 'flow_schedule')

    def __init__(
            self,
            flow: Flow,
            status: FlowRunStatus = FlowRunStatus.UNKNOWN,
            is_manual: bool = None,
            run_id: str = None,
            schedule_id: str = None,
            schedule_datetime: datetime = None
        ) -> None:
        self.flow = flow
        self.flow_schedule_id = schedule_id
        self.schedule_datetime = schedule_datetime
        self.is_manual = is_manual if is_manual is not None else True
        self.max_delay = self.flow.max_delay

        self.created_datetime = datetime.now()
        self.modified_datetime = self.created_datetime
        self.started_datetime: datetime = None

        self._task_runs: OrderedDict[Task, TaskRun] = OrderedDict()
        self._status = FlowRunStatus.UNKNOWN

        super(FlowRun, self).__init__(run_id)

        self.flow.add_run(self)

        self.logger = get_flow_run_logger(self.flow.id, self.id)

        if not self._model_exists:
            self.logger.debug(f"Set initial status for flow run to '{status.name}'.")
            self.status = status

        else:
            self._status = getattr(FlowRunStatus, self._status)
            self.flow_schedule_id = self._model.flow_schedule_id

    @property
    def flow_id(self) -> str:
        return self.flow.id

    @property
    def flow_name(self) -> str:
        return self.flow.name

    @property
    def task_runs_sorted(self) -> OrderedDict[Task, TaskRun]:
        return self._task_runs

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

        self.logger.debug(f"Set flow run status from '{self._status.name}' to '{value.name}'.")
        self._status = value
        self._model.status = self._status
        self.modified_datetime = datetime.now()
        self._model.modified_datetime = self.modified_datetime
        if self._status == FlowRunStatus.RUNNING:
            self.started_datetime = self.modified_datetime
            self._model.started_datetime = self.started_datetime
        self.save()

        if self._status == FlowRunStatus.RUNNING:
            for task_run in self.task_runs_sorted.values():
                if task_run.status != TaskRunStatus.PENDING:
                    task_run.status = TaskRunStatus.PENDING

        elif self._status in (
                FlowRunStatus.CANCELED,
                FlowRunStatus.CANCELED_BY_USER
            ):
            for _, task_run in self._task_runs.values():
                if task_run.status in (
                        TaskRunStatus.SCHEDULED,
                        TaskRunStatus.PENDING
                    ):
                    task_run.status = TaskRunStatus.CANCELED

    def add_task_run(self, task_run: TaskRun) -> None:
        if not isinstance(task_run, TaskRun):
            raise TypeError()

        self._task_runs[task_run.task] = task_run

    def execute(self) -> FlowRunStatus:
        self.logger.info(f"Run flow '{self.flow.name}'.")

        has_failed = False
        task_runs = self._task_runs.values()
        if len(task_runs) == 0:
            has_failed = True
            self.logger.error('No task run was found.')

        for task_run in self._task_runs.values():
            self.logger.debug(f"Prepare task run for '{task_run.task.name}'.")

            if task_run.status not in (
                    TaskRunStatus.SCHEDULED,
                    TaskRunStatus.PENDING
                ):
                self.logger.debug(
                    f"Task '{task_run.task.name}' is flagged as '{task_run.status.name}'"
                    f" thus it will not be run."
                )
                continue

            while True:
                task_run.execute()

                if task_run.status in (TaskRunStatus.DONE, TaskRunStatus.CANCELED):
                    break

                if task_run.attempt > task_run.retry_max:
                    self.logger.info(
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

        self.logger.debug('Delete schedule if exists.')
        if self.flow_schedule_id is not None:
            try:
                flow_schedule_model = (
                    FlowScheduleModel.select()
                    .where(FlowScheduleModel.id == self.flow_schedule_id)
                    .limit(1)
                    [0]
                )
                flow_schedule_model.delete_instance()
            except IndexError:
                pass

        return self._status

    def total_seconds(self) -> Union[float, None]:
        if self._status in (
                FlowRunStatus.DONE,
                FlowRunStatus.FAILED,
                FlowRunStatus.FAILED_TIMEOUT_DELAY,
                FlowRunStatus.FAILED_TIMEOUT_RUN
            ):
            return (self.modified_datetime - self.started_datetime).total_seconds()

    def create_task_runs(
            self,
            status: TaskRunStatus = TaskRunStatus.PENDING
        ) -> None:
        task_run_ids = dict()
        if self._model_exists:
            task_run_ids = {
                model.task.id: model.id
                for model in self._model.task_runs
            }

        for task in self.flow.tasks_sorted:
            TaskRun(
                self,
                task=task,
                attempt=1,
                status=status,
                run_id=task_run_ids.get(task.id)
            )

    def __repr__(self) -> str:
        return obj_repr(self, 'flow_id', 'flow_name', 'status')
