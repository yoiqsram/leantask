from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Union

from ..database import TaskModel, TaskDownstreamModel, TaskRunModel
from ..database.log_models import TaskDownstreamLogModel
from ..enum import TaskRunStatus
from ..logging import get_task_run_logger
from ..utils.string import obj_repr, validate_use_safe_chars
from .base import ModelMixin
from .context import FlowContext
from .output import FileTaskOutput, ObjectTaskOutput, TaskOutput, UndefinedTaskOutput


class Task(ModelMixin):
    __model__ = TaskModel
    __refs__ = ('id', 'flow')

    def __init__(
            self,
            name: str,
            output_path: Path = None,
            retry_max: int = 0,
            retry_delay: int = 0,
            attrs: dict = None,
            flow = None,
            task_id: str = None
        ) -> None:
        self._upstreams: Set[Task] = set()
        self._downstreams: Set[Task] = set()

        self.name = name
        self.retry_max = retry_max
        self.retry_delay = retry_delay
        self.attrs = attrs.copy() if attrs is not None else dict()

        if output_path is not None and not isinstance(output_path, Path):
            raise TypeError(f"Task 'output_path' must be a Path, not '{type(output_path)}'.")        
        self.output_path = output_path
        self._output = UndefinedTaskOutput()
        self._runs: List[TaskRun] = []

        self.flow = flow

        super(Task, self).__init__(
            task_id,
            flow_id=self.flow.id,
            name=self.name
        )

    @property
    def flow_id(self) -> str:
        return self.flow.id

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        validate_use_safe_chars(value)
        self._name = value

    @property
    def upstreams(self) -> Task:
        return self._upstreams

    @property
    def downstreams(self) -> Task:
        return self._downstreams

    @property
    def flow(self):
        return self._flow

    @flow.setter
    def flow(self, value) -> None:
        if value is None:
            self._flow = FlowContext.get_current_flow()
        else:
            self._flow = value

        self._flow.add_task(self)

    @property
    def runs(self) -> List[TaskRun]:
        return self._runs

    def _setup_existing_model(
            self,
            __id: str = None,
            flow_id: str = None,
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

        elif flow_id is not None and name is not None:
            try:
                self._model = (
                    self.__model__.select()
                    .where(
                        (self.__model__.flow == self.flow_id)
                        & (self.__model__.name == name)
                    )
                    .limit(1)
                    [0]
                )
                self._model_exists = True

            except IndexError:
                pass

    def add_run(self, task_run: TaskRun) -> None:
        self._runs.append(task_run)

    def run(self, logger: logging.Logger) -> None:
        '''Replace this method to be implemented by your new subclass.'''
        raise NotImplemented("You need to define Task 'run' method.")

    def inputs(self) -> Dict[str, Any]:
        '''Get inputs of this task.'''
        task_inputs = dict()
        for task in self._upstreams:
            if isinstance(task._output, UndefinedTaskOutput):
                raise ValueError(f"Task '{task.name}' has no output.")

            elif isinstance(task._output, FileTaskOutput):
                if not task._output.exists():
                    raise ValueError(f"Output file from task '{task.name}' is not exists.")

            task_inputs[task.name] = task._output

        return task_inputs

    def output(self) -> TaskOutput:
        '''Get output of this task.'''
        if self.output_path is None:
            self._output = ObjectTaskOutput()
        else:
            self._output = FileTaskOutput(self.output_path)

        return self._output

    def requires(self, obj: Task) -> None:
        '''Set upstream task.'''
        if not isinstance(obj, Task):
            raise TypeError(f"Task could only be required by another Task, not '{type(obj)}'.")

        obj._downstreams.add(self)
        self._upstreams.add(obj)

    def iter_downstream(self) -> Iterable[Task]:
        '''Iterate all downstream tasks.'''
        for downstream_task in self._downstreams:
            yield downstream_task
            yield from downstream_task.iter_downstream()

    def save(self) -> None:
        super(Task, self).save()

        for downstream_task in self.iter_downstream():
            model = TaskDownstreamModel(
                task=self.id,
                downstream_task=downstream_task.id
            )
            model.save()

            log_model = TaskDownstreamLogModel(
                ref_id=model.id,
                ref_task=self.id,
                ref_downstream_task=downstream_task.id,
                created_datetime=self._model.created_datetime
            )
            log_model.save(force_insert=True)

    def __rshift__(self, obj: Union[Task, Iterable[Task]]) -> Union[Task, Iterable[Task]]:
        '''Task can be required by another Task or tuple/list of Task.'''
        if isinstance(obj, Task):
            obj.requires(self)

        elif isinstance(obj, tuple) or isinstance(obj, list):
            for _obj in obj:
                _obj.requires(self)

        else:
            raise TypeError(
                "Task could only be required by another Task or tuple/list of Task, "
                f"not '{type(obj)}'."
            )

        return obj

    def __repr__(self) -> str:
        return obj_repr(self, 'id', 'flow_id', 'name', 'retry_max', 'retry_delay')


class TaskRun(ModelMixin):
    __model__ = TaskRunModel
    __refs__ = ('id', 'flow_run', 'task')

    def __init__(
            self,
            flow_run,
            task: Task,
            attempt: int = 1,
            retry_max: int = None,
            retry_delay: int = None,
            status: TaskRunStatus = TaskRunStatus.PENDING,
            run_id: str = None
        ) -> None:
        self.task = task
        self.flow_run = flow_run
        self.attempt = attempt
        self.retry_max = retry_max if retry_max is not None else self.task.retry_max
        self.retry_delay = retry_delay if retry_delay is not None else self.task.retry_delay
        self.created_datetime: datetime = datetime.now()
        self.modified_datetime: datetime = self.created_datetime

        self._status = status

        super(TaskRun, self).__init__(
            run_id,
            flow_run_id=self.flow_run.id,
            task_id=self.task.id
        )

        self.logger = get_task_run_logger(
            self.flow_run.flow.id,
            self.task.id,
            self.id
        )

    @property
    def flow_run_id(self) -> str:
        return self.flow_run.id

    @property
    def task_id(self) -> str:
        return self.task.id

    @property
    def task_name(self) -> str:
        return self.task.name

    @property
    def status(self) -> TaskRunStatus:
        '''Return task run status.'''
        return self._status

    @status.setter
    def status(self, value: TaskRunStatus) -> None:
        if not isinstance(value, TaskRunStatus):
            raise TypeError(
                f"Run status of flow '{self.task.name}' should be 'TaskRunStatus' not '{type(value)}'."
            )

        if self._status == TaskRunStatus.DONE or (
                value not in (TaskRunStatus.UNKNOWN, TaskRunStatus.DONE)
                and value.value <= self._status.value
            ):
            raise ValueError(
                f"Run status of flow '{self.task.name}' cannot be set to similar or backward state from "
                f"'{self._status.name}' to '{value.name}'."
            )

        self.logger.debug(f"Set task run status from '{self._status.name}' to '{value.name}'.")
        self._status = value
        self._model.status = self._status
        self.modified_datetime = datetime.now()
        self._model.modified_datetime = self.modified_datetime
        self.save()

    def _setup_existing_model(
            self,
            __id: str = None,
            flow_run_id: str = None,
            task_id: str = None
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

        elif flow_run_id is not None and task_id is not None:
            try:
                self._model = (
                    self.__model__.select()
                    .where(
                        (self.__model__.flow_run == self.flow_run_id)
                        & (self.__model__.task == task_id)
                    )
                    .order_by(self.__model__.attempt.desc())
                    .limit(1)
                    [0]
                )
                self._model_exists = True

            except IndexError:
                pass

    def total_seconds(self) -> Union[float, None]:
        '''Return total seconds from task run start to task run end.'''
        if self.status in (
                TaskRunStatus.DONE,
                TaskRunStatus.FAILED,
                TaskRunStatus.FAILED_BY_USER,
            ):
            return (self.modified_datetime - self.created_datetime).total_seconds()

    def execute(self) -> None:
        '''Execute task run and record the status.'''
        logger = get_task_run_logger(
            self.flow_run.flow.id,
            self.task.id,
            self.id
        )

        try:
            self._start_datetime = datetime.now()
            self.status = TaskRunStatus.RUNNING
            self.task.run(logger=logger)
            self.status = TaskRunStatus.DONE

        except Exception as exc:
            self.status = TaskRunStatus.FAILED
            logger.error(f'{exc.__class__.__name__}: {exc}', exc_info=True)

    def next_attempt(self) -> TaskRun:
        task_run = TaskRun(
            self.flow_run,
            task=self.task,
            attempt=self.attempt + 1,
            retry_max=self.retry_max,
            retry_delay=self.retry_delay
        )
        self.flow_run.add_task_run(task_run)
        return task_run

    def __repr__(self) -> str:
        return obj_repr(self, 'id', 'flow_run_id', 'task_name', 'attempt', 'status')
