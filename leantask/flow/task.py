from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Set, Union

from ..database.sqlite.execute import update_task_run_status_to_db
from ..enum import TaskRunStatus
from ..logging import get_task_run_logger
from ..utils.string import generate_uuid, obj_repr, validate_use_safe_chars
from .context import FlowContext, TaskContext
from .output import FileTaskOutput, ObjectTaskOutput, TaskOutput, UndefinedTaskOutput


class TaskRun:
    '''Track task run status.'''
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
        self.id = run_id if run_id is not None else generate_uuid()
        self.task = task
        self.flow_run = flow_run
        self.attempt = attempt
        self.retry_max = retry_max if retry_max is not None else self.task.retry_max
        self.retry_delay = retry_delay if retry_delay is not None else self.task.retry_delay
        self.created_datetime: datetime = datetime.now()
        self.modified_datetime: datetime = self.created_datetime

        self._status = status

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

        self._status = value
        self.modified_datetime = datetime.now()
        update_task_run_status_to_db(self)

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
            self.id,
        )

        try:
            self._start_datetime = datetime.now()
            self.status = TaskRunStatus.RUNNING
            self.task.run(logger=logger)
            self.status = TaskRunStatus.DONE

        except Exception as exc:
            self.status = TaskRunStatus.FAILED
            logger.error(f'{exc.__class__.__name__}: {exc}', exc_info=True)

    def __repr__(self) -> str:
        return obj_repr(self, 'task_name', 'attempt', 'status')


class Task:
    '''Basic Task class

    Define run method to be implemented by your new subclass.
    Use self.inputs() and self.output() to access task inputs and outputs.
    '''
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

        self.id = task_id if task_id is not None else generate_uuid()
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

    def __repr__(self) -> str:
        return obj_repr(self, 'name', 'retry_max', 'retry_delay')

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

    def __repr__(self) -> str:
        return obj_repr(self, 'name', 'retry_max', 'retry_delay')

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

    def iter_downstream(self) -> Iterable[Task]:
        '''Iterate all downstream tasks.'''
        yield self
        for downstream_task in self._downstreams:
            yield from downstream_task.iter_downstream()


def task(
        *args,
        attrs: dict = None,
        output_file: bool = False,
    ) -> Callable:
    '''Use @task decorator on your function to make it run as a Task.'''
    def task_decorator(func: Callable) -> Callable:
        def task_register(
                *task_args,
                task_name: str = None,
                task_output_path: Path = None,
                task_retry_max: int = 0,
                task_retry_delay: int = 0,
                task_flow = None,
                **task_kwargs
            ) -> Task:
            '''Register a new task function.'''
            if task_name is None:
                task_name = func.__name__

            if task_name in TaskContext.__names__:
                raise ValueError(
                    f"There's already a Task named '{task_name}'."
                    " Define a specific name or change your function name."
                )

            if 'attrs' in task_kwargs:
                raise ValueError(
                    "Task 'attrs' is a reserved keyword. "
                    "Please use different keyword name."
                )

            if 'inputs' in task_kwargs:
                raise ValueError(
                    "Task 'inputs' is a reserved keyword. "
                    "Please use different keyword name."
                )

            if output_file and task_output_path is None:
                raise AttributeError("Task 'task_output_path' should be filled.")

            class PythonTask(Task):
                def __init__(self):
                    super().__init__(
                        name=task_name,
                        output_path=task_output_path,
                        retry_max=task_retry_max,
                        retry_delay=task_retry_delay,
                        attrs=attrs,
                        flow=task_flow
                    )
                    self.task_args = task_args
                    self.task_kwargs = task_kwargs

                def run(self, logger: logging.Logger):
                    if 'logger' in func.__code__.co_varnames:
                        self.task_kwargs['logger'] = logger

                    if 'attrs' in func.__code__.co_varnames:
                        self.task_kwargs['attrs'] = self.attrs

                    if 'inputs' in func.__code__.co_varnames:
                        self.task_kwargs['inputs'] = self.inputs()

                    output_obj = func(*self.task_args, **self.task_kwargs)
                    if output_file:
                        with self.output().open('w') as f:
                            f.write(output_obj)
                    else:
                        self.output().set(output_obj)

            return PythonTask()

        return task_register

    if len(args) > 0:
        if callable(args[0]):
            return task_decorator(args[0])

    return task_decorator
