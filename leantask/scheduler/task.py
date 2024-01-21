from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, IO, Iterable, TYPE_CHECKING, Union

from leantask.logging import logger
from .context import FlowContext, TaskContext

if TYPE_CHECKING:
    from.flow import Flow

class TaskSkipped(Exception): ...
class UndefinedTaskOutput: ...


class TaskOutputFile:
    '''Basic file output class from Task output.'''
    def __init__(
            self,
            output_path: Path = None,
        ) -> None:
        self._output_path = output_path

    def open(self, method: str) -> IO[Any]:
        return open(self._output_path, method)

    def exists(self) -> bool:
        return self._output_path.exists()


class Task:
    '''Basic Task class

    Define run method to be implemented by your new subclass.
    Use self.inputs() and self.output() to access task inputs and outputs.
    '''
    def __init__(
            self,
            name: str,
            output_path: Path = None,
            retry: int = 0,
            retry_delay: int = 0,
            weight: int = 1,
            attrs: dict = None,
            flow = None
        ) -> None:
        if output_path is not None and not isinstance(output_path, Path):
            raise TypeError(f"Task 'output_path' must be a Path, not '{type(output_path)}'.")
        self.output_path = output_path
        self._output = UndefinedTaskOutput()

        self.retry = retry
        self.retry_delay = retry_delay
        self.weight = weight

        self.name = self._validate_name(name)
        self._upstream = set()
        self._downstream = set()

        if attrs is None:
            attrs = dict()
        self.attrs = attrs.copy()

        TaskContext.__names__ += (name,)
        self._flow = self._validate_flow(flow)
        if self._flow is not None:
            self._flow.add_task(self)

    def __repr__(self) -> str:
        return f"<Task name='{self.name}'>"

    @property
    def flow(self) -> Flow:
        '''Get flow that this task is in.'''
        return self._flow

    def _validate_name(self, name: str) -> str:
        '''Validate task name whether it has been registered or not.'''
        if not isinstance(name, str):
            raise TypeError(f"Task name must be a string, not '{type(name)}'.")

        if name in TaskContext.__names__:
            raise TypeError(f"Task name of '{name}' is already registered. Use different task name.")

        return name

    def _validate_flow(self, flow) -> Flow:
        # raise ReferenceError(f"Task '{self.name}' is not referencing any flow.")

        if flow is None:
            flow = FlowContext.get_current_flow()

        return flow

    def run(self) -> None:
        '''Replace this method to be implemented by your new subclass.'''
        raise NotImplemented("You need to define Task 'run' method.")

    def skip(self) -> None:
        '''Inherit this method to be implemented by your new subclass.'''
        raise TaskSkipped(f"Task '{self.name}' is skipped. Downstream task will also be skipped.")

    def inputs(self) -> Dict[str, Any]:
        '''Get inputs of this task.'''
        task_inputs = dict()
        for task in self._upstream:
            if isinstance(task._output, UndefinedTaskOutput):
                raise ValueError(f"Task '{task.name}' has no output.")

            elif isinstance(task._output, TaskOutputFile):
                if not task._output.exists():
                    raise ValueError(f"Output file from task '{task.name}' is not exists.")

            task_inputs[task.name] = task._output

        return {task.name: task._output for task in self._upstream}

    def output(self, obj: Any = None) -> Any:
        '''Get output of this task.'''
        if self.output_path is None:
            self._output = obj
        else:
            self._output = TaskOutputFile(self.output_path)

        return self._output

    def requires(self, obj: Task) -> None:
        '''Set upstream task.'''
        if not isinstance(obj, Task):
            raise TypeError(f"Task could only be required by another Task, not '{type(obj)}'.")

        obj._downstream.add(self)
        self._upstream.add(obj)

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
        for downstream_task in self._downstream:
            yield from downstream_task.iter_downstream()


def task(
        *args,
        attrs: dict = None,
        output_file: bool = False,
    ) -> Callable:
    '''Use @task decorator on your function to make it run as a Task.'''
    def task_decorator(func: Callable):
        def task_register(
                *task_args,
                task_name: str = None,
                task_output_path: Path = None,
                task_retry: int = 0,
                task_retry_delay: int = 0,
                task_weight: int = 1,
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
                raise TypeError(
                    "Task 'attrs' is a reserved keyword. "
                    "Please use different keyword name."
                )

            if 'inputs' in task_kwargs:
                raise TypeError(
                    "Task 'inputs' is a reserved keyword. "
                    "Please use different keyword name."
                )

            class PythonTask(Task):
                def __init__(
                        self,
                        name=task_name,
                        output_path=task_output_path,
                        retry=task_retry,
                        retry_delay=task_retry_delay,
                        weight=task_weight,
                        attrs=attrs,
                        flow=task_flow,
                    ):
                    super().__init__(
                        name=name,
                        output_path=output_path,
                        retry=retry,
                        retry_delay=retry_delay,
                        weight=weight,
                        attrs=attrs,
                        flow=flow
                    )
                    self.task_args = task_args
                    self.task_kwargs = task_kwargs

                def run(self):
                    if 'attrs' in func.__code__.co_varnames:
                        self.task_kwargs['attrs'] = self.attrs

                    if 'inputs' in func.__code__.co_varnames:
                        self.task_kwargs['inputs'] = self.inputs()

                    output = func(*self.task_args, **self.task_kwargs)

                    if output_file:
                        with self.output().open('w') as f:
                            f.write(output)

                    else:
                        self.output(output)

            return PythonTask(
                name=task_name,
                output_path=task_output_path if output_file else None,
                flow=task_flow
            )

        return task_register

    if len(args) > 0:
        if callable(args[0]):
            return task_decorator(args[0])

    return task_decorator
