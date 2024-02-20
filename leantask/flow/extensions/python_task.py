import logging
from pathlib import Path
from typing import Callable

from ..context import TaskContext
from ..task import Task


def python_task(
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
                    super(PythonTask, self).__init__(
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