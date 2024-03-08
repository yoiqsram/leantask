import logging
from pathlib import Path
from typing import Any, Callable, Dict

from ..task import Task


class PythonTask(Task):
    def __init__(
            self,
            func: Callable,
            name: str = None,
            output_path: Path = None,
            retry_max: int = 0,
            retry_delay: int = 0,
            attrs: Dict[str, Any] = None,
            params: Dict[str, Any] = None,
            flow = None):
        if name is None:
            name = func.__name__

        super(PythonTask, self).__init__(
            name=name,
            output_path=output_path,
            retry_max=retry_max,
            retry_delay=retry_delay,
            attrs=attrs,
            params=params,
            flow=flow
        )

        self._func = func

    def run(
            self,
            run_params: Dict[str, Any],
            logger: logging.Logger
        ):
        task_kwargs = dict()
        if 'logger' in self._func.__code__.co_varnames:
            task_kwargs['logger'] = logger

        if 'attrs' in self._func.__code__.co_varnames:
            task_kwargs['attrs'] = self.attrs

        if 'inputs' in self._func.__code__.co_varnames:
            task_kwargs['inputs'] = self.inputs()

        if 'run_params' in self._func.__code__.co_varnames:
            task_kwargs['run_params'] = run_params

        output_obj = self._func(**self.params, **task_kwargs)
        if self.output_path is not None:
            with self.output().open('w') as f:
                f.write(output_obj)
        else:
            self.output().set(output_obj)


def python_task(
        *args,
        attrs: dict = None,
        output_file: bool = False,
    ) -> Callable:
    '''Use @task decorator on your function to make it run as a Task.'''
    def task_decorator(func: Callable) -> Callable:
        def task_register(
                *,
                task_name: str = None,
                task_output_path: Path = None,
                task_retry_max: int = 0,
                task_retry_delay: int = 0,
                task_flow = None,
                **task_kwargs
            ) -> Task:
            '''Register a new task function.'''
            reserved_kwargs = {'attrs', 'inputs', 'logger', 'params', 'run_params'}
            params = dict()
            for key, value in task_kwargs.items():
                if key in reserved_kwargs:
                    raise ValueError(
                        f"Task kwargs of '{key}' is a reserved keyword. "
                        "Please use different keyword name."
                    )

                params[key] = value

            if output_file and task_output_path is None:
                raise AttributeError("Task 'task_output_path' should be filled.")

            return PythonTask(
                func,
                name=task_name,
                output_path=task_output_path,
                retry_max=task_retry_max,
                retry_delay=task_retry_delay,
                attrs=attrs,
                params=params,
                flow=task_flow
            )

        return task_register

    if len(args) > 0:
        if callable(args[0]):
            return task_decorator(args[0])

    return task_decorator
