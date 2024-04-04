import json
import logging
from leantask import python_task, Flow
from pathlib import Path


@python_task
def fail_task():
    raise Exception('Fail task.')


@python_task
def print_task(message: str, logger: logging.Logger):
    logger.info(message)


with Flow(
        'task_failed_downstream',
        description='Example of task dependencies in a workflow.'
    ) as flow:
    task_1 = fail_task(
        task_name='1_fail'
    )

    task_2 = print_task(
        task_name='2_fail_downstream',
        message='Task #2: Failed downstream'
    )

    task_1 >> task_2
