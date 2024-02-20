from leantask import python_task, Flow
from examples.task_module.task_log import logging_task


with Flow(
        'task_module',
        description='Example of task module.'
    ) as flow:
    python_task = logging_task('Example of using task on different module.')
