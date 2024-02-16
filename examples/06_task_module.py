from leantask import task, Flow
from examples.task_module.task_log import logging_task


with Flow(
        'task_module',
        description='Example of task module.'
    ) as flow:
    task = logging_task('Example of using task on different module.')
