import logging
from leantask import python_task, Flow


@python_task
def scheduled_task(logger: logging.Logger):
    logger.info(f'Succesfully run scheduled task.')


with Flow(
        'task_schedules',
        description='Example of task schedules.',
        cron_schedules='*/2 * * * *'
    ) as flow:
    python_task = scheduled_task()
