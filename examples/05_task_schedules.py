import logging
from leantask import task, Flow


@task
def scheduled_task(logger: logging.Logger):
    logger.info(f'Succesfully run scheduled task.')


with Flow(
        'task_schedules',
        description='Example of task schedules.',
        cron_schedules='*/2 * * * *'
    ) as flow:
    task = scheduled_task()
