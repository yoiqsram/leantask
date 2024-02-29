import logging
from leantask import python_task, Flow


@python_task
def scheduled_task(
        run_params: dict,
        logger: logging.Logger
    ):
    schedule_datetime = run_params['schedule_datetime']
    if schedule_datetime is not None:
        schedule_datetime = schedule_datetime.isoformat(sep=' ', timespec='minutes')
        logger.info(f"Succesfully run scheduled task on '{schedule_datetime}'.")
    else:
        raise ValueError('This task could only be run on schedule.')


with Flow(
        'task_schedules',
        description='Example of task schedules.',
        cron_schedules='*/2 * * * *',
        max_delay=60,
    ) as flow:
    python_task = scheduled_task()
