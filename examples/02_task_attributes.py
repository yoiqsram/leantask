import logging
from leantask import python_task, Flow


@python_task(attrs={'retry_count': 0})
def fail_task(
        attrs,
        logger: logging.Logger,
        on_retry: int = 1
    ):
    '''Fail task and success after 'on_retry' time(s).'''
    logger.debug(f"Running task with 'retry_count': {attrs['retry_count']}")
    if attrs['retry_count'] < on_retry:
        attrs['retry_count'] += 1
        raise Exception('Fail task.')

    logger.info(f"Succesfully run task after {attrs['retry_count']} retry(s).")


with Flow(
        'task_attributes',
        description='Example of using attributes in task.'
    ) as flow:
    fail_task(task_retry_max=3)
