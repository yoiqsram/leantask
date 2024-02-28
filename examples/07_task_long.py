import logging
import time
from leantask import python_task, Flow


@python_task
def long_task(timeout: int, logger: logging.Logger):
    logger.info(f'Waiting for {timeout}s.')
    time.sleep(timeout)
    logger.info('Finished waiting.')


with Flow(
        'task_long',
        description='Example of long-time task.'
    ) as flow:
    task = long_task(timeout=45)
