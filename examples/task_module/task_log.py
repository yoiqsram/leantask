import logging
from leantask import task


@task
def logging_task(message: str, logger: logging.Logger):
    logger.info(message)
