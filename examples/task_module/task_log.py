import logging
from leantask import python_task


@python_task
def logging_task(message: str, logger: logging.Logger):
    logger.info(message)
