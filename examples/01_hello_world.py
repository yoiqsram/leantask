import logging
from leantask import python_task, Flow


@python_task
def hello_world(name: str, logger: logging.Logger):
    logger.info(f'Hello, {name}! Welcome to the world!')


with Flow(
        'hello_world',
        description='Example of the very basic flow.'
    ) as flow:
    python_task = hello_world('there')
