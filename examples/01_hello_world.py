import logging
from leantask import task, Flow


@task
def hello_world(name: str, logger: logging.Logger):
    logger.info(f'Hello, {name}! Welcome to the world!')


with Flow(
        'hello_world',
        description='Example of the very basic flow.'
    ) as flow:
    task = hello_world('there')
