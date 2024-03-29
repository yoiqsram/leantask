import logging
from leantask import python_task, Flow


@python_task
def foo(logger: logging.Logger):
    '''This task will have an output of 'dict' object.'''
    output_data = {
        'message': 'This message is the output from foo task.'
    }
    return output_data


@python_task
def bar(inputs, logger: logging.Logger):
    '''This task will print all inputs from the exact previous task(s).'''
    logger.info(f'Inputs: {inputs}')


with Flow(
        'task_input_output',
        description='Example of using task inputs and outputs.'
    ) as flow:
    task_1 = foo()
    task_2 = bar()

    task_1 >> task_2
