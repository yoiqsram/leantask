import json
import logging
from leantask import python_task, Flow
from pathlib import Path


@python_task
def print_task(message: str, logger: logging.Logger):
    logger.info(message)


@python_task(attrs={'attempt_count': 0})
def fail_retry_task(
        attrs,
        logger: logging.Logger,
        message: str,
        success_on_attempt: int = 3
    ):
    if attrs['attempt_count'] < success_on_attempt:
        attrs['attempt_count'] += 1
        raise Exception('Fail task.')

    logger.info(message)

@python_task
def json_output(data: dict, logger: logging.Logger):
    logger.info('Pass data to the next task.')
    return data


@python_task(output_file=True)
def write_file(inputs, logger: logging.Logger):
    logger.info(f'Write file from inputs: {inputs}')
    _inputs = {
        key: value.value
        for key, value in inputs.items()
    }
    return json.dumps(_inputs)


with Flow(
        'task_dependencies',
        description='Example of task dependencies in a workflow.'
    ) as flow:
    task_0 = print_task(
        task_name='0_print',
        message='Task #0: Independent task'
    )

    task_1 = print_task(
        task_name='1_print',
        message='Task #1'
    )

    task_1_a = print_task(
        task_name='1_a_print',
        message='Task #1.a run after Task #1'
    )

    task_1_b = print_task(
        task_name='b_skip',
        message='Task #1.b run after Task #1'
    )

    task_2 = print_task(
        task_name='2_print',
        message='Task #2 run after Task #1.a'
    )

    task_2_a = fail_retry_task(
        task_name='2_a_fail',
        task_retry_max=3,
        task_retry_delay=5,
        message='Task #2.a run after some attempt(s).'
    )

    task_3 = json_output(
        task_name='3_json_output',
        data={
            'message': "This is the output from Task '3_json_output'."
        }
    )

    task_4 = write_file(
        task_name='4_write_file',
        task_output_path=Path('output.json')
    )

    task_1 >> (task_1_a, task_1_b)
    task_1_a >> task_2 >> task_3 >> task_4
    task_2 >> task_2_a
