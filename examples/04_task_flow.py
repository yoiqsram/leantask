import json
from leantask import task, Flow, TaskSkipped
from pathlib import Path


@task
def print_task(message: str):
    print(message)


@task(attrs={'retry_count': 0})
def fail_task(attrs, message: str, on_retry: int = 1):
    if attrs['retry_count'] < on_retry:
        attrs['retry_count'] += 1
        raise Exception('Fail task.')

    print(message)

@task
def skip_task():
    raise TaskSkipped('Intentionally skipped the task.')

@task
def json_output(data: dict):
    print('Pass data to the next task.')
    return data


@task(output_file=True)
def write_file(inputs):
    print('Write file from inputs:', inputs)
    return json.dumps(inputs)


with Flow('task_flow') as flow:
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

    task_1_b = skip_task(task_name='b_skip')

    task_2 = print_task(
        task_name='2_print',
        message='Task #2 run after Task #1.a'
    )

    task_2_a = fail_task(
        task_name='2_a_fail',
        task_retry=3,
        message='Task #2.a fail on the first trial'
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
