from leantask import task, Flow


@task
def foo():
    return {
        'message': 'This message is the output from foo task.'
    }


@task
def bar(inputs):
    print(inputs)


with Flow('task_input_output') as flow:
    task_1 = foo()
    task_2 = bar()

    task_1 >> task_2
