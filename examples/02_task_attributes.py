from leantask import task, Flow


@task(attrs={'retry_count': 0})
def fail_task(
        attrs,
        on_retry: int = 1
    ):
    print('Running task with retry_count:', attrs['retry_count'])
    if attrs['retry_count'] < on_retry:
        attrs['retry_count'] += 1
        raise Exception('Fail task.')

    print(f"Succesfully run task after {attrs['retry_count']} retry(s).")


with Flow('task_attrs') as flow:
    fail_task(task_retry=3)
