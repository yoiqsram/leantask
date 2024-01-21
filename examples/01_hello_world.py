from leantask import task, Flow


@task
def hello_world(name: str):
    print(f'Hello, {name}! Welcome to the world!')


with Flow('hello_world') as flow:
    task = hello_world('there')