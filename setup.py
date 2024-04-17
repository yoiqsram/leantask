from setuptools import setup, find_packages

setup(
    name='leantask',
    version='0.4.0',
    packages=find_packages(),
    install_requires=[
        'croniter==2.0.1',
        'greenlet==3.0.3',
        'peewee==3.17.1',
        'python-dateutil==2.8.2',
        'pytz==2023.3.post1',
        'six==1.16.0',
        'tabulate==0.9.0',
        'typing_extensions==4.9.0'
    ],
    author='Yoiq S Rambadian',
    description='Lite workflow scheduler to be used on low spec machine.'
)
