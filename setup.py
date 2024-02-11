from setuptools import setup, find_packages

setup(
    name='leantask',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'croniter>=2.0.1',
        'greenlet>=3.0.3',
        'python-dateutil>=2.8.2',
        'pytz>=2023.3.post1',
        'six>=1.16.0',
        'SQLAlchemy>=2.0.25',
        'typing-extensions>=4.9.0'
    ],
    author='Yoiq S Rambadian',
    description='Small flow scheduler'
)
