import os
from datetime import datetime
from pathlib import Path

from .enum import LogTableName

METADATA_DIRNAME = '.leantask'


def _prepare_log_file(file_path: Path):
    if not file_path.parent.is_dir():
        file_path.parent.mkdir(parents=True)

    if not file_path.exists():
        with open(file_path, 'w'):
            pass


class GlobalContext:
    PROJECT_DIR: Path = os.environ.get('LEANTASK_PROJECT_DIR', Path(os.getcwd()).resolve())
    FLOWS_DIR: Path = Path(os.environ.get('LEANTASK_FLOWS_DIRN', str(PROJECT_DIR)))
    if not FLOWS_DIR.is_relative_to(PROJECT_DIR):
        raise FileExistsError(f"Specified flows directory should be in project directory. '{FLOWS_DIR}'.")

    CACHE_DIRNAME: str = '__cache__'
    LOG_DIRNAME: str = 'log'

    DATABASE_NAME: str = os.environ.get('LEANTASK_DATABASE_NAME', 'leantask.db')
    LOG_DATABASE_NAME: str = os.environ.get('LEANTASK_LOG_DATABASE_NAME', 'leantask_log.db')

    LOG_DEBUG: bool = os.environ.get('LEANTASK_DEBUG', 'false').lower() == 'true'
    LOG_QUIET: bool = os.environ.get('LEANTASK_QUIET', 'false').lower() == 'true'
    DEBUG_QUERY: bool = os.environ.get('LEANTASK_DEBUG_QUERY', 'false').lower() == 'true'

    try:
        CACHE_TIMEOUT = int(os.environ.get('LEANTASK_CACHE_TIMEOUT'))
    except TypeError:
        CACHE_TIMEOUT = 1800

    LOCAL_RUN: bool = False
    SCHEDULER_SESSION_ID: str = None

    try:
        WORKER = int(os.environ.get('LEANTASK_WORKER'))
    except TypeError:
        WORKER = 1

    try:
        HEARTBEAT = int(os.environ.get('LEANTASK_HEARTBEAT'))
    except:
        HEARTBEAT = 30

    DISCOVER = os.environ.get('LEANTASK_DISCOVER', 'false').lower() == 'true'

    @classmethod
    def set_project_dir(cls, value: Path) -> None:
        if value.is_file():
            raise ValueError('Path should be a directory.')

        cls.PROJECT_DIR = value

    @classmethod
    def set_scheduler_session(
            cls,
            session_id: str,
            created_datetime: datetime = None
        ) -> None:
        cls.SCHEDULER_SESSION_ID = session_id

        if created_datetime is None:
            created_datetime = datetime.now()
        cls.SCHEDULER_SESSION_TIMESTAMP = created_datetime

    @classmethod
    def relative_path(cls, value: Path) -> Path:
        return value.relative_to(cls.PROJECT_DIR)

    @classmethod
    def metadata_dir(cls) -> Path:
        return cls.PROJECT_DIR / METADATA_DIRNAME

    @classmethod
    def workflows_dir(cls) -> Path:
        if cls.FLOWS_DIR is None:
            workflows_dir_path = cls.PROJECT_DIR
        else:
            workflows_dir_path = cls.PROJECT_DIR / cls.FLOWS_DIR

        if not workflows_dir_path.is_dir():
            workflows_dir_path.mkdir(parents=True)

        return workflows_dir_path

    @classmethod
    def cache_dir(cls) -> Path:
        cache_dir_path = cls.metadata_dir() / cls.CACHE_DIRNAME

        if not cache_dir_path.is_dir():
            cache_dir_path.mkdir(parents=True)

        return cache_dir_path

    @classmethod
    def log_dir(cls) -> Path:
        log_dir_path = cls.metadata_dir() / cls.LOG_DIRNAME

        if not log_dir_path.is_dir():
            log_dir_path.mkdir(parents=True)

        return log_dir_path

    @classmethod
    def get_local_log_file_path(cls) -> Path:
        current_time = datetime.now().isoformat()
        log_file_path = cls.log_dir() / 'local' / (current_time + '.log')
        _prepare_log_file(log_file_path)
        return log_file_path

    @classmethod
    def get_scheduler_session_log_file_path(cls) -> Path:
        log_file_path = (
            cls.log_dir()
            / LogTableName.SCHEDULER_SESSION.value
            / (datetime.now().isoformat() + '.log')
        )
        _prepare_log_file(log_file_path)
        return log_file_path

    @classmethod
    def get_flow_run_log_file_path(
            cls,
            flow_id: str,
            flow_run_id: str
        ) -> Path:
        log_file_path = (
            cls.log_dir()
            / LogTableName.FLOW_RUN.value
            / str(flow_id)
            / (str(flow_run_id) + '.log')
        )
        _prepare_log_file(log_file_path)
        return log_file_path

    @classmethod
    def get_task_run_log_file_path(
            cls,
            flow_id: str,
            task_name: str,
            task_run_id: str
        ) -> Path:
        log_file_path = (
            cls.log_dir()
            / LogTableName.TASK_RUN.value
            / str(flow_id)
            / str(task_name)
            / (str(task_run_id) + '.log')
        )
        _prepare_log_file(log_file_path)
        return log_file_path

    @classmethod
    def database_path(cls) -> Path:
        return cls.metadata_dir() / cls.DATABASE_NAME

    @classmethod
    def log_database_path(cls) -> Path:
        return cls.metadata_dir() / cls.LOG_DATABASE_NAME
