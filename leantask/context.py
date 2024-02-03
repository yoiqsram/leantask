import os
from datetime import datetime
from pathlib import Path

METADATA_DIRNAME = '.leantask'


class GlobalContext:
    PROJECT_DIR: Path = Path(os.getcwd()).resolve()
    WORKFLOWS_DIRNAME: str = None
    CACHE_DIRNAME: str = '__cache__'
    LOG_DIRNAME: str = 'log'

    DATABASE_NAME: str = 'leantask.db'
    LOG_DATABASE_NAME: str = 'leantask_log.db'
    LOG_NAME: str = None

    LOG_DEBUG: int = False
    LOG_QUIET: int = False

    SCHEDULER_SESSION_ID: str = None
    CACHE_TIMEOUT: int = 1800

    LOCAL_RUN: bool = True

    @classmethod
    def set_project_dir(cls, value: Path) -> None:
        if value.is_file():
            raise ValueError('Path should be a directory.')

        cls.PROJECT_DIR = value

    @classmethod
    def set_scheduler_session_id(cls, value: str) -> None:
        cls.SCHEDULER_SESSION_ID = value

    @classmethod
    def relative_path(cls, value: Path) -> Path:
        return value.relative_to(cls.PROJECT_DIR)

    @classmethod
    def metadata_dir(cls) -> Path:
        metadata_dir_path = cls.PROJECT_DIR / METADATA_DIRNAME

        if not metadata_dir_path.is_dir():
            metadata_dir_path.mkdir(parents=True)

        return metadata_dir_path

    @classmethod
    def workflows_dir(cls) -> Path:
        if cls.WORKFLOWS_DIRNAME is None:
            workflows_dir_path = cls.PROJECT_DIR
        else:
            workflows_dir_path = cls.PROJECT_DIR / cls.WORKFLOWS_DIRNAME

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
    def set_log_filename(cls, value: str = None) -> Path:
        if value is None:
            current_time = datetime.now().isoformat(sep=' ', timespec='seconds')
            value = current_time + '.log'

        cls.LOG_NAME = value

    @classmethod
    def get_log_file_path(cls) -> Path:
        if cls.LOG_NAME is None:
            return

        log_file_path = cls.log_dir() / cls.LOG_NAME
        if not log_file_path.exists():
            return

        return log_file_path

    @classmethod
    def database_path(cls) -> Path:
        return cls.metadata_dir() / cls.DATABASE_NAME

    @classmethod
    def log_database_path(cls) -> Path:
        return cls.metadata_dir() / cls.LOG_DATABASE_NAME
