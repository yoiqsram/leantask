import os
from pathlib import Path

METADATA_DIRNAME = '.leantask'


class GlobalContext:
    PROJECT_DIR: Path = Path(os.getcwd()).resolve()
    CACHE_DIRNAME: str = '__cache__'
    LOG_DIRNAME: str = 'log'

    DATABASE_NAME: str = 'leantask.db'
    LOG_DATABASE_NAME: str = 'leantask_log.db'

    SCHEDULER_SESSION_ID: str = None

    LOCAL_RUN: bool = False

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
        return cls.PROJECT_DIR / METADATA_DIRNAME

    @classmethod
    def cache_dir(cls) -> Path:
        return cls.metadata_dir() / cls.CACHE_DIRNAME

    @classmethod
    def log_dir(cls) -> Path:
        return cls.log_dir() / cls.LOG_DIRNAME

    @classmethod
    def database_path(cls) -> Path:
        return cls.metadata_dir() / cls.DATABASE_NAME

    @classmethod
    def log_database_path(cls) -> Path:
        return cls.metadata_dir() / cls.LOG_DATABASE_NAME
