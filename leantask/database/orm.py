import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.orm.exc import NoResultFound
from typing import Callable

from ..context import GlobalContext
from .models import Model, MetadataModel
from .models.log import LogModel


def open_db_session(database_path: Path) -> Session:
    engine = create_engine('sqlite:///' + str(database_path.resolve()))
    ConnectSession = sessionmaker(bind=engine)
    return ConnectSession()


def db_session(database_path: Path) -> Callable:
    def decorator(func: Callable) -> Callable:
        if 'session' not in func.__code__.co_varnames:
            raise ValueError(
                f"There should be 'session' keyword argument in function '{func.__name__}'."
            )

        def wrapper(*args, session: Session = None, **kwargs):
            is_independent = False
            if session is None:
                session = open_db_session(database_path)
                is_independent = True

            result = func(*args, session=session, **kwargs)

            if is_independent:
                session.close()

            return result
        return wrapper
    return decorator


def create_database(database_path: Path, model) -> None:
    if database_path.exists():
        raise FileExistsError(f'Database {database_path} already exists.')

    try:
        engine = create_engine('sqlite:///' + str(database_path))
        model.metadata.create_all(engine)

    except Exception as exc:
        database_path.unlink()
        raise exc


def create_metadata_database(
        project_name: str,
        project_description: str = None,
        replace: bool = False
    ) -> None:
    database_path = GlobalContext.database_path()
    backup_database_path = database_path.parent / (database_path.name + '.backup')

    log_database_path = GlobalContext.log_database_path()
    backup_log_database_path = log_database_path.parent / (log_database_path.name + '.backup')

    if not GlobalContext.metadata_dir().is_dir():
        GlobalContext.metadata_dir().mkdir(parents=True)

    if database_path.is_file() and replace:
        print("There's already a project exists and it will be replaced.")
        os.rename(database_path, backup_database_path)

    if log_database_path.is_file() and replace:
        os.rename(log_database_path, backup_log_database_path)

    try:
        create_database(database_path, Model)
        create_database(log_database_path, LogModel)

        project_metadata = {
            'name': project_name,
            'description': project_description,
            'is_active': True
        }

        with open_db_session(database_path) as session:
            session.add_all([
                MetadataModel(name=name, value=str(value))
                for name, value in project_metadata.items()
            ])
            session.commit()

    except Exception as exc:
        database_path.unlink(missing_ok=True)
        log_database_path.unlink(missing_ok=True)

        if replace:
            os.rename(backup_database_path, database_path)
            os.rename(backup_log_database_path, log_database_path)

        else:
            GlobalContext.metadata_dir().rmdir()

        raise exc
