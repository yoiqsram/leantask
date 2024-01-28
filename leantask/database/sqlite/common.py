import sqlite3
from pathlib import Path
from textwrap import dedent
from typing import Any, Callable, Dict, Iterable, List, Tuple, Union

from ...context import GlobalContext


def sqlite_connect(database_path: Path) -> Callable:
    def decorator(func: Callable) -> Callable:
        if 'connection' not in func.__code__.co_varnames:
            raise ValueError(
                f"There should be 'connection' keyword argument in function '{func.__name__}'."
            )

        def wrapper(*args, connection: sqlite3.Connection = None, **kwargs):
            is_independent = False
            if connection is None:
                connection = sqlite3.connect(database_path.resolve())
                is_independent = True

            result = func(*args, connection=connection, **kwargs)

            if is_independent:
                connection.close()

            return result
        return wrapper
    return decorator


def _query(
        __query: str,
        parameters: tuple = (),
        connection: sqlite3.Connection = None
    ) -> None:
    cursor = connection.cursor()
    query = dedent(__query)

    try:
        cursor.execute(query, parameters)

    except Exception as exc:
        print(type(exc), exc)
        print('Error while executing query:', query, sep='/n')
        raise exc

    result = [row for row in cursor.fetchall()]

    cursor.close()
    return result


def _insert(
        table_name: str,
        record: Dict[str, Union[float, int, str]],
        connection: sqlite3.Connection = None
    ) -> None:
    cursor = connection.cursor()
    columns = tuple(record.keys())
    values = tuple(record.values())

    query = f'''\
        INSERT INTO {table_name}
        ({', '.join(columns)})
        VALUES ({', '.join(['?'] * len(columns))})
    '''
    cursor.execute(dedent(query), values)
    connection.commit()
    cursor.close()


def _update(
        table_name: str,
        items_set: Dict[str, Union[float, int, str]],
        items_filter: Dict[str, Union[float, int, str]],
        connection: sqlite3.Connection = None
    ) -> None:
    cursor = connection.cursor()
    columns_set, values_set = tuple(items_set.keys()), tuple(items_set.keys())
    columns_filter, values_filter = tuple(items_filter.keys()), tuple(items_filter.keys())

    query = f'''\
        UPDATE {table_name}
        SET {', '.join([f"{column} = ?" for column in columns_set])}
        WHERE {', '.join([f"{column} = ?" for column in columns_filter])}
    '''
    cursor.execute(dedent(query), values_set + values_filter)
    connection.commit()
    cursor.close()


query = sqlite_connect(GlobalContext.database_path())(_query)
insert = sqlite_connect(GlobalContext.database_path())(_insert)
update = sqlite_connect(GlobalContext.database_path())(_update)

log_query = sqlite_connect(GlobalContext.log_database_path())(_query)
log_insert = sqlite_connect(GlobalContext.log_database_path())(_insert)
