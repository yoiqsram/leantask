from peewee import Model

from ..context import GlobalContext
from .common import open_db_connection

database = open_db_connection(
    GlobalContext.database_path().resolve()
)
log_database = database = open_db_connection(
    GlobalContext.log_database_path().resolve()
)


class BaseModel(Model):
    class Meta:
        database = database


class LogModel(Model):
    class Meta:
        database = log_database
