from ._base import (
    Model, Column, BIG_STRING,
    column_uuid_primary_key, column_current_datetime, column_modified_datetime
)
from ...enum import TableName


class MetadataModel(Model):
    __tablename__ = TableName.METADATA.value

    name = column_uuid_primary_key()
    description = Column(BIG_STRING)
    value = Column(BIG_STRING)

    created_datetime = column_current_datetime()
    modified_datetime = column_current_datetime()

    def __repr__(self):
        return (
            f'<Metadata(name={repr(self.name)}'
            f' value={repr(self.value)})>'
        )
