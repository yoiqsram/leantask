from ...enum import TableName
from ..base import BaseModel
from ..common import (
    column_sequence, column_medium_string, column_big_string, column_text,
    column_current_datetime, column_modified_datetime
)


class MetadataModel(BaseModel):
    id = column_sequence(primary_key=True)
    name = column_medium_string()
    description = column_text(null=True)
    value = column_big_string(null=True)

    created_datetime = column_current_datetime()
    modified_datetime = column_modified_datetime()

    class Meta:
        table_name = TableName.METADATA.value
