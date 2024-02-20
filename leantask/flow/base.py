from __future__ import annotations
from enum import Enum

from ..database import BaseModel
from ..database.common import ForeignKeyField


class ModelMixin:
    __model__ = None
    __refs__ = None

    def __init__(self, __id: str = None, **kwargs) -> None:
        if not issubclass(self.__model__, BaseModel):
            raise TypeError()

        if self.__refs__ is None:
            raise ValueError()

        self._model = None
        self._model_exists = False

        self._setup_existing_model(__id, **kwargs)

        if self._model is None:
            kwargs = dict()
            for key, field in self.__model__._meta.fields.items():
                if not hasattr(self, key) or key == 'id':
                    continue

                if isinstance(field, ForeignKeyField):
                    key += '_id'

                value = getattr(self, key)
                kwargs[key] = value if not isinstance(value, Enum) else value.name

            self._model = self.__model__(**kwargs)

    @property
    def id(self) -> str:
        return self._model.id

    def _setup_existing_model(self, __id: str = None) -> None:
        if __id is not None:
            try:
                self._model = (
                    self.__model__.select()
                    .where(self.__model__.id == __id)
                    .limit(1)
                    [0]
                )

                self._model_exists = True

            except IndexError:
                pass

    def save(self) -> None:
        log_kwargs = dict()
        for key, field in self._model._meta.fields.items():
            log_key = key
            if key in self.__class__.__refs__:
                log_key = f'ref_{key}'

            if not hasattr(self, key):
                continue

            if isinstance(field, ForeignKeyField):
                key += '_id'

            value = getattr(self, key)
            value = value if not isinstance(value, Enum) else value.name
            setattr(self._model, key, value)
            log_kwargs[log_key] = getattr(self._model, key)

        with self._model._meta.database.atomic():
            self._model.save(force_insert=not self._model_exists)
            self._model_exists = True

            log_model = self._model._meta.log_model(**log_kwargs)
            log_model.save(force_insert=True)
