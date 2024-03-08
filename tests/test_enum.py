import inspect
from enum import Enum
from leantask import enum
from typing import List, Type


def get_enum_classes() -> List[Type[Enum]]:
    return [
        enum_class
        for _, enum_class
        in inspect.getmembers(
            enum,
            predicate=lambda obj:
                inspect.isclass(obj)
                and obj != Enum
                and issubclass(obj, Enum)
        )
    ]


def get_status_enum_classes() -> List[Type[Enum]]:
    return [
        enum_class
        for enum_class in get_enum_classes()
        if enum_class.__name__.endswith('Status')
    ]


def get_table_name_enum_classes() -> List[Type[Enum]]:
    return [
        enum_class
        for enum_class in get_enum_classes()
        if enum_class.__name__.endswith('TableName')
    ]


def test_uppercase_name_for_all_enums():
    for enum_class in get_enum_classes():
        for name in dir(enum_class):
            if name.startswith('__'):
                continue

            assert name == name.upper()


def test_integer_value_for_all_status_enums():
    for enum_class in get_status_enum_classes():
        for name in dir(enum_class):
            if name.startswith('__'):
                continue

            enum = getattr(enum_class, name)
            assert isinstance(enum.value, int)


def test_unknown_for_all_status_enums():
    for enum_class in get_status_enum_classes():
        assert hasattr(enum_class, 'UNKNOWN')
        assert enum_class.UNKNOWN.value == 1


def test_lowercase_value_for_all_table_name_enums():
    for enum_class in get_table_name_enum_classes():
        for name in dir(enum_class):
            if name.startswith('__'):
                continue

            enum = getattr(enum_class, name)
            assert isinstance(enum.value, str)
            assert enum.value == enum.value.lower()
