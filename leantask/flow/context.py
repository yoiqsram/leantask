from typing import Set, Tuple


class FlowContext:
    __active__ = None
    __defined__ = None

    @classmethod
    def get_current_flow(cls):
        return cls.__active__


class TaskContext:
    __names__: Set[Tuple[str, str]] = set()

    @classmethod
    def register(cls, task) -> None:
        if (task.flow.name, task.name) in cls.__names__:
            return

        cls.__names__.add((task.flow.name, task.name))

    @classmethod
    def unregister(cls, task) -> None:
        if (task.flow.name, task.name) not in cls.__names__:
            return

        cls.__names__.remove((task.flow.name, task.name))
