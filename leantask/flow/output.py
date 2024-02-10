from pathlib import Path
from typing import Any, IO

class TaskOutput: ...
class UndefinedTaskOutput(TaskOutput): ...


class ObjectTaskOutput(TaskOutput):
    def __init__(self) -> None:
        self._value = None

    @property
    def value(self) -> Any:
        return self._value

    @value.setter
    def value(self, value: Any) -> None:
        self._value = value

    def get(self) -> Any:
        return self._value

    def set(self, value: Any) -> None:
        self._value = value


class FileTaskOutput(TaskOutput):
    '''Basic file output class from Task output.'''
    def __init__(
            self,
            output_path: Path = None,
        ) -> None:
        self._output_path = output_path

    def get(self, json: bool = False) -> Any:
        with open(self._output_path) as f:
            if json:
                import json
                value = json.load(f)
            else:
                value = f.read()

        return value

    def set(self, value: Any, json: bool = False) -> None:
        with open(self._output_path, 'w') as f:
            if json:
                import json
                json.dump(value, f)
            else:
                f.write(str(value))

    def open(self, method: str) -> IO[Any]:
        return open(self._output_path, method)

    def exists(self) -> bool:
        return self._output_path.exists()
