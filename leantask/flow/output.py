from pathlib import Path
from typing import Any, IO

class UndefinedTaskOutput: ...


class TaskOutputFile:
    '''Basic file output class from Task output.'''
    def __init__(
            self,
            output_path: Path = None,
        ) -> None:
        self._output_path = output_path

    def open(self, method: str) -> IO[Any]:
        return open(self._output_path, method)

    def exists(self) -> bool:
        return self._output_path.exists()
