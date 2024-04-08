import hashlib
import importlib
from pathlib import Path
from typing import Union


def get_confirmation(
        question: str,
        default: bool = None,
        allow_invalid_input: bool = False
    ) -> bool:
    while True:
        user_input = input(question + ' (yes/no) ').strip().lower()
        if user_input in ('y', 'yes'):
            print()
            return True

        elif user_input in ('n', 'no'):
            print()
            return False

        elif default is not None:
            return default

        print("Invalid input. Please enter 'yes' or 'no'.")
        if not allow_invalid_input:
            raise ValueError('Invalid user input.')


def calculate_md5(file_path: Union[str, Path]) -> str:
    '''Calculate md5 hash of a file.'''
    md5 = hashlib.md5()
    with open(file_path, 'rb') as file:
        for chunk in iter(lambda: file.read(4096), b''):
            md5.update(chunk)
    return md5.hexdigest()


def import_lib(name: str, file_path: Union[str, Path]):
    spec = importlib.util.spec_from_file_location(name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
