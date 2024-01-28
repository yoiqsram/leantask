import asyncio
import hashlib
import inspect
import sys
from pathlib import Path
from typing import Union


def is_main_script() -> bool:
    main_script_path = Path(sys.argv[0]).resolve()
    main_caller_path = Path(inspect.stack()[-1].filename).resolve()
    return main_caller_path != main_script_path


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
