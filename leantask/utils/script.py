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

        else:
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


def display_scrollable_text(long_text: str):
    import curses

    def _display_scrollable_text(stdscr):
        curses.curs_set(0)
        curses.use_default_colors()
        height, width = stdscr.getmaxyx()
        stdscr.clear()
        window = curses.newwin(height, width, 0, 0)

        index = 0
        end_index = index + height - 1
        new_index = index
        new_end_index = end_index
        lines = long_text.splitlines()
        while True:
            window.clear()
            new_index = max(0, new_index)
            new_end_index = new_index + height - 1
            if new_end_index <= len(lines):
                index = new_index
                end_index = new_end_index

            for row, line in enumerate(lines[index:end_index]):
                window.addstr(row, 0, line[:width])

            window.addstr(
                height - 1,
                0,
                "== Use arrow keys to scroll and 'q' to quit. ==",
                curses.A_STANDOUT
            )
            stdscr.refresh()
            window.refresh()

            key = stdscr.getch()
            if key == 258:
                new_index = index + 1
                continue

            elif key == 259:
                new_index = index - 1
                continue

            elif key == ord('q'):
                break

    curses.wrapper(_display_scrollable_text)
