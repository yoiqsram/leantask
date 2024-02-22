import os
from datetime import datetime
from fnmatch import fnmatch
from pathlib import Path
from typing import List


def is_file_match_patterns(file_path: Path, patterns: List[str]):
    filename = str(file_path)
    for pattern in patterns:
        if fnmatch(filename, pattern):
            return True
    return False


def parse_gitignore_patterns(gitignore_path: Path) -> List[str]:
    ignore_patterns = []
    with open(gitignore_path) as f:
        for line in f:
            stripped_line = line.strip()
            if stripped_line and not stripped_line.startswith("#"):
                if stripped_line.endswith('/'):
                    ignore_patterns.append(stripped_line + '*')
                else:
                    ignore_patterns.append(stripped_line)

    return ignore_patterns


def get_file_created_datetime(file_path: Path) -> datetime:
    creation_timestamp = os.path.getctime(file_path)
    return datetime.fromtimestamp(creation_timestamp)
