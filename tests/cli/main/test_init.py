import os
import shutil
import subprocess
import sys
from pathlib import Path


def init_project(
        project_dir: Path,
        name: str = None,
        replace: bool = False,
        debug: bool = False,
        short_flag: bool = False
    ):
    project_dir.mkdir(parents=True, exist_ok=True)
    name_flag = '-N' if short_flag else '--name'
    replace_flag = '-R' if short_flag else '--replace'
    debug_flag = '--debug'

    command = ' '.join(
        ['export', f"PYTHONPATH={os.getcwd()}", '&&']
        + [f'"{sys.executable}"', '-m', 'leantask', 'init']
        + ([name_flag, name] if name is not None else [])
        + ([replace_flag] if replace else [])
        + ([debug_flag] if debug else [])
    )
    process = subprocess.run(
        command,
        shell=True,
        cwd=project_dir
    )
    return process


def validate_leantask_project(project_dir: Path):
    database = project_dir / '.leantask' / 'leantask.db'
    assert os.path.getsize(database) > 0

    log_database = project_dir / '.leantask' / 'leantask_log.db'        
    assert os.path.getsize(log_database) > 0


def test_init_new(
        project_dir: Path = None,
        name: str = None,
        debug: bool = False,
        short_flag: bool = False
    ):
    if project_dir is None:
        project_dir = Path('.test_init_new')

    process = init_project(
        project_dir,
        name=name,
        debug=debug,
        short_flag=short_flag
    )

    try:
        assert process.returncode == 0

        validate_leantask_project(project_dir)

    except AssertionError as exc:
        raise exc

    finally:
        shutil.rmtree(project_dir)


def test_init_debug():
    test_init_new(debug=True)


def test_init_new_with_name(short_flag: bool = False):
    project_dir = Path('.test_init_new_with_name')
    test_init_new(
        project_dir,
        name=project_dir.name[1:],
        short_flag=short_flag
    )


def test_init_new_with_name_short_flag():
    test_init_new_with_name(short_flag=True)


def test_init_exists(
        project_dir: Path = None,
        name: str = None,
        replace: bool = False,
        debug: bool = False,
        short_flag: bool = False
    ):
    if project_dir is None:
        project_dir = Path('.test_init_exists')

    init_process = init_project(
        project_dir,
        name=name,
        debug=debug,
        short_flag=short_flag
    )

    replace_process = init_project(
        project_dir,
        name=name,
        replace=replace,
        debug=debug,
        short_flag=short_flag
    )

    try:
        assert init_process.returncode == 0
        assert replace_process.returncode == (0 if replace else 1)

        validate_leantask_project(project_dir)

    except AssertionError as exc:
        raise exc

    finally:
        shutil.rmtree(project_dir)


def test_init_exists_debug():
    test_init_exists(debug=True)


def test_init_replace(
        name: str = None,
        debug: bool = False,
        short_flag: bool = False
    ):
    project_dir = Path('.test_init_replace')
    test_init_exists(
        project_dir,
        name=name,
        replace=True,
        debug=debug,
        short_flag=short_flag
    )


def test_init_replace_debug():
    test_init_replace(debug=True)


def test_init_replace_with_name(short_flag: bool = False):
    name = 'project_name'
    test_init_replace(
        name=name,
        short_flag=short_flag
    )


def test_init_replace_with_name_short_flag():
    test_init_replace_with_name(short_flag=True)
