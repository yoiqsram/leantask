import os
import shutil
import subprocess
import sys
from pathlib import Path

from leantask.context import GlobalContext
from leantask.database.base import open_db_connection
from leantask.enum import FlowRunStatus
from leantask.utils.string import quote
from tests.cli.main.test_init import init_project


def validate_discover_process(project_dir: Path):
    command = ' '.join(
        ['export', f"PYTHONPATH={os.getcwd()}", '&&']
        + [f'"{sys.executable}"', '-m', 'leantask', 'discover']
    )
    discover_process = subprocess.run(
        command,
        shell=True,
        cwd=project_dir
    )
    assert discover_process.returncode == 0


def validate_flow_run_process(
        project_dir: Path,
        flow_file_path: Path,
        return_status: FlowRunStatus = FlowRunStatus.DONE
    ):
    command = ' '.join(
        ['export', f"PYTHONPATH={os.getcwd()}", '&&']
        + [f'yes | "{sys.executable}"', quote(flow_file_path.relative_to(project_dir)), 'run']
    )
    run_process = subprocess.run(
        command,
        shell=True,
        cwd=project_dir
    )
    assert run_process.returncode == return_status.value


def test_hello_world():
    project_dir = Path('.test_hello_world')

    init_process = init_project(project_dir)
    try:
        assert init_process.returncode == 0

        flow_file_path = project_dir / '01_hello_world.py'
        shutil.copy(
            GlobalContext.PROJECT_DIR / 'examples' / flow_file_path.relative_to(project_dir),
            flow_file_path,
        )

        validate_discover_process(project_dir)
        validate_flow_run_process(project_dir, flow_file_path)

    except:
        raise

    finally:
        shutil.rmtree(project_dir)


def test_task_attributes():
    project_dir = Path('.test_task_attributes')

    init_process = init_project(project_dir)
    try:
        assert init_process.returncode == 0

        flow_file_path = project_dir / '02_task_attributes.py'
        shutil.copy(
            GlobalContext.PROJECT_DIR / 'examples' / flow_file_path.relative_to(project_dir),
            flow_file_path
        )

        validate_discover_process(project_dir)
        validate_flow_run_process(project_dir, flow_file_path)

    except:
        raise

    finally:
        shutil.rmtree(project_dir)


def test_task_input_output():
    project_dir = Path('.task_input_output')

    init_process = init_project(project_dir)
    try:
        assert init_process.returncode == 0

        flow_file_path = project_dir / '03_task_input_output.py'
        shutil.copy(
            GlobalContext.PROJECT_DIR / 'examples' / flow_file_path.relative_to(project_dir),
            flow_file_path
        )

        validate_discover_process(project_dir)
        validate_flow_run_process(project_dir, flow_file_path)

    except:
        raise

    finally:
        shutil.rmtree(project_dir)


def test_task_dependencies():
    project_dir = Path('.test_task_dependencies')

    init_process = init_project(project_dir)
    try:
        assert init_process.returncode == 0

        flow_file_path = project_dir / '04_task_dependencies.py'
        shutil.copy(
            GlobalContext.PROJECT_DIR / 'examples' / flow_file_path.relative_to(project_dir),
            flow_file_path
        )

        validate_discover_process(project_dir)
        validate_flow_run_process(project_dir, flow_file_path)

    except:
        raise

    finally:
        shutil.rmtree(project_dir)


def test_task_schedules():
    project_dir = Path('.test_task_schedules')

    init_process = init_project(project_dir)
    try:
        assert init_process.returncode == 0

        flow_file_path = project_dir / '05_task_schedules.py'
        shutil.copy(
            GlobalContext.PROJECT_DIR / 'examples' / flow_file_path.relative_to(project_dir),
            flow_file_path
        )

        validate_discover_process(project_dir)

        # Run manually
        validate_flow_run_process(
            project_dir,
            flow_file_path,
            return_status=FlowRunStatus.FAILED
        )

        # Run by schedule
        ...

    except:
        raise

    finally:
        shutil.rmtree(project_dir)


def test_task_module():
    project_dir = Path('.test_task_module')

    init_process = init_project(project_dir)
    try:
        assert init_process.returncode == 0

        flow_file_path = project_dir / 'examples' / '06_task_module.py'
        flow_file_path.parent.mkdir(parents=True)
        shutil.copy(
            GlobalContext.PROJECT_DIR / flow_file_path.relative_to(project_dir),
            flow_file_path
        )

        module_dir = project_dir / 'examples' / 'task_module'
        shutil.copytree(
            GlobalContext.PROJECT_DIR / module_dir.relative_to(project_dir),
            module_dir
        )

        validate_discover_process(project_dir)
        validate_flow_run_process(project_dir, flow_file_path)

    except:
        raise

    finally:
        shutil.rmtree(project_dir)


def test_task_long():
    project_dir = Path('.test_task_long')

    init_process = init_project(project_dir)
    try:
        assert init_process.returncode == 0

        flow_file_path = project_dir / '07_task_long.py'
        shutil.copy(
            GlobalContext.PROJECT_DIR / 'examples' / flow_file_path.relative_to(project_dir),
            flow_file_path
        )

        validate_discover_process(project_dir)
        validate_flow_run_process(project_dir, flow_file_path)

    except:
        raise

    finally:
        shutil.rmtree(project_dir)


def test_task_failed_downstream():
    project_dir = Path('.test_task_failed_downstream')

    init_process = init_project(project_dir)
    try:
        assert init_process.returncode == 0

        flow_file_path = project_dir / '08_task_failed_downstream.py'
        shutil.copy(
            GlobalContext.PROJECT_DIR / 'examples' / flow_file_path.relative_to(project_dir),
            flow_file_path
        )

        validate_discover_process(project_dir)
        validate_flow_run_process(
            project_dir,
            flow_file_path,
            return_status=FlowRunStatus.FAILED
        )

    except:
        raise

    finally:
        shutil.rmtree(project_dir)
