import ast
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Set

from .context import GlobalContext
from .enum import FlowIndexStatus
from .logging import get_logger
from .utils.script import calculate_md5
from .utils.string import quote


FLOW_MODULES = [
    'leantask',
    'leantask.flow',
    'leantask.flow.flow'
]
FLOW = 'Flow'
FLOW_CALLABLES = ['.'.join((module, FLOW)) for module in FLOW_MODULES]


def is_flow_module_node(node) -> bool:
    if isinstance(node, ast.Import):
        for alias in node.names:
            if alias.name in FLOW_MODULES:
                return True

    elif isinstance(node, ast.ImportFrom):
        if node.module in FLOW_MODULES:
            return True

    return False


def is_flow_call_node(node) -> bool:
    if not isinstance(node, ast.Call):
        return False

    if isinstance(node.func, ast.Name):
        if node.func.id == FLOW:
            return True

    elif isinstance(node.func, ast.Attribute)\
            and isinstance(node.func.value, ast.Name):
        module_name = node.func.value.id
        if module_name in FLOW_MODULES and node.func.attr == FLOW:
            return True

    return False


def has_declare_flow(file_path: Path) -> bool:
    with open(file_path, 'r') as file:
        tree = ast.parse(file.read(), filename=file_path)

    has_import_flow_module = False
    has_init_flow = False
    for node in ast.walk(tree):
        if not has_import_flow_module:
            has_import_flow_module = is_flow_module_node(node)

        if not has_init_flow:
            has_init_flow = is_flow_call_node(node)

        if has_import_flow_module and has_init_flow:
            return True

    return False


def find_flow_checksums() -> Dict[Path, str]:
    return {
        GlobalContext.relative_path(file_path): calculate_md5(file_path)
        for file_path in GlobalContext.PROJECT_DIR.rglob('*.py')
        if has_declare_flow(file_path)
    }


def index_flow(
        flow_path: Path,
        log_file_path: Path = None,
        scheduler_session_id: str = None
    ):
    command = ' '.join(
        [quote(sys.executable), quote(flow_path), 'index']
        + (['--log-file', quote(log_file_path)] if log_file_path is not None else [])
        + (['--scheduler-session-id', scheduler_session_id] if scheduler_session_id is not None else [])
    )
    flow_index_result = subprocess.run(command, shell=True)
    return FlowIndexStatus(flow_index_result.returncode)


def update_flow_records(
        flow_records: Set,
        log_file_path: Path
    ) -> Dict[Any, FlowIndexStatus]:
    from .database.execute import get_flow_record_by_path, get_flow_records
    from .database.orm import open_db_session

    logger = get_logger('discover', log_file_path)

    total_changes = 0
    flow_checksums = find_flow_checksums()
    updated_flow_records = dict()
    with open_db_session(GlobalContext.database_path()) as session:
        if flow_records is None:
            logger.debug('Get flow records from database.')
            flow_records = get_flow_records(session=session)
        else:
            flow_records = flow_records.copy()

        for flow_record in flow_records:
            flow_path = Path(flow_record.path)
            if flow_path not in flow_checksums:
                logger.info(f"Flow '{flow_record.name}' from '{flow_record.path}' has been removed.")
                session.delete(flow_record)
                session.commit()
                total_changes += 1
                continue

            if flow_record.checksum != flow_checksums[flow_path]:
                logger.info(f"Flow '{flow_record.name}' from '{flow_record.path}' has been changed.")
                index_status = index_flow(flow_path, log_file_path, GlobalContext.SCHEDULER_SESSION_ID)
                if index_status == FlowIndexStatus.UPDATED:
                    total_changes += 1

                updated_flow_record = get_flow_record_by_path(flow_path, session=session)
                updated_flow_records[updated_flow_record] = index_status
                continue

            updated_flow_records[flow_record] = FlowIndexStatus.UNCHANGED

        new_flow_paths = set(flow_checksums.keys()) - set(Path(flow_record.path) for flow_record in flow_records)
        for flow_path in new_flow_paths:
            logger.info(f"Found a new flow from '{flow_path}'.")
            index_status = index_flow(
                flow_path=flow_path,
                log_file_path=log_file_path,
                scheduler_session_id=GlobalContext.SCHEDULER_SESSION_ID
            )
            updated_flow_record = get_flow_record_by_path(flow_path, session=session)
            updated_flow_records[updated_flow_record] = index_status
            if index_status == FlowIndexStatus.UPDATED:
                total_changes += 1

        logger.debug(f'Total changes made on flows: {total_changes}.')

    return updated_flow_records
