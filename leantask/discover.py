import ast
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Set

from .context import GlobalContext
from .database import FlowModel
from .enum import FlowIndexStatus
from .logging import get_logger
from .utils.path import is_file_match_patterns, parse_gitignore_patterns
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
    gitignore_path = GlobalContext.PROJECT_DIR / '.gitignore'
    if gitignore_path.exists():
        gitignore_patterns = parse_gitignore_patterns(gitignore_path)
    else:
        gitignore_patterns = []

    flow_checksums = dict()
    for file_path in GlobalContext.PROJECT_DIR.rglob('*.py'):
        file_path = GlobalContext.relative_path(file_path)

        if is_file_match_patterns(file_path, gitignore_patterns):
            continue

        if not has_declare_flow(file_path):
            continue

        flow_checksums[file_path] = calculate_md5(file_path)

    return flow_checksums


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


def index_all_flows(
        flow_models: Set = None,
        log_file_path: Path = None
    ) -> Dict[FlowModel, FlowIndexStatus]:
    logger = get_logger('discover', log_file_path)

    flow_checksums = find_flow_checksums()

    if flow_models is None:
        logger.debug('Get flow records from database.')
        flow_models = list(FlowModel.select())
    else:
        flow_models = flow_models.copy()

    total_changes = 0
    updated_flow_models = {
        flow_model: FlowIndexStatus.UNCHANGED
        for flow_model in flow_models
    }
    for flow_model in flow_models:
        flow_path = Path(flow_model.path)
        if flow_path not in flow_checksums:
            logger.info(f"Flow '{flow_model.name}' from '{flow_model.path}' has been removed.")
            total_changes += 1
            continue

        if flow_model.checksum != flow_checksums[flow_path]:
            logger.info(f"Flow '{flow_model.name}' from '{flow_model.path}' has been changed.")
            index_status = index_flow(flow_path, log_file_path, GlobalContext.SCHEDULER_SESSION_ID)
            if index_status == FlowIndexStatus.UPDATED:
                total_changes += 1

            updated_flow_model = (
                FlowModel.select()
                .where(FlowModel.path == flow_model.path)
                .limit(1)
                [0]
            )
            updated_flow_models[updated_flow_model] = index_status
            continue

    new_flow_paths = set(flow_checksums.keys()) - set(Path(flow_model.path) for flow_model in flow_models)
    for flow_path in new_flow_paths:
        logger.info(f"Found a new flow from '{flow_path}'.")

        index_status = index_flow(
            flow_path=flow_path,
            log_file_path=log_file_path,
            scheduler_session_id=GlobalContext.SCHEDULER_SESSION_ID
        )
        updated_flow_model = (
            FlowModel.select()
            .where(FlowModel.path == str(flow_path))
            .limit(1)
            [0]
        )
        updated_flow_models[updated_flow_model] = index_status

        if index_status == FlowIndexStatus.UPDATED:
            total_changes += 1

    logger.debug(f'Total changes made on flows: {total_changes}.')
    return updated_flow_models
