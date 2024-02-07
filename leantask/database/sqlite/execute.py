from typing import Any, Dict, List

from ...context import GlobalContext
from ...enum import FlowRunStatus
from ...utils.string import generate_uuid


def update_flow_run_status_to_db(flow_run) -> None:
    if GlobalContext.LOCAL_RUN:
        return

    from .common import delete, insert, log_insert, query, update
    from ...enum import TableName, LogTableName

    record = {
        'id': flow_run.id,
        'flow_id': flow_run.flow.id,
        'status': flow_run.status.name,
        'schedule_datetime': flow_run.schedule_datetime,
        'max_delay': flow_run.max_delay,
        'flow_schedule_id': flow_run.schedule_id
    }

    flow_run_exists = query('select count(*) from flow_runs where id = ?', (flow_run.id, ))[0][0] > 0
    if flow_run_exists:
        record['modified_datetime'] = flow_run.modified_datetime
        items_set = {column: record[column] for column in ['status', 'modified_datetime']}
        items_filter = {column: record[column] for column in ['id']}

        update(TableName.FLOW_RUN.value, items_set, items_filter)

    else:
        record['created_datetime'] = flow_run.created_datetime

        insert(TableName.FLOW_RUN.value, record)

    if flow_run.schedule_id is not None \
            and flow_run.status in (
                FlowRunStatus.CANCELED, FlowRunStatus.CANCELED_BY_USER,
                FlowRunStatus.DONE, FlowRunStatus.FAILED
            ):
        delete(TableName.FLOW_SCHEDULE.value, {'id': flow_run.schedule_id})

    log_record = {
        'id': generate_uuid(),
        'status': record['status'],
        'schedule_datetime': record['schedule_datetime'],
        'max_delay': record['max_delay'],
        'ref_id': record['id'],
        'ref_flow_id': record['flow_id'],
        'ref_flow_schedule_id': record['flow_schedule_id'],
        'created_datetime': flow_run.modified_datetime,
    }
    log_insert(LogTableName.FLOW_RUN.value, log_record)


def update_task_run_status_to_db(task_run) -> None:
    if GlobalContext.LOCAL_RUN:
        return

    from .common import query, insert, update, log_insert
    from ...enum import TableName, LogTableName

    record = {
        'id': task_run.id,
        'flow_run_id': task_run.flow_run.id,
        'task_id': task_run.task.id,
        'attempt': task_run.attempt,
        'retry_max': task_run.retry_max,
        'retry_delay': task_run.retry_max,
        'status': task_run.status.name
    }

    record_exists = query('select count(*) from task_runs where id = ?', (task_run.id, ))[0][0] > 0
    if record_exists:
        record['modified_datetime'] = task_run.modified_datetime
        items_set = {column: record[column] for column in ['status', 'modified_datetime']}
        items_filter = {column: record[column] for column in ['id']}

        update(TableName.TASK_RUN.value, items_set, items_filter)

    else:
        record['created_datetime'] = task_run.created_datetime

        insert(TableName.TASK_RUN.value, record)

    log_record = {
        'id': generate_uuid(),
        'attempt': record['attempt'],
        'retry_max': record['retry_max'],
        'retry_delay': record['retry_delay'],
        'status': record['status'],
        'ref_id': record['id'],
        'ref_flow_run_id': record['flow_run_id'],
        'ref_task_id': record['task_id'],
        'created_datetime': task_run.modified_datetime,
    }
    log_insert(LogTableName.TASK_RUN.value, log_record)
