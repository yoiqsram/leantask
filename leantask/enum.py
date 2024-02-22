from enum import Enum
from typing import Any


class UnknownEnumMixin:
    @classmethod
    def _missing_(cls, value: object) -> Any:
        return cls.UNKNOWN


class FlowIndexStatus(UnknownEnumMixin, Enum):
    UPDATED = 0
    '''Successfully indexed all changes in the flow script.'''

    UNCHANGED = 10
    '''No change was found in the flow script.'''

    FAILED = 20
    '''An error is occured while indexing the flow script.'''

    UNKNOWN = 1


class FlowScheduleStatus(UnknownEnumMixin, Enum):
    SCHEDULED = 0
    '''Successfully set schedule for the flow.'''

    NO_SCHEDULE = 10
    '''The flow has no schedule.'''

    FAILED = 20
    '''An error is occured while indexing the flow script.'''

    FAILED_SCHEDULE_EXISTS = 21
    '''The flow has already been scheduled.'''

    UNKNOWN = 1


class FlowRunStatus(UnknownEnumMixin, Enum):
    DONE = 0
    '''Successfully run all tasks in the flow.'''

    SCHEDULED = 10
    '''The flow has been scheduled.'''

    SCHEDULED_BY_USER = 11
    '''The flow has been scheduled manually by user.'''

    CANCELED = 12
    '''The flow has been scheduled and waiting to run, but it's set to inactive thus schedule is canceled.'''

    CANCELED_BY_USER = 13
    '''The flow has been scheduled and waiting to run, but user has cancel the run thus schedule is canceled.'''

    RUNNING = 20
    '''Some tasks are running in the flow.'''

    FAILED = 30
    '''Tasks has been run, but the endpoint task in the flow is failed.'''

    FAILED_TIMEOUT_DELAY = 31
    '''The flow is failed due to late run over the max delay time.'''

    FAILED_TIMEOUT_RUN = 32
    '''The flow is failed due to running time was too long and reached time out.'''

    UNKNOWN = 1


class TaskRunStatus(UnknownEnumMixin, Enum):
    DONE = 0
    '''Successfully run the task.'''

    PENDING = 10
    '''The task has been scheduled and is waiting to be executed.'''

    CANCELED = 11
    '''The flow, task parent, has been canceled while waiting to run thus it's canceled.'''

    RUNNING = 20
    '''The task is running.'''

    FAILED = 30
    '''An error is occured during running the task.'''

    FAILED_TIMEOUT_DELAY = 31
    '''The task is failed due to late run over the max delay time.'''

    FAILED_TIMEOUT_RUN = 32
    '''The task is failed due to running time was too long and reached time out.'''

    FAILED_BY_USER = 33
    '''The task is failed due to user interuption.'''

    FAILED_UPSTREAM = 39
    '''The task is failed due to failure on the upstream task(s).'''

    UNKNOWN = 1


class TableName(Enum):
    METADATA = 'metadata'

    FLOW = 'flows'
    FLOW_SCHEDULE = 'flow_schedules'
    FLOW_RUN = 'flow_runs'

    TASK = 'tasks'
    TASK_DOWNSTREAM = 'task_downstreams'
    TASK_SCHEDULE = 'task_schedules'
    TASK_RUN = 'task_runs'


class LogTableName(Enum):
    FLOW = 'flows'
    FLOW_RUN = 'flow_runs'

    TASK = 'tasks'
    TASK_DOWNSTREAM = 'task_downstreams'
    TASK_RUN = 'task_runs'

    SCHEDULER_SESSION = 'scheduler_sessions'
