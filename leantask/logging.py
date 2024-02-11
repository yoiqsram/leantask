import logging
from pathlib import Path

from .context import GlobalContext

LOG_SHORT_FORMAT = '[%(asctime)s] %(message)s'
LOG_LONG_FORMAT = '[%(asctime)s] %(levelname)s (%(name)s) %(message)s'


class FlushFileHandler(logging.FileHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

def get_logger(
        name: str = None,
        log_file_path: Path = None
    ) -> logging.Logger:
    logging_level = logging.INFO
    logging_format = LOG_SHORT_FORMAT
    
    if GlobalContext.LOG_DEBUG:
        logging_level = logging.DEBUG
        logging_format = LOG_LONG_FORMAT
    elif GlobalContext.LOG_QUIET:
        logging_level = logging.ERROR

    logger = logging.Logger(name if name is not None else 'leantask', logging.INFO)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter(logging_format))
    logger.addHandler(stream_handler)

    if log_file_path is not None:
        file_handler = FlushFileHandler(str(log_file_path))
        file_handler.setFormatter(logging.Formatter(LOG_LONG_FORMAT))
        logger.addHandler(file_handler)

    logger.setLevel(logging_level)
    return logger


def get_local_logger(name: str = None) -> logging.Logger:
    log_file_path = GlobalContext.get_local_log_file_path()
    return get_logger(name, log_file_path)


def get_flow_run_logger(
        flow_id: str,
        flow_run_id: str
    ) -> logging.Logger:
    log_file_path = GlobalContext.get_flow_run_log_file_path(
        flow_id,
        flow_run_id
    )
    return get_logger('flow', log_file_path)


def get_task_run_logger(
        flow_id: str,
        task_id: str,
        task_run_id: str
    ) -> logging.Logger:
    log_file_path = GlobalContext.get_task_run_log_file_path(
        flow_id,
        task_id,
        task_run_id
    )
    return get_logger('task', log_file_path)
