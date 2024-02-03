import logging

from .context import GlobalContext

LOG_SHORT_FORMAT = '%(message)s'
LOG_LONG_FORMAT = '[%(asctime)s] %(name)s - %(message)s'


class FlushFileHandler(logging.FileHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()


def get_logger(name: str = None):
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_LONG_FORMAT if GlobalContext.LOG_DEBUG else LOG_SHORT_FORMAT
    )

    logger = logging.getLogger(name if name is not None else 'leantask')

    if GlobalContext.LOG_DEBUG:
        logger.setLevel(logging.DEBUG)
    elif GlobalContext.LOG_QUIET:
        logger.setLevel(logging.ERROR)

    log_filename = GlobalContext.get_log_file_path()
    if log_filename is not None:
        handler = FlushFileHandler(str(log_filename))
        handler.setLevel(logging.DEBUG if GlobalContext.LOG_DEBUG else logging.INFO)
        handler.setFormatter(logging.Formatter(LOG_LONG_FORMAT))
        logger.addHandler(handler)

    return logger


def create_log_file(filename: str = None):
    GlobalContext.set_log_filename(filename)
    with open(GlobalContext.get_log_file_path(), 'w'):
        pass
