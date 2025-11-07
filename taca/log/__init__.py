"""TACA logging module for external scripts"""

import logging
from datetime import datetime

# session id is the timestamp when this module is imported (script start)
SESSION_ID = datetime.now().strftime("%y%m%d%H%M")


class SessionFilter(logging.Filter):
    """Attach session_id (script start timestamp) to every LogRecord."""

    def filter(self, record):
        record.session_id = SESSION_ID
        return True


# get root logger
ROOT_LOG = logging.getLogger()
ROOT_LOG.setLevel(logging.INFO)

# Console logger with session_id in format
formatter = logging.Formatter(
    "%(asctime)s - %(session_id)s - %(name)s - %(levelname)s - %(message)s"
)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.addFilter(SessionFilter())
ROOT_LOG.addHandler(stream_handler)

LOG_LEVELS = {
    "ERROR": logging.ERROR,
    "WARN": logging.WARN,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}


def init_logger_file(log_file, log_level="INFO"):
    """Append a FileHandler to the root logger.

    :param str log_file: Path to the log file
    :param str log_level: Logging level
    """
    ROOT_LOG.handlers = []
    log_level = (
        LOG_LEVELS[log_level] if log_level in LOG_LEVELS.keys() else logging.INFO
    )

    ROOT_LOG.setLevel(log_level)

    file_handle = logging.FileHandler(log_file)
    file_handle.setLevel(log_level)
    file_handle.setFormatter(formatter)
    file_handle.addFilter(SessionFilter())
    ROOT_LOG.addHandler(file_handle)
    ROOT_LOG.addHandler(stream_handler)
