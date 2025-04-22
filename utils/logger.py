import logging
import os
from datetime import datetime

class PipelineLogger:
    def __init__(self, name: str, log_dir: str = "logs"):
        self.name = name
        self.log_dir = log_dir
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        os.makedirs(self.log_dir, exist_ok=True)
        log_date = datetime.now().strftime("%Y-%m-%d")
        log_path = os.path.join(self.log_dir, f"{log_date}.log")

        logger = logging.getLogger(self.name)
        logger.setLevel(logging.DEBUG)

        # Prevent adding multiple handlers in interactive sessions
        if not logger.handlers:
            formatter = logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] %(name)s - %(filename)s:%(lineno)d - %(funcName)s() :: %(message)s"
            )

            file_handler = logging.FileHandler(log_path, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)

            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(logging.INFO)
            stream_handler.setFormatter(formatter)

            logger.addHandler(file_handler)
            logger.addHandler(stream_handler)

        return logger

    def get_logger(self) -> logging.Logger:
        return self.logger
