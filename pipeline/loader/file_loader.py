import os
import sys
import json
import pandas as pd
from datetime import datetime

from ..schema_validator import SchemaValidator
from utils import DataHandler, PipelineLogger

logger = PipelineLogger(__name__).get_logger()

class FileLoader:
    SUPPORTED_FORMATS = {'csv', 'json', 'parquet'}

    def __init__(self, data_frame: pd.DataFrame, output_path: str = "archive/", file_name: str = None, file_format: str = 'csv'):
        self.output_path = output_path
        self.data_frame = data_frame
        self.file_format = file_format.lower()

        # Generate default file name if not provided
        if file_name is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.file_name = f"export_{timestamp}.{self.file_format}"
        else:
            self.file_name = file_name if file_name.endswith(f".{self.file_format}") else f"{file_name}.{self.file_format}"

        self._validate_format()

    def _validate_format(self):
        if self.file_format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported file format '{self.file_format}'. Supported formats: {self.SUPPORTED_FORMATS}")

    def load(self):
        try:
            DataHandler.save(self.data_frame, self.output_path, self.file_name, self.file_format)
            logger.info(f"File saved successfully to {os.path.join(self.output_path, self.file_name)}")
        except Exception as e:
            logger.error(f"Error saving file: {e}")
            raise
