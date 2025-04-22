import os
import sys
import json
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import DataHandler, PipelineLogger

logger = PipelineLogger(__name__).get_logger()

class SchemaValidator:

    def __init__(self, schema_config_path: str = 'pipeline/config/schema.json'):
        """
        Initializes SchemaValidator with a hardcoded or config-based schema.
        """
        self.schemas = self._load_schemas(schema_config_path)

    def _load_schemas(self, config_path):
        
        with open(config_path, "r") as file:
            return json.load(file)
        
    def validate(self, file_name: str, df: pd.DataFrame) -> bool:
        expected_columns = self.schemas.get(file_name)

        if not expected_columns:
            logger.warning(f"No schema found for file: {file_name}")
            return False

        actual_columns = df.columns.tolist()
        if actual_columns != expected_columns:
            logger.error(
                f"Schema mismatch in {file_name}.\nExpected: {expected_columns}\nFound: {actual_columns}"
            )
            return False

        logger.info(f"Schema validation passed for {file_name}")
        return True
