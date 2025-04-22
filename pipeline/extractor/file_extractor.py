import os
import sys
import json
import pandas as pd
from ..schema_validator import SchemaValidator

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import DataHandler, PipelineLogger


logger = PipelineLogger(__name__).get_logger()


class FileExtractor:

    @staticmethod
    def extract(file_path: str) -> pd.DataFrame:
        """
        Extracts data from a file and returns a DataFrame.
        
        Args:
            file_path (str): Full path to the data file.
        
        Returns:
            pd.DataFrame: Extracted data.
        """
        df = DataHandler.read_file(file_path)

        schema_validator = SchemaValidator()
        file_name = file_path.split('/')[-1]

        if schema_validator.validate(file_name, df):

            return {
                'data': df, 
                'valid': True
            }
        
        return {
            'data': None,
            'valid': False
        }
