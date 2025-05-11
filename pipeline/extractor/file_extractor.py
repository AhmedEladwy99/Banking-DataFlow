import os
import sys
import json
import pandas as pd
from ..schema_validator import SchemaValidator
from utils import GmailNotifier

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import DataHandler, PipelineLogger


logger = PipelineLogger(__name__).get_logger()


class FileExtractor:

    @staticmethod
    def extract(file_path: str) -> dict:

        df = DataHandler.read_file(file_path)

        schema_validator = SchemaValidator()
        file_name = file_path.split('/')[-1]

        if schema_validator.validate(file_name, df):

            return {
                'data': df, 
                'valid': True
            }
        
        GmailNotifier().send_email_if_error(f"{file_name} cannot be extracted. Ensure the host is correct and accessible.")

        return {
            'data': None,
            'valid': False
        }
