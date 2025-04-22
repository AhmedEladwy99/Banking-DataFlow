import os
import sys
import json
import pandas as pd
from ..schema_validator import SchemaValidator
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import DataHandler, PipelineLogger

logger = PipelineLogger(__name__).get_logger()

class FileLoader:
    
    def __init__(self, data_frame: pd.DataFrame, output_path: str = "archive/", file_name: str = None, file_format='csv'):

        self.__output_path = output_path
        self.__data_frame = data_frame
        self.__file_name = file_name
        self.__file_format = file_format

    def load(self):
        
        DataHandler.save(self.__data_frame, self.__output_path, self.__file_name, self.__file_format)

