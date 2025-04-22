import os
import pandas as pd
import shutil
from .logger import PipelineLogger
from datetime import datetime
logger = PipelineLogger(__name__).get_logger()


class DataHandler:

    def __init__(self, file_path=None, data_frame=None):
        self.__file_path = file_path
        self.__data_frame = data_frame

    @staticmethod
    def save(data_frame, output_path, file_name=None, file_format='csv'):
        """Saves a DataFrame to a specified path and format (default is CSV)."""
        
        if not os.path.exists(output_path):
            logger.error(f"Error: The Path {output_path} was not found.")
            return
        
        if file_name == None:
            file_name = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}.{file_format}'

        output_path = os.path.join(output_path, file_name)

        try:
            # Check the file format and save accordingly
            if file_format == 'csv':
                data_frame.to_csv(output_path, index=False)
            elif file_format == 'excel':
                data_frame.to_excel(output_path, index=False)
            elif file_format == 'json':
                data_frame.to_json(output_path, orient='records', lines=True)
            elif output_path.endswith('parquet'):
                data_frame.to_parquet(output_path, index=False)
            else:
                logger.error(f"Error: Unsupported file format '{file_format}'.")
                return

            logger.info(f"File Loaded successfully at {output_path}")
        except Exception as e:
            logger.error(f"Error Loading file: {e}")

    @staticmethod
    def copy_file(input_path, output_path):
        """Copies a file from one path to another."""
        try:
            shutil.copy(input_path, output_path)
            logger.info(f"File copied successfully from {input_path} to {output_path}")
        except FileNotFoundError:
            logger.error(f"Error: The file at {input_path} was not found.")
        except Exception as e:
            logger.error(f"Error copying file: {e}")

    @staticmethod
    def read_file(input_path):
        """Reads a file into a pandas DataFrame (supporting both CSV and JSON)."""
        if input_path.endswith('.csv'):
            return DataHandler.read_csv(input_path)
        elif input_path.endswith('.json'):
            return DataHandler.read_json(input_path)
        elif input_path.endswith('.txt'):
            return DataHandler.read_txt(input_path)
        else:
            logger.error(f"Unsupported file type: {input_path}")
            return None

    @staticmethod
    def read_json(input_path):
        """Reads a JSON file into a pandas DataFrame."""
        try:
            data_frame = pd.read_json(input_path)
            logger.info(f"File {input_path} read successfully.")
            return data_frame
        except pd.errors.ParserError as e:
            logger.error(f"ParserError reading Text file: {e}")
        except FileNotFoundError:
            logger.error(f"Error: The file at {input_path} was not found.")
        except Exception as e:
            logger.error(f"Error reading Text file: {e}")
        return None

    @staticmethod
    def read_csv(input_path):
        """Reads a CSV file into a pandas DataFrame."""
        try:
            data_frame = pd.read_csv(input_path)
            logger.info(f"File {input_path} read successfully.")
            return data_frame
        except pd.errors.ParserError as e:
            logger.error(f"ParserError reading Text file: {e}")
        except FileNotFoundError:
            logger.error(f"Error: The file at {input_path} was not found.")
        except Exception as e:
            logger.error(f"Error reading Text file: {e}")
        return None

    @staticmethod
    def read_txt(input_path, sep='|'):
        """Reads a pipe-separated .txt file into a pandas DataFrame."""
        try:
            data_frame = pd.read_csv(input_path, sep=sep)
            logger.info(f"File {input_path} read successfully.")
            return data_frame
        except pd.errors.ParserError as e:
            logger.error(f"ParserError reading Text file: {e}")
        except FileNotFoundError:
            logger.error(f"Error: The file at {input_path} was not found.")
        except Exception as e:
            logger.error(f"Error reading Text file: {e}")
        return None