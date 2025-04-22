# In any script or module
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import DataHandler, PipelineLogger
from pipeline.extractor.file_extractor import FileExtractor

logger = PipelineLogger(__name__).get_logger()


if __name__ == '__main__':
    path = 'incoming_data/2025-04-18/14/customer_profiles.csv'
    extractor = FileExtractor()
    response = extractor.extract(path)
    if response['valid']:
        data = response['data']
        print(data.head())
    else:
        print("No Data")