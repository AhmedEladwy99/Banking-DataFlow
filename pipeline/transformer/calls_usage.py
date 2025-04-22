import pandas as pd
from datetime import datetime
from utils import PipelineLogger

logger = PipelineLogger(__name__).get_logger()

class CallsUsageTransformer:
    
    def __init__(self, partition_date, partition_hour):
        self.partition_date = partition_date
        self.partition_hour = partition_hour

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass