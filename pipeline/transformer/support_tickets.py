import pandas as pd
from datetime import datetime
from utils import PipelineLogger
from .transformer import Transformer

logger = PipelineLogger(__name__).get_logger()

class SupportTicketsTransformer(Transformer):
    
    def __init__(self, partition_date: str, partition_hour: int):
        super().__init__(partition_date, partition_hour)
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        #add a new column to the dataframe called age representing the number of days since the ticket was created
        df['age']= (datetime.now() - pd.to_datetime(df['complaint_date'])).dt.days
        return df