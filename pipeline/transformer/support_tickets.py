import pandas as pd
from datetime import datetime
from utils import PipelineLogger
from .transformer import Transformer

logger = PipelineLogger(__name__).get_logger()

class SupportTicketsTransformer(Transformer):
    
    def __init__(self, partition_date: str, partition_hour: int):
        super().__init__(partition_date, partition_hour)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
