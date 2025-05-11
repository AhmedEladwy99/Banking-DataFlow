import pandas as pd
from datetime import datetime
from utils import PipelineLogger, CryptoHelper
from .transformer import Transformer
import uuid

logger = PipelineLogger(__name__).get_logger()



class LoansTransformer(Transformer):
    
    def __init__(self, partition_date: str, partition_hour: int):
        super().__init__(partition_date, partition_hour)
        self.crypto_helper = CryptoHelper(self.partition_date, self.partition_hour)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:

        df['loan_reason'] = df['loan_reason'].apply(
            lambda x: 
                self.crypto_helper.encrypt(x)
            )
        df['processing_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['partition_date'] = self.partition_date
        df['partition_hour'] = self.partition_hour

        return df
    
    def decrypt(self, df: pd.DataFrame) -> pd.DataFrame:
        df['loan_reason'] = df['loan_reason'].apply(
            lambda x: 
                self.crypto_helper.decrypt(x, x.name), axis = 1
            )
        return df

