import pandas as pd
from datetime import datetime
from utils import PipelineLogger
from .transformer import Transformer


logger = PipelineLogger(__name__).get_logger()

class TransactionsTransformer(Transformer):
    
    def __init__(self, partition_date: str, partition_hour: int):
        super().__init__(partition_date, partition_hour)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        
    #create a new column called cost representing the cost of the transaction
    #knowing that each transaction costs the customer 50 cents + 0.1 percent of the transaction amount
    #cost = 0.5 + 0.1 * transaction_amount
       
        #cost =  * quantity
        df['cost'] = 0.5 + ( df['transaction_amount']*0.001)
        df['total_amount']=df['cost'] + df['transaction_amount']
        return df

        