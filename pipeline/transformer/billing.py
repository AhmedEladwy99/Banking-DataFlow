import pandas as pd
from datetime import datetime
from utils import PipelineLogger
from .transformer import Transformer
logger = PipelineLogger(__name__).get_logger()


class BillingTransformer(Transformer):
    
    def __init__(self, partition_date: str, partition_hour: int):
        super().__init__(partition_date, partition_hour)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        
    #Add a new Boolean column named fully_paid, set to True if the customer has paid the fullbill amount, and False otherwise.
    #bill_id, customer_id, month, amount_due, amount_paid, payment_date.
        if df['amount_due']== df['amount_paid']:
            df['fully_paid'] = True
        else:
            df['fully_paid'] = False

    #Add a new Integer column named debt, representing the remaining due amount afterpayment.
        df['debt'] = df['amount_due'] - df['amount_paid']
   # Add a new Integer column named late_days, representing the number of days between the bill’s due date (always the 1st of each month) and the actual payment date.
        df['late_days'] = (pd.to_datetime(df['payment_date']) - pd.to_datetime(df['month'])).dt.days
   # Add a new Float column named fine, representing the fine charged to customers for late payments, calculated as: Fine = late_days * 5.15
        df['fine'] = df['late_days'] * 5.15
   # Add a new Float column named total_amount, calculated as: total_amount = amount_due + fine
        df['total_amount'] = df['amount_due'] + df['fine']
        return df
    