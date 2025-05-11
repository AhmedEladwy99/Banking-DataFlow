import pandas as pd
from datetime import datetime
from utils import PipelineLogger
from .transformer import Transformer

logger = PipelineLogger(__name__).get_logger()

class BillingTransformer(Transformer):

    def __init__(self, partition_date: str, partition_hour: int):
        super().__init__(partition_date, partition_hour)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            # Convert dates
            df['payment_date'] = pd.to_datetime(df['payment_date'])
            df['due_date'] = pd.to_datetime(df['month'].astype(str) + '-01')

            # fully_paid column (Boolean)
            df['fully_paid'] = df['amount_due'] == df['amount_paid']

            # debt column (Integer)
            df['debt'] = (df['amount_due'] - df['amount_paid']).astype(int)

            # late_days column (Integer), set to 0 if payment was early
            df['late_days'] = (df['payment_date'] - df['due_date']).dt.days
            df['late_days'] = df['late_days'].apply(lambda x: max(x, 0))

            # fine column (Float)
            df['fine'] = df['late_days'] * 5.15

            # total_amount column (Float)
            df['total_amount'] = df['amount_due'] + df['fine']

            # Drop helper column
            df.drop(columns=['due_date'], inplace=True)

            logger.info("Billing transformation completed successfully.")
            return df

        except Exception as e:
            logger.error(f"Error during billing transformation: {e}")
            raise
