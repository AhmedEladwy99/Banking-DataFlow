import pandas as pd
from datetime import datetime
from utils import PipelineLogger

logger = PipelineLogger(__name__).get_logger()


class CustomerTransformer:

    def __init__(self, partition_date: str, partition_hour: int):
        self.partition_date = partition_date
        self.partition_hour = partition_hour


    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Started transforming customer profile data")

        # Ensure sign_up_date is datetime
        df['sign_up_date'] = pd.to_datetime(df['sign_up_date'], errors='coerce')

        # Compute years since joining
        current_date = datetime.now()
        df['years'] = df['sign_up_date'].apply(
            lambda d: round((current_date - d).days / 365) if pd.notnull(d) else None
        ).astype("Int64")

        # Assign customer_tier
        def get_tier(years):
            if pd.isnull(years):
                return "Unknown"
            elif years > 5:
                return "Loyal"
            elif years < 1:
                return "Newcomer"
            else:
                return "Normal"

        df['customer_tier'] = df['years'].apply(get_tier)

        # Add processing metadata
        df['processing_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['partition_date'] = self.partition_date
        df['partition_hour'] = self.partition_hour

        logger.info(f"Finished transforming customer data. Total records: {len(df)}")
        return df
    



