import pandas as pd
from abc import ABC, abstractmethod

class Transformer(ABC):

    def __init__(self, partition_date: str, partition_hour: int):
        self.partition_date = partition_date
        self.partition_hour = partition_hour
    
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


