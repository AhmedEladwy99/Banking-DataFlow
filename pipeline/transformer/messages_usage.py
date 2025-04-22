import pandas as pd
from datetime import datetime
from utils import PipelineLogger

logger = PipelineLogger(__name__).get_logger()

class MessagesUsageTransformer:
    def __init__(self, partition_date, partition_hour):
        self.partition_date = partition_date
        self.partition_hour = partition_hour

    def caesar_cipher(self, text, shift):
        result = []
        for char in text:
            if char.isalpha():
                base = ord('A') if char.isupper() else ord('a')
                result.append(chr((ord(char) - base + shift) % 26 + base))
            else:
                result.append(char)
        return ''.join(result)
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass