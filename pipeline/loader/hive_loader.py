import os
import sys
import json
import pandas as pd
from datetime import datetime
from utils import HiveHandler

class HiveLoader:

    @staticmethod
    def load(dataframe, table):
        hive = HiveHandler().connect()
        file_format = 'orc'
        hive.upload2(dataframe, table, database="staging", file_format=file_format)
        hive.close()
