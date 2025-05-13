
import os
import sys
from . import *

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import  PipelineLogger


logger = PipelineLogger(__name__).get_logger()

class Pipeline():

    def __init__(self, input_file_path: str, output_path: str, output_file_name: str = None, output_file_format: str = 'csv'):
        self.__input_path = input_file_path
        self.__output_path = output_path
        self.__partition_date = input_file_path.split(os.sep)[-3]
        self.__partion_hour = input_file_path.split(os.sep)[-2]
        self.__output_file_name = output_file_name
        self.__output_file_format = output_file_format
        self.__dataframe = None
        self.__transformer_kind = input_file_path.split(os.sep)[-1].split('.')[0]
        self.__transformers = {
            'customer_profiles': CustomerTransformer, 
            'credit_cards_billing': BillingTransformer, 
            'loans': LoansTransformer, 
            'transactions': TransactionsTransformer,
            'support_tickets': SupportTicketsTransformer
        }

    def extract(self):
        response = FileExtractor.extract(self.__input_path)
        if response['valid']:
            self.__dataframe = response['data']

    def transform(self):
        transformer = self.__transformers[self.__transformer_kind](self.__partition_date, self.__partion_hour)
        self.__dataframe = transformer.transform(self.__dataframe)

    def load(self):
        loader = FileLoader(self.__dataframe, 
                            self.__output_path, 
                            self.__output_file_name, 
                            self.__output_file_format)
        loader.load()
    
    def run(self):
        self.extract()
        self.transform()
        self.load()
    
    def showDataFrame(self):
        print(self.__dataframe.head())

    def getDataFrame(self):
        return self.__dataframe

    def setInputPath(self, path: str):
        self.__input_path = path
    
    def setOutputPath(self, path: str):
        self.__output_path = path

    def __str__(self):
        return f"Pipeline(input_file_path={self.__input_path}, output_path={self.__output_path})"