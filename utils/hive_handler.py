from pyhive import hive
import pandas as pd
import os
from .datahandler import FileHandler

class HiveHandler:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(HiveHandler, cls).__new__(cls)
            cls._instance._initialized = False  # One-time init flag
        return cls._instance

    def __init__(self, host='localhost', port=10000, database='default'):
        if self._initialized:
            return
        self.host = host
        self.port = port
        self.database = database
        self.conn = None
        self.cursor = None
        self._initialized = True

    def connect(self):
        if not self.conn:
            self.conn = hive.Connection(
                host=self.host,
                port=self.port,
                database=self.database
            )
            self.cursor = self.conn.cursor()
        return self
    
    def upload(self, dataframe: pd.DataFrame, table: str):
        if not self.cursor:
            raise Exception("Connection not established. Call connect() first.")

        for _, row in dataframe.iterrows():
            values = []
            for val in row:
                if pd.isna(val):
                    values.append("NULL")
                elif isinstance(val, str):
                    # Escape single quotes in strings
                    safe_val = val.replace("'", "''")
                    values.append(f"'{safe_val}'")
                else:
                    values.append(str(val))

            values_str = ", ".join(values)
            insert_query = f"INSERT INTO {table} VALUES ({values_str})"

            try:
                self.cursor.execute(insert_query)
            except Exception as e:
                print(f"Failed to insert row: {row.tolist()} -- Error: {e}")


    def upload2(self, dataframe: pd.DataFrame, table_name: str, database: str='airline_dwh', overwrite: bool=True, file_format: str='orc'):
        temp_dir = './tmp'
        file_name = f'{table_name}_temp.{file_format}'
        temp_file = os.path.join(temp_dir, file_name)
        try:
            # dataframe.to_parquet(temp_file, index=False)
            FileHandler.save(dataframe, temp_dir, file_name, file_format)

            if not os.path.exists(temp_file):
                raise FileNotFoundError(f"Temp file not found: {temp_file}")
                    
            temp_file = os.path.join('/home/hduser/tmp', f'{table_name}_temp.{file_format}')
            print(temp_file)
            load_sql = f"""
                LOAD DATA LOCAL INPATH '{temp_file}'
                {'OVERWRITE INTO TABLE' if overwrite else 'INTO TABLE'} `{database}`.`{table_name}`
            """
            print(load_sql)

            self.cursor.execute(load_sql)
            print(f"Successfully uploaded data to {database}.{table_name}")
        except Exception as e:
            print(f"Error uploading data: {str(e)}")
            raise
        finally:
            if os.path.exists(temp_file):
                # os.remove(temp_file)
                pass


    def execute_query(self, query):
        if not self.cursor:
            raise Exception("Connection not established. Call connect() first.")
        
        self.cursor.execute(query)

        # If SELECT query, return a DataFrame
        if query.strip().lower().startswith("select"):
            columns = [col[0] for col in self.cursor.description]
            rows = self.cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        
        return None

    def close(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.conn:
            self.conn.close()
            self.conn = None
