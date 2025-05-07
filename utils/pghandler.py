import sqlalchemy

class PgHandler:
    
    def __init__(self, host: str="localhost", port: int=5432, database: str="telco_db", user: str="telco_user", password: str="telco_password"):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
    
    def connect(self):
        self.engine = sqlalchemy.create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}')
        self.connection = self.engine.connect()

        return self
    

    def execute(self, query: str):
        return self.connection.execute(query)


    def close(self):
        self.connection.close()
