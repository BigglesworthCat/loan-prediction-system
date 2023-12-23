import psycopg2
from sqlalchemy import create_engine

import os
from dotenv import load_dotenv

load_dotenv()


class DatabaseConnection:
    db_params = {
        'host': os.getenv('POSTGRES_HOST'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'database': os.getenv('POSTGRES_DB'),
        'port': os.getenv('POSTGRES_PORT'),
    }

    engine = create_engine(
        f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')

    schema = os.getenv('POSTGRES_SCHEMA')
    original_table = os.getenv('POSTGRES_ORIGINAL_TABLE')
    transformed_table = os.getenv('POSTGRES_TRANSFORMED_TABLE')

    def __enter__(self):
        self.conn = psycopg2.connect(**self.db_params)
        return self.conn

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()
