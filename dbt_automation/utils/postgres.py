"""helpers for postgres"""
import os
import psycopg2
from logging import basicConfig, getLogger, INFO

basicConfig(level=INFO)
logger = getLogger()


class PostgresClient:
    """a postgres client that can be used as a context manager"""

    @staticmethod
    def get_connection(host: str, port: str, user: str, password: str, database: str):
        """returns a psycopg connection"""
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )
        return connection

    def __init__(self, conn_info: dict = None):
        self.CONN_INFO = {
            "DBHOST": os.getenv("DBHOST"),
            "DBPORT": os.getenv("DBPORT"),
            "DBUSER": os.getenv("DBUSER"),
            "DBPASSWORD": os.getenv("DBPASSWORD"),
            "DBNAME": os.getenv("DBNAME"),
        }
        if conn_info is None:
            conn_info = self.CONN_INFO
        self.connection = PostgresClient.get_connection(
            conn_info.get("DBHOST", conn_info.get("host")),
            conn_info.get("DBPORT", conn_info.get("port")),
            conn_info.get("DBUSER", conn_info.get("user")),
            conn_info.get("DBPASSWORD", conn_info.get("password")),
            conn_info.get("DBNAME", conn_info.get("name")),
        )

    def execute(self, statement: str) -> list:
        """run a query and return the results"""
        cursor = self.connection.cursor()
        cursor.execute(statement)
        return cursor.fetchall()

    def get_tables(self, schema: str) -> list:
        """returns the list of table names in the given schema"""
        resultset = self.execute(
            f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}'
        """
        )
        return [x[0] for x in resultset]

    def get_columnspec(self, schema: str, table: str):
        """get the column schema for this table"""
        return [
            x[0]
            for x in self.execute(
                f"""SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table}'
            """
            )
        ]

    def get_json_columnspec(self, schema: str, table: str):
        """get the column schema from the _airbyte_data json field for this table"""
        return [
            x[0]
            for x in self.execute(
                f"""SELECT DISTINCT 
                    jsonb_object_keys(_airbyte_data)
                FROM "{schema}"."{table}"
            """
            )
        ]

    def close(self):
        try:
            self.connection.close()
        except Exception:
            logger.error("something went wrong while closing the postgres connection")

        return True
