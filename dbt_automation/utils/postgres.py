"""helpers for postgres"""
from logging import basicConfig, getLogger, INFO
import psycopg2

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

    def __init__(self, conn_info: dict):
        self.name = "postgres"
        if conn_info is None:
            raise ValueError("connection info required")

        self.connection = PostgresClient.get_connection(
            conn_info.get("DBHOST", conn_info.get("host")),
            conn_info.get("DBPORT", conn_info.get("port")),
            conn_info.get("DBUSER", conn_info.get("username")),
            conn_info.get("DBPASSWORD", conn_info.get("password")),
            conn_info.get("DBNAME", conn_info.get("database")),
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

    def get_schemas(self) -> list:
        """returns the list of schema names in the given database connection"""
        resultset = self.execute(
            f"""
            SELECT nspname
            FROM pg_namespace
            WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema';
            """
        )
        return [x[0] for x in resultset]
    
    def get_table_data(self, schema: str, table: str, limit: int) -> list:
        """returns limited rows from the specified table in the given schema"""
        resultset = self.execute(
            f"""
            SELECT * 
            FROM {schema}.{table}
            LIMIT {limit};
            """
        )
        return resultset
    
    def get_table_columns(self, schema: str, table: str) -> list:
        """returns the column names of the specified table in the given schema"""
        resultset = self.execute(
            f"""
            SELECT column_name 
            FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}';
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
