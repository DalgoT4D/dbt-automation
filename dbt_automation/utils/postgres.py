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
            conn_info.get("host"),
            conn_info.get("port"),
            conn_info.get("username"),
            conn_info.get("password"),
            conn_info.get("database"),
        )
        self.cursor = None

    def runcmd(self, statement: str):
        """runs a command"""
        if self.cursor is None:
            self.cursor = self.connection.cursor()
        self.cursor.execute(statement)
        self.connection.commit()

    def execute(self, statement: str) -> list:
        """run a query and return the results"""
        if self.cursor is None:
            self.cursor = self.connection.cursor()
        self.cursor.execute(statement)
        return self.cursor.fetchall()

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
        )  # returns an array of tuples of values
        col_names = [desc[0] for desc in self.cursor.description]
        rows = [dict(zip(col_names, row)) for row in resultset]

        return rows

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

    def ensure_schema(self, schema: str):
        """creates the schema if it doesn't exist"""
        self.runcmd(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    def ensure_table(self, schema: str, table: str, columns: list):
        """creates the table if it doesn't exist. all columns are TEXT"""
        column_defs = [f"{column} TEXT" for column in columns]
        self.runcmd(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                {','.join(column_defs)}
            );
            """
        )

    def drop_table(self, schema: str, table: str):
        """drops the table if it exists"""
        self.runcmd(f"DROP TABLE IF EXISTS {schema}.{table};")

    def insert_row(self, schema: str, table: str, row: dict):
        """inserts a row into the table"""
        columns = ",".join(row.keys())
        values = ",".join([f"'{x}'" for x in row.values()])
        self.runcmd(
            f"""
            INSERT INTO {schema}.{table} ({columns})
            VALUES ({values});
            """
        )

    def close(self):
        try:
            self.connection.close()
        except Exception:
            logger.error("something went wrong while closing the postgres connection")

        return True
