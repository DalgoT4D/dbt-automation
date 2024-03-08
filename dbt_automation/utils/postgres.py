"""helpers for postgres"""

from logging import basicConfig, getLogger, INFO
import psycopg2
import os
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface


basicConfig(level=INFO)
logger = getLogger()


class PostgresClient(WarehouseInterface):
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
        if conn_info is None:  # take creds from env
            conn_info = {
                "host": os.getenv("DBHOST"),
                "port": os.getenv("DBPORT"),
                "username": os.getenv("DBUSER"),
                "password": os.getenv("DBPASSWORD"),
                "database": os.getenv("DBNAME"),
            }

        self.connection = PostgresClient.get_connection(
            conn_info.get("host"),
            conn_info.get("port"),
            conn_info.get("username"),
            conn_info.get("password"),
            conn_info.get("database"),
        )
        self.cursor = None
        self.conn_info = conn_info

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

    def get_table_data(
        self,
        schema: str,
        table: str,
        limit: int,
        page: int = 1,
        order_by: str = None,
        order: int = 1,  # ASC
    ) -> list:
        """
        returns limited rows from the specified table in the given schema
        """
        offset = (page - 1) * limit
        # total_rows = self.execute(f"SELECT COUNT(*) FROM {schema}.{table}")[0][0]

        # select
        query = f"""
        SELECT * 
        FROM "{schema}"."{table}"
        """

        # order
        if order_by:
            query += f"""
            ORDER BY {quote_columnname(order_by, "postgres")} {"ASC" if order == 1 else "DESC"}
            """

        # offset, limit
        query += f"""
        OFFSET {offset} LIMIT {limit};
        """

        resultset = self.execute(query)  # returns an array of tuples of values
        col_names = [desc[0] for desc in self.cursor.description]
        rows = [dict(zip(col_names, row)) for row in resultset]

        return rows

    def get_table_columns(self, schema: str, table: str) -> list:
        """returns the column names of the specified table in the given schema"""
        resultset = self.execute(
            f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}';
            """
        )
        return [{"name": x[0], "data_type": x[1]} for x in resultset]

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

    def get_json_columnspec(self, schema: str, table: str, column: str):
        """get the column schema from the specified json field for this table"""
        return [
            x[0]
            for x in self.execute(
                f"""SELECT DISTINCT 
                    jsonb_object_keys({column}::jsonb)
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

    def json_extract_op(self, json_column: str, json_field: str, sql_column: str):
        """outputs a sql query snippet for extracting a json field"""
        return f"{json_column}::json->>'{json_field}' as \"{sql_column}\""

    def close(self):
        try:
            self.connection.close()
        except Exception:
            logger.error("something went wrong while closing the postgres connection")

        return True

    def generate_profiles_yaml_dbt(self, project_name, default_schema):
        """Generates the profiles.yml dictionary object for dbt"""
        if project_name is None or default_schema is None:
            raise ValueError("project_name and default_schema are required")

        target = "prod"

        """
        <project_name>: 
            outputs:
                prod: 
                    dbname: 
                    host: 
                    password: 
                    port: 5432
                    user: airbyte_user
                    schema: 
                    threads: 4
                    type: postgres
            target: prod
        """
        profiles_yml = {
            f"{project_name}": {
                "outputs": {
                    f"{target}": {
                        "dbname": self.conn_info["database"],
                        "host": self.conn_info["host"],
                        "password": self.conn_info["password"],
                        "port": int(self.conn_info["port"]),
                        "user": self.conn_info["username"],
                        "schema": default_schema,
                        "threads": 4,
                        "type": "postgres",
                    }
                },
                "target": target,
            },
        }

        return profiles_yml

    def get_total_rows(self, schema: str, table: str) -> int:
        """Fetches the total number of rows for a specified table."""
        try:
            resultset = self.execute(
                f"""
                SELECT COUNT(*) 
                FROM "{schema}"."{table}";
                """
            )
            total_rows = resultset[0][0] if resultset else 0
            return total_rows
        except Exception as e:
            logger.error(f"Failed to fetch total rows for {schema}.{table}: {e}")
            raise