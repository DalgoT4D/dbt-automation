"""utilities for working with bigquery"""

from logging import basicConfig, getLogger, INFO
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

basicConfig(level=INFO)
logger = getLogger()


class BigQueryClient:
    """a bigquery client that can be used as a context manager"""

    def __init__(self, conn_info=None):
        self.name = "bigquery"
        self.bqclient = None
        if conn_info is None:  # take creds from env
            self.bqclient = bigquery.Client()
        else:
            creds1 = service_account.Credentials.from_service_account_info(conn_info)
            self.bqclient = bigquery.Client(
                credentials=creds1, project=creds1.project_id
            )

    def execute(self, statement: str, **kwargs) -> list:
        """run a query and return the results"""
        query_job = self.bqclient.query(statement, **kwargs)
        return query_job.result()

    def get_tables(self, schema: str) -> list:
        """returns the list of table names in the given schema"""
        tables = self.bqclient.list_tables(schema)
        return [x.table_id for x in tables]

    def get_schemas(self) -> list:
        """returns the list of schema names in the given connection"""
        datasets = self.bqclient.list_datasets()
        return [x.dataset_id for x in datasets]

    def get_table_columns(self, schema: str, table: str) -> list:
        """fetch the list of columns from a BigQuery table."""
        table_ref = f"{schema}.{table}"
        table: bigquery.Table = self.bqclient.get_table(table_ref)
        column_names = [field.name for field in table.schema]
        return column_names

    def get_table_data(self, schema: str, table: str, limit: int) -> list:
        """returns limited rows from the specified table in the given schema"""
        table_ref = f"{schema}.{table}"
        table: bigquery.Table = self.bqclient.get_table(table_ref)
        records = self.bqclient.list_rows(table=table, max_results=limit)
        rows = [dict(record) for record in records]

        return rows

    def get_columnspec(self, schema: str, table_id: str):
        """fetch the list of columns from a BigQuery table."""
        return self.get_table_columns(schema, table_id)

    def get_json_columnspec(
        self, schema: str, table: str, *args
    ):  # pylint:disable=unused-argument
        """get the column schema from the _airbyte_data json field for this table"""
        query = self.execute(
            f'''
                    CREATE TEMP FUNCTION jsonObjectKeys(input STRING)
                    RETURNS Array<String>
                    LANGUAGE js AS """
                    return Object.keys(JSON.parse(input));
                    """;
                    WITH keys AS (
                    SELECT
                        jsonObjectKeys(_airbyte_data) AS keys
                    FROM
                        `{schema}`.`{table}`
                    WHERE _airbyte_data IS NOT NULL
                    )
                    SELECT
                    DISTINCT k
                    FROM keys
                    CROSS JOIN UNNEST(keys.keys) AS k
                ''',
            location="asia-south1",
        )
        return [json_field["k"] for json_field in query]

    def schema_exists_(self, schema: str) -> bool:
        """checks if the schema exists"""
        try:
            self.bqclient.get_dataset(schema)
            return True
        except NotFound:
            return False

    def ensure_schema(self, schema: str):
        """creates the schema if it doesn't exist"""
        if not self.schema_exists_(schema):
            self.bqclient.create_dataset(schema)
            logger.info("created schema %s", schema)

    def ensure_table(self, schema: str, table: str, columns: list):
        """creates the table if it doesn't exist"""
        if not self.table_exists_(schema, table):
            table_ref = f"{self.bqclient.project}.{schema}.{table}"
            bqtable = bigquery.Table(table_ref)
            bqtable.schema = [bigquery.SchemaField(col, "STRING") for col in columns]
            self.bqclient.create_table(bqtable)
            logger.info("created table %s.%s", schema, table)

    def table_exists_(self, schema: str, table: str) -> bool:
        """checks if the table exists"""
        table_ref = f"{self.bqclient.project}.{schema}.{table}"
        try:
            self.bqclient.get_table(table_ref)
            return True
        except NotFound:
            return False

    def drop_table(self, schema: str, table: str):
        """drops the table if it exists"""
        if self.table_exists_(schema, table):
            logger.info("dropping table %s.%s", schema, table)
            table_ref = f"{self.bqclient.project}.{schema}.{table}"
            self.bqclient.delete_table(table_ref)

    def insert_row(self, schema: str, table: str, row: dict):
        """inserts a row into the table"""
        table_ref = f"{self.bqclient.project}.{schema}.{table}"
        bqtable = self.bqclient.get_table(table_ref)
        errors = self.bqclient.insert_rows(bqtable, [row])
        if errors:
            # pylint:disable=logging-fstring-interpolation
            logger.error(f"row insertion failed: {errors}")

    def close(self):
        """closing the connection and releasing system resources"""
        try:
            self.bqclient.close()
        except Exception:  # pylint:disable=broad-except
            logger.error("something went wrong while closing the bigquery connection")

        return True
