"""utilities for working with bigquery"""

from logging import basicConfig, getLogger, INFO
from google.cloud import bigquery
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
        self, schema: str, table: str, column: str, *args
    ):  # pylint:disable=unused-argument
        """get the column schema from the specified json field for this table"""
        query = self.execute(
            f'''
                    CREATE TEMP FUNCTION jsonObjectKeys(input STRING)
                    RETURNS Array<String>
                    LANGUAGE js AS """
                    return Object.keys(JSON.parse(input));
                    """;
                    WITH keys AS (
                    SELECT
                        jsonObjectKeys({column}) AS keys
                    FROM
                        `{schema}`.`{table}`
                    WHERE {column} IS NOT NULL
                    )
                    SELECT
                    DISTINCT k
                    FROM keys
                    CROSS JOIN UNNEST(keys.keys) AS k
                ''',
            location="asia-south1",
        )
        return [json_field["k"] for json_field in query]

    def json_extract_op(self, json_column: str, json_field: str, sql_column: str):
        """outputs a sql query snippet for extracting a json field"""
        json_field = json_field.replace("'", "\\'")
        return f"json_value({json_column}, '$.\"{json_field}\"') as `{sql_column}`"

    def close(self):
        """closing the connection and releasing system resources"""
        try:
            self.bqclient.close()
        except Exception:
            logger.error("something went wrong while closing the bigquery connection")

        return True
