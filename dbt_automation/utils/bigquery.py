"""utilities for working with bigquery"""

from logging import basicConfig, getLogger, INFO
from google.cloud import bigquery


basicConfig(level=INFO)
logger = getLogger()


class BigQueryClient:
    """a bigquery client that can be used as a context manager"""

    def __init__(self):
        self.bqclient = bigquery.Client()
        self.name = "bigquery"

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
    
    # def get_table_data(self, schema: str, table: str, limit: int) -> list:
    #     """returns limited rows from the specified table in the given schema"""
    #     resultset = self.execute(
    #         f"""
    #         SELECT * 
    #         FROM {schema}.{table}
    #         LIMIT {limit};
    #         """
    #     )
    #     return resultset

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

    def close(self):
        """closing the connection and releasing system resources"""
        try:
            self.bqclient.close()
        except Exception:
            logger.error("something went wrong while closing the bigquery connection")

        return True
