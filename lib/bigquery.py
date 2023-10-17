"""utilities for working with bigquery"""

from google.cloud import bigquery


class BigQueryClient:
    """a bigquery client that can be used as a context manager"""

    def __init__(self):
        self.bqclient = bigquery.Client()

    def execute(self, statement: str, **kwargs) -> list:
        """run a query and return the results"""
        query_job = self.bqclient.query(statement, **kwargs)
        return query_job.result()

    def get_tables(self, schema: str) -> list:
        """returns the list of table names in the given schema"""
        tables = self.bqclient.list_tables(schema)
        return [x.table_id for x in tables]

    def get_table_columns(self, schema: str, table: str) -> list:
        """fetch the list of columns from a BigQuery table."""
        table_ref = f"{schema}.{table}"
        table: bigquery.Table = self.bqclient.get_table(table_ref)
        column_names = [field.name for field in table.schema]
        return column_names

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
