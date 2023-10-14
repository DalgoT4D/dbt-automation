"""utilities for working with bigquery"""

from google.cloud import bigquery


def get_columnspec(schema: str, table_id: str):
    """
    Fetch the list of columns from a BigQuery table.

    Args:
    - schema (str): The BigQuery dataset ID.
    - table_id (str): The BigQuery table ID.

    Returns:
    - List[str]: List of column names.
    """

    client = bigquery.Client()
    table_ref = f"{schema}.{table_id}"
    table = client.get_table(table_ref)
    column_names = [field.name for field in table.schema]

    return column_names


def get_json_columnspec(schema: str, table: str, conn_info: dict):
    """get the column schema from the _airbyte_data json field for this table"""
    client = bigquery.Client()
    query = client.query(
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
