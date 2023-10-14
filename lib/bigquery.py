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
