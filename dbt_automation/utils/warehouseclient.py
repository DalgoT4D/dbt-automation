"""constructs and returns an instance of the client for the right warehouse"""

from dbt_automation.utils.postgres import PostgresClient
from dbt_automation.utils.bigquery import BigQueryClient


def get_client(warehouse: str):
    """constructs and returns an instance of the client for the right warehouse"""
    if warehouse == "postgres":
        client = PostgresClient()
    elif warehouse == "bigquery":
        client = BigQueryClient()
    else:
        raise ValueError("unknown warehouse")
    return client
