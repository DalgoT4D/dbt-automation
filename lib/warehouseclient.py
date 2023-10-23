"""constructs and returns an instance of the client for the right warehouse"""

from lib.postgres import PostgresClient
from lib.bigquery import BigQueryClient


def get_client(warehouse: str):
    """constructs and returns an instance of the client for the right warehouse"""
    if warehouse == "postgres":
        client = PostgresClient()
    elif warehouse == "bigquery":
        client = BigQueryClient()
    else:
        raise ValueError("unknown warehouse")
    return client
