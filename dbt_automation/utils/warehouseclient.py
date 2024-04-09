"""constructs and returns an instance of the client for the right warehouse"""

from dbt_automation.utils.postgres import PostgresClient
from dbt_automation.utils.bigquery import BigQueryClient


def get_client(warehouse: str, conn_info: dict = None, location: str = None):
    """constructs and returns an instance of the client for the right warehouse"""
    if warehouse == "postgres":
        # conn_info gets passed to psycopg2.connect
        client = PostgresClient(conn_info)
    elif warehouse == "bigquery":
        client = BigQueryClient(conn_info, location)
    else:
        raise ValueError("unknown warehouse")
    return client
