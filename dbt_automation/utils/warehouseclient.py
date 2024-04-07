"""constructs and returns an instance of the client for the right warehouse"""

from dbt_automation.utils.postgres import PostgresClient
from dbt_automation.utils.bigquery import BigQueryClient


def map_airbyte_keys_to_postgres_keys(conn_info: dict):
    """called below and by `post_system_transformation_tasks`"""
    if "tunnel_method" in conn_info:
        method = conn_info["tunnel_method"]

        if method["tunnel_method"] in ["SSH_KEY_AUTH", "SSH_PASSWORD_AUTH"]:
            conn_info["ssh_host"] = method["tunnel_host"]
            conn_info["ssh_port"] = method["tunnel_port"]
            conn_info["ssh_username"] = method["tunnel_user"]

        if method["tunnel_method"] == "SSH_KEY_AUTH":
            conn_info["ssh_pkey"] = method["ssh_key"]

        elif method["tunnel_method"] == "SSH_PASSWORD_AUTH":
            conn_info["ssh_password"] = method["tunnel_user_password"]

    conn_info["user"] = conn_info["username"]

    return conn_info


def get_client(warehouse: str, conn_info: dict = None, location: str = None):
    """constructs and returns an instance of the client for the right warehouse"""
    if warehouse == "postgres":
        # conn_info gets passed to psycopg2.connect
        conn_info = map_airbyte_keys_to_postgres_keys(conn_info)
        client = PostgresClient(conn_info)
    elif warehouse == "bigquery":
        client = BigQueryClient(conn_info, location)
    else:
        raise ValueError("unknown warehouse")
    return client
